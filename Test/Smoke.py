import base64, json, os, uuid, datetime as dt, logging
from typing import Any, Dict, List

from google.cloud import pubsub_v1
from google.cloud import bigquery

# --------- ENV ----------
PROJECT_ID = os.environ.get("PROJECT_ID", "")
CF3_TOPIC  = os.environ.get("CF3_TOPIC", "")  # "projects/<p>/topics/<t>" or "<t>"
BQ_DATASET = os.environ.get("AWG_BQ_DATASET", "")
BQ_TABLE   = os.environ.get("AWG_BQ_TABLE", "")
FORCE_STATUS = os.environ.get("AWG_FORCE_STATUS", "").upper().strip()  # SUCCESS/FAILED/""


# --------- DEFAULTS ----------
DEFAULTS = {
    "app_id": "AWG",
    "cdm_process_id": f"PROC-{uuid.uuid4()}",
    "drt_id": "drtaccount",
    "data_pipeline_type": "INGRESS_INCREMENTAL_G2C",
    "data_movement_pattern_type": "RDBMS",
    "cdm_object_mapping": [
        {
            "object_name": "account_demo",
            "dataplex_object_identifier": (
                "projects/sandbox-corp-blade-ts2-6ebd/datasets/AWG_RAW/tables/account_demo"
            ),
            "cdm_landing_type": "BQ",
            "cdm_landing_path": (
                "projects/sandbox-corp-blade-sfr-9092/datasets/"
                "cdm_quarantine_ingress_data/tables/account_demo"
            ),
            "cdm_target_type": "BQ",
            "cdm_target_path": (
                "projects/sandbox-corp-blade-ts2-6ebd/datasets/AWG_RAW/tables/account_demo"
            ),
            "write_disposition": "WRITE_TRUNCATE",
        }
    ],
}

# --------- HELPERS ----------
def _topic_path(topic: str) -> str:
    if topic.startswith("projects/"):
        return topic
    if not PROJECT_ID:
        raise RuntimeError("PROJECT_ID env var is required to build topic path")
    return f"projects/{PROJECT_ID}/topics/{topic}"

def _coalesce(src: Dict[str, Any], key: str, fallback: Any) -> Any:
    val = src.get(key)
    return fallback if val in (None, "", []) else val

def _normalize_pipeline_type(s: str) -> str:
    # Accept underscore/space variants from screenshots
    return s.replace("  ", " ").replace("INGRESS_INCREMENTAL G2C", "INGRESS_INCREMENTAL_G2C")

def _ensure_mapping(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    cms = payload.get("cdm_object_mapping")
    if isinstance(cms, list) and cms:
        return cms
    return DEFAULTS["cdm_object_mapping"]

def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()

def _build_completion(input_msg: Dict[str, Any], iteration_id: str) -> Dict[str, Any]:
    app_id = _coalesce(input_msg, "app_id", DEFAULTS["app_id"])
    cdm_process_id = _coalesce(input_msg, "cdm_process_id", DEFAULTS["cdm_process_id"])
    drt_id = _coalesce(input_msg, "drt_id", DEFAULTS["drt_id"])
    pipeline_type = _normalize_pipeline_type(
        _coalesce(input_msg, "data_pipeline_type", DEFAULTS["data_pipeline_type"])
    )
    movement_type = _coalesce(
        input_msg, "data_movement_pattern_type", DEFAULTS["data_movement_pattern_type"]
    )

    mapping = _ensure_mapping(input_msg)

    # Synthetic "job id" & branch to mimic DAG-2/3
    branch = "KAFKA" if movement_type.upper() == "KAFKA" else "ALT"
    job_prefix = "stream" if branch == "KAFKA" else "batch"
    job_id = f"{job_prefix}-{uuid.uuid4().hex[:8]}"

    # Build dataTransferred (example mirrors your lines 78–110)
    target_name = mapping[0].get("cdm_target_path") or mapping[0].get("dataplex_object_identifier")
    data_transferred = {
        "businessProcessingDateTime": _now_iso(),
        "numberOfColumns": 6,
        "totalNumberOfRecords": 1_000_000,
        "type": "BIG_QUERY",
        "object": [
            {
                "name": target_name or "projects/demo/datasets/demo/tables/account_demo",
                "numberOfRecords": 1_000_000,
                "numberOfColumns": 6,
            }
        ],
    }

    status = FORCE_STATUS if FORCE_STATUS in ("SUCCESS", "FAILED") else "SUCCESS"

    completion = {
        "cdm_process_id": cdm_process_id,
        "app_id": app_id,
        "drt_id": drt_id,
        "data_movement_pattern_type": movement_type,
        "data_pipeline_type": pipeline_type,
        "cdm_object_mapping": [
            {
                **mapping[0],
                "dataTransferred": data_transferred,
            }
        ],
        "message": [],
        "status": status,
        # Extras helpful to CF3 / auditing
        "iteration_id": iteration_id,
        "branch": branch,
        "job_id": job_id,
        "input_echo": input_msg,  # keep the full original for traceability
    }
    return completion

def _publish_to_cf3(msg: Dict[str, Any]):
    topic_path = _topic_path(CF3_TOPIC)
    pub = pubsub_v1.PublisherClient()
    data = json.dumps(msg, separators=(",", ":")).encode("utf-8")
    future = pub.publish(topic_path, data)
    future.result(timeout=30)

def _insert_audit_row(input_msg: Dict[str, Any], completion: Dict[str, Any]):
    if not (BQ_DATASET and BQ_TABLE):
        return
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    row = {
        "ts_utc": _now_iso(),
        "component": "CF2",
        "iteration_id": completion.get("iteration_id"),
        "status": completion.get("status"),
        "branch": completion.get("branch"),
        "job_id": completion.get("job_id"),
        "cdm_process_id": completion.get("cdm_process_id"),
        "drt_id": completion.get("drt_id"),
        "app_id": completion.get("app_id"),
        "input_json": json.dumps(input_msg),
        "output_json": json.dumps(completion),
    }
    client.insert_rows_json(table_id, [row])  # best-effort; no raise

# --------- ENTRYPOINT (Gen1 Pub/Sub) ----------
def cdmnxtsmoketestcf(event, context):
    """
    Pub/Sub trigger.
    CF1 publishes the 'input' message (format like lines 42–68).
    CF2 mocks DAG-1/2/3 and publishes a 'completion' for CF3 (like lines 78–110).
    """
    if not PROJECT_ID or not CF3_TOPIC:
        raise RuntimeError("PROJECT_ID and CF3_TOPIC env vars are required")

    raw = base64.b64decode(event.get("data", b"")).decode("utf-8", errors="ignore") if event and "data" in event else "{}"
    try:
        input_msg = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        logging.exception("Invalid JSON in Pub/Sub message")
        input_msg = {}

    # Fill top-level defaults
    for k in ("app_id","cdm_process_id","drt_id","data_pipeline_type","data_movement_pattern_type"):
        input_msg.setdefault(k, DEFAULTS[k])
    input_msg["data_pipeline_type"] = _normalize_pipeline_type(input_msg["data_pipeline_type"])
    if "cdm_object_mapping" not in input_msg or not isinstance(input_msg["cdm_object_mapping"], list):
        input_msg["cdm_object_mapping"] = DEFAULTS["cdm_object_mapping"]

    iteration_id = str(uuid.uuid4())
    completion = _build_completion(input_msg, iteration_id)

    # Publish to CF3
    _publish_to_cf3(completion)

    # Optional: audit row
    try:
        _insert_audit_row(input_msg, completion)
    except Exception:
        logging.exception("BigQuery audit insert failed (non-fatal)")

    logging.info("CF2 completed. iteration_id=%s status=%s branch=%s",
                 iteration_id, completion.get("status"), completion.get("branch"))
    return ("OK", 200)



google-cloud-pubsub>=2.20.0
google-cloud-bigquery>=3.25.0
