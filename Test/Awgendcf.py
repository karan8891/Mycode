import base64, json, logging, os
from typing import Any, Dict, Tuple
from datetime import datetime, timezone
from google.cloud import storage, bigquery

log = logging.getLogger("cf3_completion")
log.setLevel(logging.INFO)
_storage = storage.Client()
_bq = None

# -------- BQ audit config --------
BQ_PROJECT = os.getenv("BQ_PROJECT") or os.getenv("PROJECT_ID")  # optional; defaults to runtime project
BQ_DATASET = os.getenv("BQ_AUDIT_DATASET")      # e.g. "cdmnxtsmoketestcf"
BQ_TABLE   = os.getenv("BQ_AUDIT_TABLE")        # e.g. "run_audit"

def _bq_client():
    global _bq
    if _bq is None:
        _bq = bigquery.Client(project=BQ_PROJECT) if BQ_PROJECT else bigquery.Client()
    return _bq

def _ensure_audit_table():
    """Create table if missing; ok to be a no-op if envs aren’t set."""
    if not (BQ_DATASET and BQ_TABLE):
        return False
    client = _bq_client()
    table_id = f"{client.project}.{BQ_DATASET}.{BQ_TABLE}"
    try:
        client.get_table(table_id)
        return True
    except Exception:
        schema = [
            bigquery.SchemaField("ts_utc", "TIMESTAMP"),
            bigquery.SchemaField("cdm_process_id", "STRING"),
            bigquery.SchemaField("object_name", "STRING"),
            bigquery.SchemaField("stage", "STRING"),     # CF3_RECEIVED | CF3_VERIFIED | CF3_DONE
            bigquery.SchemaField("status", "STRING"),    # SUCCESS/FAIL/COMPLETE/PARTIAL
            bigquery.SchemaField("movement_type", "STRING"),
            bigquery.SchemaField("sor_bucket", "STRING"),
            bigquery.SchemaField("run_root", "STRING"),
            bigquery.SchemaField("details_json", "STRING"),
        ]
        client.create_table(bigquery.Table(table_id, schema=schema))
        return True

def _audit_insert(*, cdm_process_id, object_name, stage, status,
                  movement_type=None, sor_bucket=None, run_root=None, details=None):
    """Append one audit row; logs on error; no-op if envs not set."""
    if not (BQ_DATASET and BQ_TABLE):
        log.info("BQ audit disabled (set BQ_AUDIT_DATASET & BQ_AUDIT_TABLE to enable).")
        return
    if not _ensure_audit_table():
        log.warning("BQ audit table not available.")
        return
    client = _bq_client()
    table_id = f"{client.project}.{BQ_DATASET}.{BQ_TABLE}"
    row = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "cdm_process_id": cdm_process_id or "",
        "object_name": object_name or "",
        "stage": stage,
        "status": status,
        "movement_type": (movement_type or "")[:32],
        "sor_bucket": sor_bucket or "",
        "run_root": run_root or "",
        "details_json": json.dumps(details or {}, separators=(",", ":"))[:900000],
    }
    errs = client.insert_rows_json(table_id, [row])
    if errs:
        log.error("BQ audit insert failed: %s", errs)

# -------- helpers --------
def _b64_json(event) -> Dict[str, Any]:
    data = event.get("data")
    raw = base64.b64decode(data if isinstance(data, (bytes, bytearray)) else data.encode("utf-8"))
    txt = raw.decode("utf-8")
    log.info("CF3 got: %s", txt[:600])
    return json.loads(txt)

def _split_gs(path: str) -> Tuple[str, str]:
    s = path.strip()
    if s.startswith("gs://"): s = s[5:]
    if "/" in s:
        b, p = s.split("/", 1)
        return b, p.rstrip("/") + "/"
    return s, ""

def _exists(bkt: str, name: str) -> bool:
    return _storage.bucket(bkt).blob(name).exists()

def _write_json(bkt: str, name: str, obj: Dict[str, Any]) -> None:
    _storage.bucket(bkt).blob(name).upload_from_string(
        json.dumps(obj, indent=2), content_type="application/json"
    )

# -------- entry point --------
def cf3_completion(event, context) -> None:
    msg = _b64_json(event)

    proc_id   = msg.get("cdm_process_id")
    obj_name  = msg.get("object_name")
    mv_type   = msg.get("data_movement_pattern_type") or msg.get("movement_type")
    run_root  = msg.get("run_root")              # preferred: gs://bucket/runs/<proc>/<obj>/
    sor_root  = msg.get("sor_bucket")            # optional: gs://bucket[/prefix]

    # 1) Audit: RECEIVED
    _audit_insert(
        cdm_process_id=proc_id,
        object_name=obj_name,
        stage="CF3_RECEIVED",
        status="SUCCESS",
        movement_type=mv_type,
        sor_bucket=sor_root,
        run_root=run_root,
        details={"payload": msg},
    )

    # Resolve bucket/prefix where CF2 wrote artifacts
    if run_root:
        bucket, prefix = _split_gs(run_root)
    else:
        if not (sor_root and proc_id and obj_name):
            # 2) Audit: VERIFICATION failed if we can’t resolve paths
            _audit_insert(
                cdm_process_id=proc_id, object_name=obj_name,
                stage="CF3_VERIFIED", status="FAIL",
                movement_type=mv_type, sor_bucket=sor_root, run_root="",
                details={"reason":"Cannot resolve run_root; need run_root or (sor_bucket, cdm_process_id, object_name)."}
            )
            raise ValueError("Cannot resolve run_root")
        b, p = _split_gs(sor_root)
        bucket, prefix = b, f"{p}runs/{proc_id}/{obj_name}/"

    # Check expected artifacts
    expected = ["verification.json", "dag1_ingest.json", "dag2_quality.json", "dag3_publish.json"]
    present, missing = [], []
    for f in expected:
        name = prefix + f
        if _exists(bucket, name):
            present.append(f)
        else:
            missing.append(f)

    # 2) Audit: VERIFIED
    ver_status = "SUCCESS" if not missing else "FAIL"
    _audit_insert(
        cdm_process_id=proc_id, object_name=obj_name,
        stage="CF3_VERIFIED", status=ver_status,
        movement_type=mv_type,
        sor_bucket=f"gs://{bucket}",
        run_root=f"gs://{bucket}/{prefix}",
        details={"present": present, "missing": missing},
    )

    # Summarize + write cf3_done.json
    final_status = "COMPLETE" if not missing else "PARTIAL"
    summary = {
        "receive_payload": msg,
        "bucket": bucket,
        "run_prefix": prefix,
        "expected_files": expected,
        "present_files": present,
        "missing_files": missing,
        "status": final_status,
    }
    _write_json(bucket, prefix + "cf3_done.json", summary)

    # 3) Audit: DONE
    _audit_insert(
        cdm_process_id=proc_id, object_name=obj_name,
        stage="CF3_DONE", status=final_status,
        movement_type=mv_type,
        sor_bucket=f"gs://{bucket}",
        run_root=f"gs://{bucket}/{prefix}",
        details={"wrote":"cf3_done.json"},
    )

    log.info("CF3 wrote gs://%s/%scf3_done.json with status %s", bucket, prefix, final_status)



google-cloud-storage>=2.10.0
google-cloud-bigquery>=3.25.0
