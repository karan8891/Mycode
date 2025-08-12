import os, json, time, uuid, base64
from datetime import datetime
from typing import List, Dict

from google.cloud import storage, pubsub_v1
import google.auth
from google.auth.transport.requests import Request
import requests

# ========= ENV (no hardcoding) =========
PROJECT_ID              = os.getenv("PROJECT_ID")                      # GDW project (for Pub/Sub etc.)
COMPLETION_SUBSCRIPTION = os.getenv("COMPLETION_SUBSCRIPTION")         # projects/.../subscriptions/...
CDMNXT_TOPIC            = os.getenv("CDMNXT_TOPIC")                    # projects/.../topics/...
DEST_PROJECT_ID         = os.getenv("DEST_PROJECT_ID")                 # STS project
DEST_BUCKET             = os.getenv("DEST_BUCKET")                     # sink bucket for STS
DEST_PATH               = os.getenv("DEST_PATH", "")                   # sink path (can be empty)
SOURCE_BUCKET           = os.getenv("SOURCE_BUCKET")                   # source bucket for STS
DELETE_AFTER_TRANSFER   = os.getenv("DELETE_AFTER_TRANSFER", "false").lower() == "true"
NOTIFICATION_TOPIC_FQN  = os.getenv("NOTIFICATION_TOPIC_FQN")          # projects/.../topics/sts-completion-topic
DAG_TRIGGER_URL         = os.getenv("DAG_TRIGGER_URL")                 # e.g. https://<composer>/api/experimental/dags/<dag_id>/dag_runs

BUFFER_SECONDS          = int(os.getenv("BUFFER_SECONDS", "0"))        # 0 = flush immediately
MAX_BATCH               = int(os.getenv("MAX_BATCH", "100"))           # cap batch size

# ========= Clients =========
storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()

# ========= In-memory buffer (best-effort) =========
_BUFFER: List[Dict] = []
_LAST_FLUSH = 0.0


# ========= Helpers =========
def _read_toc(toc_bucket: str, toc_name: str) -> Dict:
    text = storage_client.bucket(toc_bucket).blob(toc_name).download_as_text()
    return json.loads(text)

def _validate_toc(toc: Dict):
    required = ["cdm_process_id", "app_id", "cdm_object_mapping", "type", "businessProcessingDateTime"]
    for k in required:
        if k not in toc:
            raise ValueError(f"TOC missing required key: {k}")
    if not isinstance(toc["cdm_object_mapping"], list) or not toc["cdm_object_mapping"]:
        raise ValueError("TOC.cdm_object_mapping must be non-empty list")

def _collect_include_prefixes(tocs: List[Dict]) -> List[str]:
    prefixes: List[str] = []
    for t in tocs:
        for m in t.get("cdm_object_mapping", []):
            for o in m.get("dataobject", []):
                # include exact object names
                name = o.get("name")
                if name:
                    prefixes.append(name)
    # de-dup, preserve order
    seen = set()
    dedup = []
    for p in prefixes:
        if p not in seen:
            seen.add(p)
            dedup.append(p)
    return dedup

def _make_unique_job_name() -> str:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    return f"event_sts_job_{ts}_{uuid.uuid4().hex[:8]}"

def _build_sts_body(include_prefixes: List[str], job_name: str) -> Dict:
    """
    Build STS job body using only ENV variables + collected prefixes.
    """
    return {
        "transferJob": {
            "name": job_name,                    # unique name per run (helps correlate completion)
            "description": f"Event STS job {job_name}",
            "status": "ENABLED",
            "transferSpec": {
                "gcsDataSource": {"bucketName": SOURCE_BUCKET},
                "gcsDataSink":   {"bucketName": DEST_BUCKET, "path": DEST_PATH},
                "objectConditions": {"includePrefixes": include_prefixes},
                "transferOptions": {
                    "overwriteObjectsAlreadyExistingInSink": True,
                    "deleteObjectsFromSourceAfterTransfer": DELETE_AFTER_TRANSFER
                }
            },
            "notificationConfig": {
                "pubsubTopic": NOTIFICATION_TOPIC_FQN,    # FQN required
                "eventTypes": ["TRANSFER_OPERATION_SUCCESS", "TRANSFER_OPERATION_FAILED"],
                "payloadFormat": "JSON"
            }
        }
    }

def _access_token() -> str:
    """
    Normal OAuth access token; works for Composer webserver if not IAP-protected.
    """
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    creds.refresh(Request())
    return creds.token

def _trigger_airflow_dag(conf: Dict, dag_run_id: str):
    """
    Calls Composer experimental endpoint:
      POST {DAG_TRIGGER_URL}
      Body: {"conf": <dict>}  (optionally include run_id if your env supports it)
    Many Composer envs accept an extra "run_id"; if not, they'll ignore it.
    """
    if not DAG_TRIGGER_URL:
        raise RuntimeError("DAG_TRIGGER_URL env var not set")

    headers = {
        "Authorization": f"Bearer {_access_token()}",
        "Content-Type": "application/json"
    }
    # most experimental endpoints expect only {"conf": {...}}
    body = {"conf": conf}
    # some support run_id (harmless if ignored)
    body["run_id"] = dag_run_id

    resp = requests.post(DAG_TRIGGER_URL, headers=headers, json=body, timeout=60)
    print(f"[CF] Trigger DAG: HTTP {resp.status_code} | {resp.text[:300]}")
    if resp.status_code not in (200, 201, 202):  # experimental often returns 200/201; some return 202
        raise RuntimeError(f"Airflow trigger failed: {resp.status_code} {resp.text}")

def _flush(batch_events: List[Dict]):
    """
    Process a buffered batch: read all TOCs, build a single STS body with includePrefixes, trigger the DAG.
    """
    tocs: List[Dict] = []
    toc_pointers: List[Dict] = []

    for ev in batch_events[:MAX_BATCH]:
        toc_bucket = ev["toc_bucket"]
        toc_name   = ev["toc_name"]
        toc = _read_toc(toc_bucket, toc_name)
        _validate_toc(toc)
        tocs.append(toc)
        toc_pointers.append({"toc_bucket": toc_bucket, "toc_name": toc_name})

    include_prefixes = _collect_include_prefixes(tocs)
    job_name = _make_unique_job_name()
    sts_body = _build_sts_body(include_prefixes, job_name)

    dag_run_id = f"sts_run_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:6]}"
    conf = {
        "project_id": PROJECT_ID,
        "completion_subscription": COMPLETION_SUBSCRIPTION,
        "cdmnxt_topic": CDMNXT_TOPIC,
        "job_name": job_name,
        "sts_body": sts_body,
        # pass only pointers; DAG will read actual TOC JSON from GCS:
        "toc_files": toc_pointers
    }

    print(f"[CF] Flush: job_name={job_name}, prefixes={len(include_prefixes)}, tocs={len(toc_pointers)}, run={dag_run_id}")
    _trigger_airflow_dag(conf, dag_run_id)


# ========= CFv1 entrypoint =========
def main(event, context):
    """
    Trigger: Pub/Sub (message from APMF CF)
    Expected message body (JSON):
      {
        "bucket": "<original bucket>",
        "name": "<original object>",
        "toc_bucket": "<bucket storing .toc>",
        "toc_name": "<path/to/file.toc>"
      }
    """
    global _LAST_FLUSH, _BUFFER

    # Pub/Sub envelope
    if "data" in event:
        msg = json.loads(base64.b64decode(event["data"]).decode("utf-8"))
    else:
        msg = event  # direct call (tests)

    # Validate required fields from APMF pointer
    for k in ("toc_bucket", "toc_name"):
        if k not in msg:
            raise ValueError(f"Missing '{k}' in message")

    _BUFFER.append({"toc_bucket": msg["toc_bucket"], "toc_name": msg["toc_name"]})
    now = time.time()
    if _LAST_FLUSH == 0:
        _LAST_FLUSH = now

    # Flush either immediately (no buffering) OR after window elapsed
    if BUFFER_SECONDS == 0 or (now - _LAST_FLUSH) >= BUFFER_SECONDS:
        batch = _BUFFER
        _BUFFER = []
        _LAST_FLUSH = now
        if batch:
            _flush(batch)
        else:
            print("[CF] Flush: buffer empty; nothing to do.")
    else:
        print(f"[CF] Buffered: size={len(_BUFFER)}; next flush in {int(BUFFER_SECONDS - (now - _LAST_FLUSH))}s")
