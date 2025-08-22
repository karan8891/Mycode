# dags/sts_event_e2e_audit_retry_publish.py
"""
Airflow DAG: STS Event → Audit → Retry → Publish TOC

Flow Overview:
1. Pull TOC JSON from a Pub/Sub subscription.
2. Create or re-use an STS job for given includePrefixes.
3. Run the STS job and log creation/trigger details to BigQuery audit table.
4. Poll the STS operation until SUCCESS/FAILED/ABORTED.
5. On SUCCESS:
    - Publish original TOC JSON to cdmnxt-trigger-topic.
    - Log success to BigQuery.
6. On FAILED/ABORTED:
    - Compare destination bucket contents with TOC file list.
    - Build a retry STS job with only missing files.
    - Log retry creation/trigger details to BigQuery.
    - Wait for retry completion and log results.
    - On retry SUCCESS, publish TOC JSON to cdmnxt-trigger-topic.
7. All stages log an event to BigQuery for full audit.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor

import os, json, base64, time, uuid, datetime as dt
from typing import List, Dict, Any

from google.cloud import storage, bigquery, pubsub_v1
from googleapiclient.discovery import build

# ========= ENV VARS (configure in Composer) =========
PROJECT_ID             = os.getenv("PROJECT_ID")               # Current project where DAG runs
PUBSUB_SUB_FQN         = os.getenv("TOC_SUBSCRIPTION_FQN")     # Fully-qualified Pub/Sub subscription for TOC messages
COMPLETION_TOPIC_FQN   = os.getenv("STS_COMPLETION_TOPIC_FQN") # Fully-qualified topic for STS completion notifications
CDMNXT_TOPIC_FQN       = os.getenv("CDMNXT_TOPIC_FQN")         # Fully-qualified topic to send final TOC
DEST_BUCKET            = os.getenv("DEST_BUCKET")              # Destination bucket for STS
DEST_PATH              = os.getenv("DEST_PATH", "")            # Destination path prefix (optional)
AUDIT_TABLE_FQN        = os.getenv("AUDIT_TABLE_FQN")          # BigQuery table for audit logs (project.dataset.table)
MAX_RETRY_PREFIXES     = int(os.getenv("MAX_RETRY_PREFIXES", "1000")) # Safety limit for retry
POLL_INTERVAL_SEC      = int(os.getenv("POLL_INTERVAL_SEC", "20"))
POLL_TIMEOUT_SEC       = int(os.getenv("POLL_TIMEOUT_SEC", "3600"))

# ========= Helper: Write to BigQuery Audit Table =========
def _bq_insert(row: Dict[str, Any]):
    client = bigquery.Client(project=PROJECT_ID)
    errors = client.insert_rows_json(AUDIT_TABLE_FQN, [row])
    if errors:
        raise RuntimeError(f"BQ insert failed: {errors}")

def _audit(event_type: str, context, **kw):
    """
    Central audit function — inserts a row into BigQuery for each important stage.
    event_type: JOB_CREATED, JOB_TRIGGERED, JOB_SUCCESS, JOB_FAILURE, RETRY_TRIGGERED, RETRY_SUCCESS, RETRY_FAILURE, PUBLISH_TOC, etc.
    kw: optional extra fields for status, counters, error details, etc.
    """
    ti  = context["ti"]
    now = dt.datetime.utcnow().isoformat() + "Z"
    base = {
        "event_ts": now,
        "event_type": event_type,
        "status": kw.get("status"),
        "dag_id": context["dag"].dag_id,
        "run_id": context["run_id"],
        "task_id": context["task"].task_id,
        "project_id": PROJECT_ID,
        "sts_job_name": ti.xcom_pull(key="sts_job_name"),
        "operation_name": ti.xcom_pull(key="operation_name"),
        "dest_bucket": DEST_BUCKET,
        "dest_path": DEST_PATH,
        "bytes_found": kw.get("bytes_found"),
        "bytes_copied": kw.get("bytes_copied"),
        "objects_found": kw.get("objects_found"),
        "objects_copied": kw.get("objects_copied"),
        "start_time": kw.get("start_time"),
        "end_time": kw.get("end_time"),
        "error_message": kw.get("error_message"),
    }
    _bq_insert(base)

# ========= Helper: Normalize path for STS =========
def _normalize_path(path: str) -> str:
    if not path:
        return ""
    p = path.strip("/")
    return f"{p}/" if p else ""

# ========= Helper: Read TOC from Pub/Sub message =========
def _read_toc_json_from_msg(msg_data_b64: str) -> Dict[str, Any]:
    """
    Decodes Pub/Sub message and returns TOC JSON.
    Supports two formats:
      - Full TOC JSON in message
      - Pointer JSON: {"toc_bucket": "...", "toc_name": "..."}
    """
    raw = base64.b64decode(msg_data_b64).decode("utf-8")
    payload = json.loads(raw)
    if "cdm_object_mapping" in payload:
        return payload
    if "toc_bucket" in payload and "toc_name" in payload:
        gcs = storage.Client()
        text = gcs.bucket(payload["toc_bucket"]).blob(payload["toc_name"]).download_as_text()
        return json.loads(text)
    raise ValueError("Invalid TOC format in message")

# ========= Helper: Extract prefixes from TOC =========
def _extract_include_prefixes_from_toc(toc: Dict[str, Any]) -> List[str]:
    prefixes = []
    for m in toc.get("cdm_object_mapping", []):
        for o in m.get("dataobject", []):
            name = o.get("name")
            if name:
                prefixes.append(name)
    # Remove duplicates
    seen, out = set(), []
    for p in prefixes:
        if p not in seen:
            seen.add(p); out.append(p)
    return out

# ========= STS Client =========
def _sts_client():
    return build("storagetransfer", "v1", cache_discovery=False)

# ========= Create or reuse STS Job =========
def _create_job_if_needed(job_desc: str, src_bucket: str, include_prefixes: List[str]) -> str:
    """
    Checks for an existing job with matching description; creates if not found.
    Sets notificationConfig so Pub/Sub gets completion messages.
    """
    svc = _sts_client()
    resp = svc.transferJobs().list(filter=json.dumps({"project_id": PROJECT_ID})).execute()
    for job in resp.get("transferJobs", []):
        if job.get("description") == job_desc:
            return job["name"]

    body = {
        "description": job_desc,
        "status": "ENABLED",
        "projectId": PROJECT_ID,
        "transferSpec": {
            "gcsDataSource": {"bucketName": src_bucket},
            "gcsDataSink":   {"bucketName": DEST_BUCKET, "path": _normalize_path(DEST_PATH)},
            "objectConditions": {"includePrefixes": include_prefixes[:MAX_RETRY_PREFIXES]},
            "transferOptions": {
                "overwriteObjectsAlreadyExistingInSink": True
            }
        },
        "schedule": {
            "scheduleStartDate": {"year": dt.datetime.utcnow().year, "month": dt.datetime.utcnow().month, "day": dt.datetime.utcnow().day},
            "startTimeOfDay": {"hours": dt.datetime.utcnow().hour, "minutes": dt.datetime.utcnow().minute},
            "scheduleEndDate": {"year": dt.datetime.utcnow().year + 1, "month": dt.datetime.utcnow().month, "day": dt.datetime.utcnow().day}
        },
        "notificationConfig": {
            "pubsubTopic": COMPLETION_TOPIC_FQN,
            "eventTypes": ["TRANSFER_OPERATION_SUCCESS","TRANSFER_OPERATION_FAILED","TRANSFER_OPERATION_ABORTED"],
            "payloadFormat": "JSON"
        }
    }
    created = svc.transferJobs().create(body=body).execute()
    return created["name"]

# ========= Run STS Job =========
def _run_job(job_name: str) -> str:
    svc = _sts_client()
    op = svc.transferJobs().run(jobName=job_name, body={"projectId": PROJECT_ID}).execute()
    return op["name"]

# ========= Poll Operation until complete =========
def _poll_operation(op_name: str, timeout_s: int, interval_s: int) -> Dict[str, Any]:
    svc = _sts_client()
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        op = svc.transferOperations().get(name=op_name).execute()
        status = op.get("metadata", {}).get("status")
        if status in ("SUCCESS", "FAILED", "ABORTED"):
            return op
        time.sleep(interval_s)
    return op

# ========= Compare destination contents to TOC =========
def _missing_from_dest(toc: Dict[str, Any]) -> List[str]:
    """
    Returns list of files from TOC not found in destination bucket/path.
    """
    cli = storage.Client()
    missing = []
    for m in toc.get("cdm_object_mapping", []):
        for o in m.get("dataobject", []):
            src_name = o.get("name")
            dest_obj = f"{_normalize_path(DEST_PATH)}{src_name}" if DEST_PATH else src_name
            if not cli.bucket(DEST_BUCKET).blob(dest_obj).exists():
                missing.append(src_name)
    return missing[:MAX_RETRY_PREFIXES]

# ========= Publish helper =========
def _publish(topic_fqn: str, data: bytes):
    pub = pubsub_v1.PublisherClient()
    pub.publish(topic_fqn, data).result()

# ========= Tasks =========
def t_pull_toc_from_pubsub(messages, context):
    """
    Sensor callback: takes first valid TOC, stores in XCom.
    """
    ti = context["ti"]
    for m in messages:
        try:
            toc = _read_toc_json_from_msg(m["message"]["data"])
            ti.xcom_push(key="toc_json", value=toc)
            _audit("TOC_RECEIVED", context, status="OK")
            return [m]
        except Exception:
            continue
    return []

def t_create_or_run(**context):
    """
    Creates or reuses an STS job from TOC includePrefixes and runs it.
    """
    ti = context["ti"]
    toc = ti.xcom_pull(key="toc_json")
    src_bucket = os.getenv("SOURCE_BUCKET")
    include_prefixes = _extract_include_prefixes_from_toc(toc)
    job_desc = f"event_sts_{toc.get('cdm_process_id', uuid.uuid4().hex[:8])}"
    job_name = _create_job_if_needed(job_desc, src_bucket, include_prefixes)
    ti.xcom_push(key="sts_job_name", value=job_name)
    _audit("JOB_CREATED", context, status="ENABLED")

    op_name = _run_job(job_name)
    ti.xcom_push(key="operation_name", value=op_name)
    _audit("JOB_TRIGGERED", context, status="RUNNING")

def t_wait_and_branch(**context):
    """
    Waits for STS operation completion.
    Returns True if SUCCESS, else False for retry path.
    """
    ti = context["ti"]
    op = _poll_operation(ti.xcom_pull(key="operation_name"), POLL_TIMEOUT_SEC, POLL_INTERVAL_SEC)
    md = op.get("metadata", {})
    status = md.get("status", "UNKNOWN")
    counters = md.get("transferCounters", {}) or {}
    _audit("JOB_FINALIZED", context, status=status, **counters)
    return status == "SUCCESS"

def t_publish_toc_to_cdmnxt(**context):
    """
    Publishes TOC JSON to CDMNXT topic on success.
    """
    ti = context["ti"]
    toc = ti.xcom_pull(key="toc_json")
    _publish(CDMNXT_TOPIC_FQN, json.dumps(toc).encode("utf-8"))
    _audit("PUBLISH_TOC", context, status="SUCCESS")

def t_checksum_and_retry(**context):
    """
    On failure, compares TOC vs destination, retries STS with missing files.
    """
    ti = context["ti"]
    toc = ti.xcom_pull(key="toc_json")
    missing = _missing_from_dest(toc)
    if not missing:
        _audit("RETRY_SKIPPED", context, status="NO_MISSING")
        return False
    src_bucket = os.getenv("SOURCE_BUCKET")
    job_desc = f"retry_event_sts_{toc.get('cdm_process_id', uuid.uuid4().hex[:8])}"
    job_name = _create_job_if_needed(job_desc, src_bucket, missing)
    ti.xcom_push(key="sts_job_name", value=job_name)
    op_name = _run_job(job_name)
    ti.xcom_push(key="operation_name", value=op_name)
    _audit("RETRY_TRIGGERED", context, status="RUNNING")
    op2 = _poll_operation(op_name, POLL_TIMEOUT_SEC, POLL_INTERVAL_SEC)
    status2 = op2.get("metadata", {}).get("status", "UNKNOWN")
    _audit("RETRY_FINALIZED", context, status=status2)
    return status2 == "SUCCESS"

# ========= DAG Definition =========
default_args = {"owner": "data-eng", "retries": 0}

with DAG(
    dag_id="sts_event_e2e_audit_retry_publish",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    default_args=default_args,
) as dag:

    pull_toc = PubSubPullSensor(
        task_id="pull_toc",
        project_id=PROJECT_ID,
        subscription=PUBSUB_SUB_FQN,
        ack_messages=True,
        max_messages=10,
        poke_interval=10,
        timeout=300,
        messages_callback=t_pull_toc_from_pubsub,
    )

    create_or_run = PythonOperator(task_id="create_or_run", python_callable=t_create_or_run, provide_context=True)
    wait_and_branch = ShortCircuitOperator(task_id="wait_and_branch", python_callable=t_wait_and_branch, provide_context=True)
    publish_toc = PythonOperator(task_id="publish_toc", python_callable=t_publish_toc_to_cdmnxt, provide_context=True)
    checksum_retry = ShortCircuitOperator(task_id="checksum_and_retry", python_callable=t_checksum_and_retry, provide_context=True)
    publish_after_retry = PythonOperator(task_id="publish_after_retry", python_callable=t_publish_toc_to_cdmnxt, provide_context=True)

    pull_toc >> create_or_run >> wait_and_branch
    wait_and_branch >> publish_toc
    wait_and_branch >> checksum_retry >> publish_after_retry
