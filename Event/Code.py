import os, json, uuid, base64
from datetime import datetime
from google.cloud import storage, pubsub_v1

# ENV
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC")  # e.g. sts-completion-topic
TOC_FOLDER   = os.getenv("TOC_FOLDER", "toc/")  # folder to write .toc under the same bucket

storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()

def _make_toc(event):
    # Build a minimal but complete TOC with dynamic example values
    file_name = event["name"]
    bucket    = event["bucket"]
    return {
        "cdm_process_id": str(uuid.uuid4()),
        "app_id": os.getenv("APP_ID", "BMG"),
        "drt_id": os.getenv("DRT_ID", "drt"),
        "data_pipeline_type": os.getenv("PIPELINE_TYPE", "INGRESS_INCREMENTAL_C2C"),
        "data_movement_pattern_type": os.getenv("MOVEMENT_PATTERN", "FILE_STS"),
        "cdm_object_mapping": [{
            "object_name": file_name.rsplit("/", 1)[-1].split(".")[0],
            "cdm_object_format": os.getenv("CDM_OBJECT_FORMAT", "parquet"),
            "cdm_landing_type": os.getenv("CDM_LANDING_TYPE", "GCS"),
            "cdm_landing_path": f"gs://{bucket}",
            "cdm_target_type": os.getenv("CDM_TARGET_TYPE", "GCS"),
            "cdm_target_path": os.getenv("CDM_TARGET_PATH", "gs://dest-bucket/path"),
            "cdm_object_identifier": os.getenv("CDM_OBJECT_IDENTIFIER", "projects/p/datasets/d/tables/t"),
            "write_disposition": os.getenv("WRITE_DISPOSITION", "WRITE_TRUNCATE"),
            "dataobject": [{
                "name": file_name,
                "numberOfRecords": 0,
                "numberOfColumns": 0
            }]
        }],
        "type": "STRUCTURED_DATA_FILE",
        "businessProcessingDateTime": datetime.utcnow().isoformat("T") + "Z",
        "totalNumberOfRecords": 0,
        "numberOfColumns": 0
    }

def _write_toc(bucket, toc_dict):
    # name = {TOC_FOLDER}/sts_{ts}_{uuid}.toc
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    toc_name = f"{TOC_FOLDER.rstrip('/')}/sts_{ts}_{uuid.uuid4().hex[:8]}.toc"
    blob = storage_client.bucket(bucket).blob(toc_name)
    blob.upload_from_string(json.dumps(toc_dict), content_type="application/json")
    return toc_name

def _publish_pointer(event, toc_bucket, toc_name):
    message = {
        "bucket": event["bucket"],     # original object’s bucket
        "name": event["name"],         # original object’s name
        "toc_bucket": toc_bucket,      # where .toc was written
        "toc_name": toc_name           # path to .toc
    }
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), PUBSUB_TOPIC)
    publisher.publish(topic_path, json.dumps(message).encode("utf-8")).result()

def main(event, context):
    # event is a GCS finalize (Pub/Sub-encoded if using CFv1 Pub/Sub trigger)
    if "data" in event:
        event = json.loads(base64.b64decode(event["data"]).decode("utf-8"))

    bucket = event["bucket"]
    toc = _make_toc(event)
    toc_name = _write_toc(bucket, toc)
    _publish_pointer(event, bucket, toc_name)



import os, json, time, uuid, base64
from datetime import datetime
from typing import List, Dict
from google.cloud import storage, pubsub_v1
from google.auth.transport import requests as gauth_requests
from google.oauth2 import id_token
import requests

# ==== ENV (no hardcoding) ====
PROJECT_ID             = os.getenv("PROJECT_ID")                    # GDW project (for Pub/Sub + DAG)
COMPLETION_SUBSCRIPTION= os.getenv("COMPLETION_SUBSCRIPTION")       # e.g. projects/..../subscriptions/sts-completion-sub
CDMNXT_TOPIC           = os.getenv("CDMNXT_TOPIC")                  # e.g. projects/.../topics/cdmnxt-trigger-topic
DEST_PROJECT_ID        = os.getenv("DEST_PROJECT_ID")               # STS project
DEST_BUCKET            = os.getenv("DEST_BUCKET")
DEST_PATH              = os.getenv("DEST_PATH", "")
SOURCE_BUCKET          = os.getenv("SOURCE_BUCKET")                 # (from env, not TOC)
DELETE_AFTER_TRANSFER  = os.getenv("DELETE_AFTER_TRANSFER", "false").lower() == "true"
NOTIF_TOPIC_FQN        = os.getenv("NOTIFICATION_TOPIC_FQN")        # projects/.../topics/sts-completion-topic
AIRFLOW_API_URL        = os.getenv("AIRFLOW_API_URL")               # https://<composer-webserver>/api/v1
AIRFLOW_DAG_ID         = os.getenv("AIRFLOW_DAG_ID")                # e.g. gdw_sts_event_final
IAP_CLIENT_ID          = os.getenv("IAP_CLIENT_ID")                 # If behind IAP: the OAuth client ID
BUFFER_SECONDS         = int(os.getenv("BUFFER_SECONDS", "0"))      # optional: set 300 for 5 min
MAX_BATCH              = int(os.getenv("MAX_BATCH", "100"))         # optional: cap

storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()

# In-memory buffer (best-effort).
_BUFFER: List[Dict] = []
_LAST_FLUSH = 0.0

# ===== Helpers =====
def _read_toc(toc_bucket: str, toc_name: str) -> Dict:
    content = storage_client.bucket(toc_bucket).blob(toc_name).download_as_text()
    return json.loads(content)

def _validate_toc(toc: Dict):
    for k in ["cdm_process_id", "app_id", "cdm_object_mapping", "type", "businessProcessingDateTime"]:
        if k not in toc:
            raise ValueError(f"TOC missing required key: {k}")
    if not isinstance(toc["cdm_object_mapping"], list) or not toc["cdm_object_mapping"]:
        raise ValueError("TOC.cdm_object_mapping must be non-empty list")

def _collect_include_prefixes(tocs: List[Dict]) -> List[str]:
    prefixes = []
    for t in tocs:
        for m in t["cdm_object_mapping"]:
            for o in m.get("dataobject", []):
                # Use object names exactly as keys to include
                prefixes.append(o["name"])
    # de-dup but preserve order
    seen = set(); dedup=[]
    for p in prefixes:
        if p not in seen:
            seen.add(p); dedup.append(p)
    return dedup

def _make_unique_job_name() -> str:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    return f"event_sts_job_{ts}_{uuid.uuid4().hex[:8]}"

def _build_sts_body(include_prefixes: List[str], job_name: str) -> Dict:
    return {
        "transferJob": {
            "name": job_name,  # unique name per run
            "description": f"Event STS job {job_name}",
            "status": "ENABLED",
            "transferSpec": {
                "gcsDataSource": {
                    "bucketName": SOURCE_BUCKET
                },
                "gcsDataSink": {
                    "bucketName": DEST_BUCKET,
                    "path": DEST_PATH
                },
                "objectConditions": {
                    "includePrefixes": include_prefixes
                },
                "transferOptions": {
                    "overwriteObjectsAlreadyExistingInSink": True,
                    "deleteObjectsFromSourceAfterTransfer": DELETE_AFTER_TRANSFER
                }
            },
            "notificationConfig": {
                "pubsubTopic": NOTIF_TOPIC_FQN,  # FQN required by STS
                "eventTypes": ["TRANSFER_OPERATION_SUCCESS", "TRANSFER_OPERATION_FAILED"],
                "payloadFormat": "JSON"
            }
        }
    }

def _iap_id_token(url: str) -> str:
    # If your Composer is behind IAP; otherwise you can use other auth approaches.
    request = gauth_requests.Request()
    return id_token.fetch_id_token(request, IAP_CLIENT_ID)

def _trigger_airflow_dag(conf: Dict, dag_run_id: str):
    # POST /api/v1/dags/{dag_id}/dagRuns
    url = f"{AIRFLOW_API_URL}/dags/{AIRFLOW_DAG_ID}/dagRuns"
    headers = {"Content-Type": "application/json"}
    if IAP_CLIENT_ID:
        headers["Authorization"] = f"Bearer {_iap_id_token(url)}"
    payload = {"dag_run_id": dag_run_id, "conf": conf}
    resp = requests.post(url, headers=headers, json=payload, timeout=60)
    if resp.status_code not in (200, 201, 409):  # 409 if same dag_run_id exists
        raise RuntimeError(f"Airflow trigger failed: {resp.status_code} {resp.text}")

def _flush(batch_events: List[Dict]):
    """Process a buffered batch: read all TOCs, build a single STS body with includePrefixes, trigger the DAG."""
    tocs = []
    toc_pointers = []
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
    _trigger_airflow_dag(conf, dag_run_id)

def main(event, context):
    global _LAST_FLUSH, _BUFFER
    # event from APMF CF via Pub/Sub: contains bucket/name (original), and toc_bucket/toc_name pointer
    if "data" in event:
        msg = json.loads(base64.b64decode(event["data"]).decode("utf-8"))
    else:
        msg = event  # direct call

    # Expect: {bucket, name, toc_bucket, toc_name}
    for k in ("toc_bucket", "toc_name"):
        if k not in msg:
            raise ValueError(f"Missing '{k}' in message")

    _BUFFER.append({"toc_bucket": msg["toc_bucket"], "toc_name": msg["toc_name"]})
    now = time.time()
    if _LAST_FLUSH == 0:
        _LAST_FLUSH = now

    # Flush either immediately if no buffering OR when buffer window elapsed
    if BUFFER_SECONDS == 0 or (now - _LAST_FLUSH) >= BUFFER_SECONDS:
        batch = _BUFFER
        _BUFFER = []
        _LAST_FLUSH = now
        if batch:
            _flush(batch)



from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCreateJobOperator,
    # CloudDataTransferServiceDeleteJobOperator   # (commented for future)
)
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from google.cloud import storage
import json

# ---- Helpers that read only from dag_run.conf (no hardcoding) ----

def _get_conf(ti):
    # Safe loader for conf
    dag_run_conf = ti.dag_run.conf or {}
    required = ["project_id", "completion_subscription", "cdmnxt_topic", "job_name", "sts_body", "toc_files"]
    missing = [k for k in required if k not in dag_run_conf]
    if missing:
        raise ValueError(f"Missing in dag_run.conf: {missing}")
    return dag_run_conf

def _get_project_id(**kwargs):
    return _get_conf(kwargs["ti"])["project_id"]

def _get_completion_subscription(**kwargs):
    return _get_conf(kwargs["ti"])["completion_subscription"]

def _get_sts_body(**kwargs):
    return _get_conf(kwargs["ti"])["sts_body"]

def _get_job_name(**kwargs):
    return _get_conf(kwargs["ti"])["job_name"]

def _messages_filter_callback(messages, context):
    """Ack only messages whose payload jobName endswith our job_name."""
    ti = context["ti"]
    job_name = _get_job_name(ti=ti)
    matched = []
    for m in messages:
        try:
            payload = json.loads(m["message"]["data"])
            # STS typically includes full job name; we match suffix or exact.
            job = payload.get("jobName", "")
            if job.endswith(job_name) or job == job_name:
                matched.append(m)
        except Exception:
            continue
    # If we found a match, return only those; sensor will ack them because ack_messages=True.
    return matched if matched else []

def _publish_tocs_to_cdmnxt(**kwargs):
    ti = kwargs["ti"]
    conf = _get_conf(ti)
    topic_fqn = conf["cdmnxt_topic"]            # projects/.../topics/...
    project_id = conf["project_id"]
    toc_files = conf["toc_files"]               # list of {toc_bucket, toc_name}

    # Publish each TOC JSON (as-is) to CDMNXt topic
    from google.cloud import pubsub_v1
    publisher = pubsub_v1.PublisherClient()

    # If full FQN is passed, keep; otherwise construct
    if topic_fqn.startswith("projects/"):
        topic_path = topic_fqn
    else:
        topic_path = publisher.topic_path(project_id, topic_fqn)

    storage_client = storage.Client()
    for ptr in toc_files:
        bucket = storage_client.bucket(ptr["toc_bucket"])
        text = bucket.blob(ptr["toc_name"]).download_as_text()
        # Send the original TOC JSON as message body (no extra fields)
        publisher.publish(topic_path, text.encode("utf-8")).result()

    print(f"Published {len(toc_files)} TOC JSONs to {topic_path}")

# ---- DAG ----

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="gdw_sts_event_final",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,     # event-driven via REST
    catchup=False,
    max_active_runs=10,         # allow concurrent runs
    tags=["sts", "gdw", "event"],
) as dag:

    # 1) Create STS job with the body provided in conf (includes unique job_name)
    create_sts_job = CloudDataTransferServiceCreateJobOperator(
        task_id="create_sts_job",
        project_id=_get_project_id,     # pulled dynamically
        body=_get_sts_body,             # pulled dynamically (dict)
    )

    # 2) Wait for completion by listening for the matching job_name
    wait_for_sts_completion = PubSubPullSensor(
        task_id="wait_for_sts_completion",
        project_id=_get_project_id,
        subscription=_get_completion_subscription,  # projects/.../subscriptions/...
        ack_messages=True,
        messages_callback=_messages_filter_callback, # filter by job_name
        timeout=60 * 30,             # 30 min max
        poke_interval=20,
        max_messages=10              # poll up to 10 each poke
    )

    # 3) Publish original TOC JSON(s) (loaded from GCS) to CDMNXt topic
    publish_toc_to_cdmnxt = PythonOperator(
        task_id="publish_toc_to_cdmnxt",
        python_callable=_publish_tocs_to_cdmnxt,
        provide_context=True,
    )

    # --------------------- (Commented) STS Delete Job block ---------------------
    # from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceDeleteJobOperator
    #
    # delete_sts_job = CloudDataTransferServiceDeleteJobOperator(
    #     task_id="delete_sts_job",
    #     project_id=_get_project_id,
    #     job_name=_get_job_name,          # same unique name set by CF
    #     trigger_rule="all_done",
    # )
    # ----------------------------------------------------------------------------

    # ------------------- (Commented) BigQuery Logging block ---------------------
    # def _log_result_to_bq(**kwargs):
    #     """
    #     Placeholder to log success/failure and metadata to BigQuery.
    #     Fill in table_id and row schema per your governance.
    #     """
    #     from google.cloud import bigquery
    #     ti = kwargs["ti"]
    #     conf = ti.dag_run.conf or {}
    #     table_id = "your-project.your_dataset.sts_runs"
    #     row = {
    #         "dag_run_id": ti.run_id,
    #         "job_name": conf.get("job_name"),
    #         "project_id": conf.get("project_id"),
    #         "completed_at": kwargs["ts"],
    #         "status": "SUCCESS"
    #     }
    #     client = bigquery.Client()
    #     errors = client.insert_rows_json(table_id, [row])
    #     if errors:
    #         raise RuntimeError(f"BQ insert errors: {errors}")
    #
    # log_to_bq = PythonOperator(
    #     task_id="log_to_bq",
    #     python_callable=_log_result_to_bq,
    #     provide_context=True,
    #     trigger_rule="all_done",
    # )
    # ----------------------------------------------------------------------------

    # Flow
    create_sts_job >> wait_for_sts_completion >> publish_toc_to_cdmnxt
    # wait_for_sts_completion >> delete_sts_job
    # wait_for_sts_completion >> log_to_bq

