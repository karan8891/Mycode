
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from datetime import timedelta, datetime
import json, requests, os, uuid, time
import google.auth
from google.auth.transport.requests import Request as GAAuthRequest
from google.cloud import storage
from google.cloud import pubsub_v1
import dummy_aws_base_hook  # keep so google provider import doesn’t break

# ---------- helpers ----------
def _conf(ti):
    conf = ti.dag_run.conf or {}
    required = ["project_id", "completion_subscription", "cdmnxt_topic", "sts_body", "job_name", "toc_files"]
    miss = [k for k in required if k not in conf]
    if miss:
        raise ValueError(f"Missing in dag_run.conf: {miss}")
    return conf

def _access_token() -> str:
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    if not creds.valid:
        creds.refresh(GAAuthRequest())
    return creds.token

def _normalize_path(*parts: str) -> str:
    return "/".join(s.strip("/") for s in parts if s).strip("/")

def _find_missing_in_dest(sts_body: dict) -> list[str]:
    """Compare includePrefixes against sink; return those not present."""
    spec = sts_body["transferJob"]["transferSpec"]
    dest_bucket = spec["gcsDataSink"]["bucketName"]
    dest_path   = spec["gcsDataSink"].get("path", "")
    prefixes    = spec["objectConditions"].get("includePrefixes", [])
    client = storage.Client()
    bucket = client.bucket(dest_bucket)

    missing: list[str] = []
    for p in prefixes:
        dest_obj = _normalize_path(dest_path, p) if dest_path else p
        if not bucket.blob(dest_obj).exists():
            missing.append(p)
    return missing

def _wait_for_job_completion_sync(subscription_fqn: str, job_name: str,
                                  timeout_sec: int = 30*60, batch: int = 10, sleep_s: int = 20):
    """Block until we see a Pub/Sub notification for the specific job_name."""
    sub = pubsub_v1.SubscriberClient()
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        resp = sub.pull(request={"subscription": subscription_fqn, "max_messages": batch}, timeout=10)
        ack_ids = []
        for rm in resp.received_messages:
            try:
                payload = json.loads(rm.message.data.decode("utf-8"))
                job = payload.get("jobName", "")
                if job.endswith(job_name) or job == job_name:
                    ack_ids.append(rm.ack_id)
            except Exception:
                pass  # ignore junk
        if ack_ids:
            sub.acknowledge(request={"subscription": subscription_fqn, "ack_ids": ack_ids})
            return
        time.sleep(sleep_s)
    raise RuntimeError("Timed out waiting for restarted STS job completion")

def _restart_missing_job_sync(**kwargs):
    """
    After the first STS run finished, check destination for gaps.
    If any includePrefixes are missing, submit a new STS job with only the missing ones
    and wait for its completion.
    """
    ti = kwargs["ti"]
    conf = _conf(ti)
    body = conf["sts_body"]
    completion_sub = conf["completion_subscription"]

    missing = _find_missing_in_dest(body)
    if not missing:
        print("No missing files detected; no restart needed.")
        ti.xcom_push(key="restarted", value=False)
        return

    # clone and narrow includePrefixes
    from copy import deepcopy
    new_body = deepcopy(body)
    job = new_body["transferJob"]
    spec = job["transferSpec"]
    if "objectConditions" not in spec:
        spec["objectConditions"] = {}
    spec["objectConditions"]["includePrefixes"] = missing

    # unique retry job name
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    job["name"] = f'{conf["job_name"]}_retry_{ts}_{uuid.uuid4().hex[:6]}'

    # ensure notifications (FQN)
    if "notificationConfig" not in job and os.getenv("NOTIFICATION_TOPIC_FQN"):
        job["notificationConfig"] = {
            "pubsubTopic": os.getenv("NOTIFICATION_TOPIC_FQN"),
            "eventTypes": ["TRANSFER_OPERATION_SUCCESS", "TRANSFER_OPERATION_FAILED"],
            "payloadFormat": "JSON",
        }

    # create via REST
    url = "https://storagetransfer.googleapis.com/v1/transferJobs"
    headers = {
        "Authorization": f"Bearer {_access_token()}",
        "Content-Type": "application/json",
        "X-Goog-User-Project": conf["project_id"],
    }
    resp = requests.post(url, headers=headers, json=new_body, timeout=60)
    if resp.status_code not in (200, 201, 409):
        raise RuntimeError(f"STS restart failed: {resp.status_code} {resp.text}")

    print(f"Restarted STS for {len(missing)} missing object(s); job: {job['name']}")

    _wait_for_job_completion_sync(completion_sub, job["name"])

    ti.xcom_push(key="restarted", value=True)
    ti.xcom_push(key="restart_job_name", value=job["name"])

# ---------- part 1: create job ----------
def _create_sts_job(**kwargs):
    """
    Part 1 (single task): pull all values, build/validate transfer spec,
    and create the STS job via REST.
    """
    ti = kwargs["ti"]
    conf = _conf(ti)

    project_id = conf["project_id"]
    job_name   = conf["job_name"]
    body       = conf["sts_body"]         # already assembled by CF

    # light validation
    if "transferJob" not in body or "transferSpec" not in body["transferJob"]:
        raise ValueError("sts_body must contain transferJob.transferSpec")

    # force unique name we’ll correlate on
    body["transferJob"]["name"] = job_name

    url = "https://storagetransfer.googleapis.com/v1/transferJobs"
    headers = {
        "Authorization": f"Bearer {_access_token()}",
        "Content-Type": "application/json",
        "X-Goog-User-Project": project_id,
    }
    resp = requests.post(url, headers=headers, json=body, timeout=60)
    if resp.status_code not in (200, 201, 409):  # 409 = already exists (idempotent)
        raise RuntimeError(f"STS create failed: {resp.status_code} {resp.text}")

    # xcom for downstream tasks
    ti.xcom_push(key="job_name", value=job_name)
    ti.xcom_push(key="project_id", value=project_id)
    ti.xcom_push(key="completion_subscription", value=conf["completion_subscription"])
    ti.xcom_push(key="cdmnxt_topic", value=conf["cdmnxt_topic"])
    ti.xcom_push(key="toc_files", value=conf["toc_files"])
    print(f"STS job created (or already existed): {job_name}")

# ---------- sensor filter ----------
def _messages_filter_callback(messages, context):
    """Ack only messages whose payload jobName endswith our job_name."""
    ti = context["ti"]
    job_name = ti.xcom_pull(task_ids="create_sts_job", key="job_name")
    matched = []
    for m in messages:
        try:
            payload = json.loads(m["message"]["data"])
            job = payload.get("jobName", "")
            if job.endswith(job_name) or job == job_name:
                matched.append(m)
        except Exception:
            continue
    return matched

# ---------- part 3: publish TOCs ----------
def _publish_tocs_to_cdmnxt(**kwargs):
    ti = kwargs["ti"]
    project_id = ti.xcom_pull(task_ids="create_sts_job", key="project_id")
    topic_fqn  = ti.xcom_pull(task_ids="create_sts_job", key="cdmnxt_topic")
    toc_files  = ti.xcom_pull(task_ids="create_sts_job", key="toc_files")

    pub = pubsub_v1.PublisherClient()
    topic_path = topic_fqn if topic_fqn.startswith("projects/") else pub.topic_path(project_id, topic_fqn)

    storage_client = storage.Client()
    for p in toc_files:
        text = storage_client.bucket(p["toc_bucket"]).blob(p["toc_name"]).download_as_text()
        pub.publish(topic_path, text.encode("utf-8")).result()
    print(f"Published {len(toc_files)} TOC JSONs to {topic_path}")

# ---------- DAG ----------
default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=1)}

with DAG(
    dag_id="event_sts_transfer_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=10,
    tags=["sts", "gdw", "event"],
) as dag:

    # Part 1
    create_sts_job = PythonOperator(
        task_id="create_sts_job",
        python_callable=_create_sts_job,
        provide_context=True,
    )

    # Part 2
    wait_for_sts_completion = PubSubPullSensor(
        task_id="wait_for_sts_completion",
        project_id="{{ ti.xcom_pull(task_ids='create_sts_job', key='project_id') }}",
        subscription="{{ ti.xcom_pull(task_ids='create_sts_job', key='completion_subscription') }}",
        ack_messages=True,
        messages_callback=_messages_filter_callback,
        timeout=60*30,
        poke_interval=20,
        max_messages=10,
    )

    # Optional: restart missing files
    restart_missing_if_needed = PythonOperator(
        task_id="restart_missing_if_needed",
        python_callable=_restart_missing_job_sync,
        provide_context=True,
    )

    # Part 3
    publish_toc_to_cdmnxt = PythonOperator(
        task_id="publish_toc_to_cdmnxt",
        python_callable=_publish_tocs_to_cdmnxt,
        provide_context=True,
    )

    create_sts_job >> wait_for_sts_completion >> restart_missing_if_needed >> publish_toc_to_cdmnxt
