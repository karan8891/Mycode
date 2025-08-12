# cdm_sts_test.py  (only the parts you need to change)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from datetime import timedelta, datetime
import json, requests
import google.auth
from google.auth.transport.requests import Request as GAuthRequest
from google.cloud import storage
import dummy_aws_base_hook  # keep so google provider import doesn't break

# ---- helpers ----
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
        creds.refresh(GAuthRequest())
    return creds.token

def _create_sts_job(**kwargs):
    """
    Part 1 (single task): pull all values, build/validate transfer spec,
    and create the STS job via REST.
    """
    ti = kwargs["ti"]
    conf = _conf(ti)

    project_id = conf["project_id"]
    job_name   = conf["job_name"]
    body       = conf["sts_body"]  # already assembled by CF; we can still sanity check

    # Optional light validation to catch obvious shape errors
    if "transferJob" not in body or "transferSpec" not in body["transferJob"]:
        raise ValueError("sts_body must contain transferJob.transferSpec")

    # Force the unique name we’re using to correlate completion
    body["transferJob"]["name"] = job_name

    # Create job via REST (works on any Composer image; no extra libs)
    url = "https://storagetransfer.googleapis.com/v1/transferJobs"
    headers = {
        "Authorization": f"Bearer {_access_token()}",
        "Content-Type": "application/json",
        "X-Goog-User-Project": project_id,  # billing project
    }
    resp = requests.post(url, headers=headers, json=body, timeout=60)

    # 409 means a job with the same name already exists — treat as OK (idempotent)
    if resp.status_code not in (200, 201, 409):
        raise RuntimeError(f"STS create failed: {resp.status_code} {resp.text}")

    # XCom for downstream tasks (optional)
    ti.xcom_push(key="job_name", value=job_name)
    ti.xcom_push(key="project_id", value=project_id)
    ti.xcom_push(key="completion_subscription", value=conf["completion_subscription"])
    ti.xcom_push(key="cdmnxt_topic", value=conf["cdmnxt_topic"])
    ti.xcom_push(key="toc_files", value=conf["toc_files"])
    print(f"STS job created (or already existed): {job_name}")

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

def _publish_tocs_to_cdmnxt(**kwargs):
    ti = kwargs["ti"]
    project_id = ti.xcom_pull(task_ids="create_sts_job", key="project_id")
    topic_fqn  = ti.xcom_pull(task_ids="create_sts_job", key="cdmnxt_topic")
    toc_files  = ti.xcom_pull(task_ids="create_sts_job", key="toc_files")

    from google.cloud import pubsub_v1
    pub = pubsub_v1.PublisherClient()
    topic_path = topic_fqn if topic_fqn.startswith("projects/") else pub.topic_path(project_id, topic_fqn)

    storage_client = storage.Client()
    for p in toc_files:
        text = storage_client.bucket(p["toc_bucket"]).blob(p["toc_name"]).download_as_text()
        pub.publish(topic_path, text.encode("utf-8")).result()
    print(f"Published {len(toc_files)} TOC JSONs to {topic_path}")

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=1)}

with DAG(
    dag_id="event_sts_transfer_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=10,
    tags=["sts","gdw","event"],
) as dag:

    # Part 1: build + create STS job (single Python task)
    create_sts_job = PythonOperator(
        task_id="create_sts_job",
        python_callable=_create_sts_job,
        provide_context=True,
    )

    # Part 2: wait for completion for that exact job
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

    # Part 3: publish original TOC JSON(s) to CDMNXt
    publish_toc_to_cdmnxt = PythonOperator(
        task_id="publish_toc_to_cdmnxt",
        python_callable=_publish_tocs_to_cdmnxt,
        provide_context=True,
    )

    create_sts_job >> wait_for_sts_completion >> publish_toc_to_cdmnxt

    # (Your commented Delete Job and BQ logging blocks can remain unchanged)
