
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceUpdateJobOperator,
    CloudDataTransferServiceDeleteJobOperator,
)
from airflow.models import Variable
import json
import uuid

PROJECT_ID = "your-gcp-project-id"
CF_TRIGGER_SUBSCRIPTION = "cf-trigger-sub"
STS_COMPLETION_SUBSCRIPTION = "sts-completion-sub"
CDMNXT_TOPIC = "cdmnxt-topic"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 30),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def parse_cf_messages(**kwargs):
    messages = Variable.get("enriched_cf_messages", deserialize_json=True)
    grouped = {"true": [], "false": []}
    for msg in messages:
        grouped[str(msg["delete_after_transfer"]).lower()].append(msg)
    kwargs["ti"].xcom_push(key="grouped_messages", value=grouped)

def build_sts_job_body(delete_flag, **kwargs):
    grouped = kwargs["ti"].xcom_pull(key="grouped_messages", task_ids="parse_cf")
    jobs = []
    for file in grouped.get(delete_flag, []):
        job = {
            "projectId": PROJECT_ID,
            "transferSpec": {
                "gcsDataSource": {"bucketName": file["source_bucket"]},
                "gcsDataSink": {"bucketName": file["destination_bucket"]},
                "objectConditions": {"includePrefixes": [file["object_name"]]},
                "transferOptions": {"deleteObjectsFromSourceAfterTransfer": file["delete_after_transfer"]},
            },
            "description": f"sts-job-{uuid.uuid4()}",
            "status": "ENABLED",
            "schedule": {"scheduleStartDate": {"year": 2025, "month": 7, "day": 30},
                         "scheduleEndDate": {"year": 2025, "month": 7, "day": 30}},
        }
        jobs.append(job)
    kwargs["ti"].xcom_push(key=f"job_bodies_{delete_flag}", value=jobs)

def return_sts_job_bodies(delete_flag, **kwargs):
    return kwargs["ti"].xcom_pull(task_ids=f"build_job_{delete_flag}", key=f"job_bodies_{delete_flag}")

def return_job_name(task_id, **kwargs):
    job = kwargs["ti"].xcom_pull(task_ids=task_id)
    return job["name"]

def notify_cdmnxt_topic(toc_file, **kwargs):
    print(f"Publishing {toc_file} to {CDMNXT_TOPIC} (or log if not found)")

with DAG(
    "gdw_event_sts_final_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["sts", "event-driven"],
) as dag:

    parse_cf = PythonOperator(
        task_id="parse_cf",
        python_callable=parse_cf_messages
    )

    build_job_true = PythonOperator(
        task_id="build_job_true",
        python_callable=build_sts_job_body,
        op_kwargs={"delete_flag": "true"},
    )

    build_job_false = PythonOperator(
        task_id="build_job_false",
        python_callable=build_sts_job_body,
        op_kwargs={"delete_flag": "false"},
    )

    create_job_true = CloudDataTransferServiceCreateJobOperator(
        task_id="create_job_true",
        body=return_sts_job_bodies("true"),
        project_id=PROJECT_ID,
    )

    create_job_false = CloudDataTransferServiceCreateJobOperator(
        task_id="create_job_false",
        body=return_sts_job_bodies("false"),
        project_id=PROJECT_ID,
    )

    update_job_true = CloudDataTransferServiceUpdateJobOperator(
        task_id="update_job_true",
        body={},
        project_id=PROJECT_ID,
        job_name=lambda **kwargs: return_job_name("create_job_true", **kwargs),
    )

    update_job_false = CloudDataTransferServiceUpdateJobOperator(
        task_id="update_job_false",
        body={},
        project_id=PROJECT_ID,
        job_name=lambda **kwargs: return_job_name("create_job_false", **kwargs),
    )

    wait_true = PubSubPullSensor(
        task_id="wait_completion_true",
        project_id=PROJECT_ID,
        subscription=STS_COMPLETION_SUBSCRIPTION,
        ack_messages=True,
        max_messages=10,
        timeout=600,
    )

    wait_false = PubSubPullSensor(
        task_id="wait_completion_false",
        project_id=PROJECT_ID,
        subscription=STS_COMPLETION_SUBSCRIPTION,
        ack_messages=True,
        max_messages=10,
        timeout=600,
    )

    delete_job_true = CloudDataTransferServiceDeleteJobOperator(
        task_id="delete_job_true",
        project_id=PROJECT_ID,
        job_name=lambda **kwargs: return_job_name("create_job_true", **kwargs),
    )

    delete_job_false = CloudDataTransferServiceDeleteJobOperator(
        task_id="delete_job_false",
        project_id=PROJECT_ID,
        job_name=lambda **kwargs: return_job_name("create_job_false", **kwargs),
    )

    notify_toc_true = PythonOperator(
        task_id="notify_toc_true",
        python_callable=notify_cdmnxt_topic,
        op_kwargs={"toc_file": "toc_true.csv"},
    )

    notify_toc_false = PythonOperator(
        task_id="notify_toc_false",
        python_callable=notify_cdmnxt_topic,
        op_kwargs={"toc_file": "toc_false.csv"},
    )

    parse_cf >> [build_job_true, build_job_false]
    build_job_true >> create_job_true >> update_job_true >> wait_true >> delete_job_true >> notify_toc_true
    build_job_false >> create_job_false >> update_job_false >> wait_false >> delete_job_false >> notify_toc_false
