
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceRunJobOperator,
    CloudDataTransferServiceDeleteJobOperator,
)
from airflow.models import Variable
from google.cloud import storage, pubsub_v1
import json
import uuid
import os

PROJECT_ID = "sandbox-corp-gdw-sfr-cdb8"
DESTINATION_BUCKET = "gdw2-sandbox-corp-gdw-sfr-cdb8"
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
    grouped = kwargs["ti"].xcom_pull(key="grouped_messages", task_ids="parse_cf_messages")
    jobs = []
    for file in grouped.get(delete_flag, []):
        job = {
            "projectId": PROJECT_ID,
            "transferSpec": {
                "gcsDataSource": {"bucketName": file["source_bucket"]},
                "gcsDataSink": {"bucketName": DESTINATION_BUCKET},
                "objectConditions": {"includePrefixes": [file["object_name"]]},
                "transferOptions": {"deleteObjectsFromSourceAfterTransfer": file["delete_after_transfer"]},
            },
            "description": f"sts-job-{uuid.uuid4()}",
            "status": "ENABLED",
            "schedule": {
                "scheduleStartDate": {"year": 2025, "month": 7, "day": 30},
                "scheduleEndDate": {"year": 2025, "month": 7, "day": 30},
            },
        }
        jobs.append(job)
    kwargs["ti"].xcom_push(key=f"job_bodies_{delete_flag}", value=jobs)


def get_sts_body(delete_flag, **kwargs):
    return kwargs["ti"].xcom_pull(task_ids=f"build_job_{delete_flag}", key=f"job_bodies_{delete_flag}")


def get_toc_filename(delete_flag, **kwargs):
    return f"toc_{delete_flag}_{uuid.uuid4()}.json"


with DAG("gdw_sts_event_driven_dag", default_args=default_args, schedule_interval=None, catchup=False) as dag:

    parse_cf = PythonOperator(
        task_id="parse_cf_messages",
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
        body=get_sts_body("true"),
    )

    create_job_false = CloudDataTransferServiceCreateJobOperator(
        task_id="create_job_false",
        body=get_sts_body("false"),
    )

    parse_cf >> [build_job_true, build_job_false] >> [create_job_true, create_job_false]
