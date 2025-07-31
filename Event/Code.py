from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator

import json
import base64

PROJECT_ID = "sandbox-corp-gdw-sfr-cd8b"
SUBSCRIPTION_NAME = "cf-trigger-dag"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


def parse_cf_and_build_sts_body(**kwargs):
    ti = kwargs["ti"]
    messages = ti.xcom_pull(task_ids="wait_for_cf_message")

    if not messages:
        raise ValueError("No messages pulled from Pub/Sub.")

    message = messages[0]
    raw_data = message.get("message", {}).get("data", "")
    if not raw_data:
        raise ValueError("Message has no data field.")

    decoded = base64.b64decode(raw_data).decode("utf-8")
    parsed = json.loads(decoded)

    print("✅ Decoded CF message:")
    print(json.dumps(parsed, indent=2))

    file_event = parsed["file_events"][0]
    source_bucket = file_event["cdm_source_bucket"]
    prefix = file_event["cdm_file_prefix_pattern"]
    source_project_id = file_event["cdm_source_project_id"]
    delete_source = file_event["delete_source_files_after_transfer"]

    job_body = {
        "description": "STS job from CF message",
        "status": "ENABLED",
        "projectId": PROJECT_ID,
        "transferSpec": {
            "gcsDataSource": {"bucketName": source_bucket},
            "gcsDataSink": {"bucketName": "gdw2-sandbox-corp-gdw-sfr-cd8b"},
            "objectConditions": {"includePrefixes": [prefix]},
            "transferOptions": {"deleteObjectsFromSourceAfterTransfer": delete_source},
        },
        "schedule": {
            "scheduleStartDate": {"year": 2025, "month": 7, "day": 31},
            "scheduleEndDate": {"year": 2025, "month": 7, "day": 31},
        },
    }

    ti.xcom_push(key="sts_job_body", value=job_body)


def get_sts_body_from_xcom(**kwargs):
    return kwargs["ti"].xcom_pull(task_ids="build_sts_body", key="sts_job_body")


with DAG(
    dag_id="Five9_to_CDM_Test",
    default_args=default_args,
    description="Parse CF message and print to logs",
    schedule_interval=None,
    catchup=False,
    start_date=datetime(2023, 1, 1),
    tags=["cf", "sts", "apmf"],
) as dag:

    wait_for_cf_message = PubSubPullSensor(
        task_id="wait_for_cf_message",
        project_id=PROJECT_ID,
        subscription=SUBSCRIPTION_NAME,
        ack_messages=True,
        max_messages=1,
        timeout=60,
    )

    build_sts_body = PythonOperator(
        task_id="build_sts_body",
        python_callable=parse_cf_and_build_sts_body,
    )

    create_sts_job = CloudDataTransferServiceCreateJobOperator(
        task_id="create_sts_job",
        project_id=PROJECT_ID,
        body=get_sts_body_from_xcom,
    )

    wait_for_cf_message >> build_sts_body >> create_sts_job
