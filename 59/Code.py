from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator

import json
import base64
import uuid
from datetime import timedelta

PROJECT_ID = "sandbox-corp-gdw-sfr-cdb8"
SUBSCRIPTION_NAME = "cf-trigger-dag"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="cf_message_to_sts_job_v1",
    default_args=default_args,
    description="Parse CF message and create STS job dynamically",
    schedule_interval=None,
    catchup=False,
    start_date=days_ago(1),
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
                "gcsDataSource": {
                    "bucketName": source_bucket,
                },
                "gcsDataSink": {
                    "bucketName": "gdw-target-destination-bucket",  # TODO: Replace with your destination bucket
                },
                "objectConditions": {
                    "includePrefixes": [prefix],
                },
                "transferOptions": {
                    "deleteObjectsFromSourceAfterTransfer": delete_source,
                },
            },
            "schedule": {
                "scheduleStartDate": {
                    "year": 2025,
                    "month": 7,
                    "day": 31,
                },
                "scheduleEndDate": {
                    "year": 2025,
                    "month": 7,
                    "day": 31,
                },
            },
        }

        return job_body

    create_sts_job = CloudDataTransferServiceCreateJobOperator(
        task_id="create_sts_job",
        project_id=PROJECT_ID,
        body=parse_cf_and_build_sts_body,
    )

    wait_for_cf_message >> create_sts_job
