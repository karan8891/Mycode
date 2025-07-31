from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from datetime import datetime
import base64
import json

# Global variables to be filled from CF message
cdm_source_project_id = None
cdm_source_bucket = None
cdm_file_prefix_pattern = None
delete_source_files_after_transfer = None

PROJECT_ID = "your-gcp-project-id"
SUBSCRIPTION_NAME = "cf-trigger-sub"
DESTINATION_BUCKET = "your-destination-bucket"

def extract_cf_message(**kwargs):
    global cdm_source_project_id, cdm_source_bucket, cdm_file_prefix_pattern, delete_source_files_after_transfer
    messages = kwargs["ti"].xcom_pull(task_ids="wait_for_cf_message")
    decoded = base64.b64decode(messages[0]["message"]["data"]).decode("utf-8")
    parsed = json.loads(decoded)
    file_event = parsed["file_events"][0]

    cdm_source_project_id = file_event["cdm_source_project_id"]
    cdm_source_bucket = file_event["cdm_source_bucket"]
    cdm_file_prefix_pattern = file_event["cdm_file_prefix_pattern"]
    delete_source_files_after_transfer = file_event["delete_source_files_after_transfer"]

default_args = {
    "start_date": datetime(2025, 7, 30),
}

with DAG(
    "cf_sts_global_json_body_dag",
    default_args=default_args,
    description="Parse CF message and create STS job with global vars",
    schedule_interval=None,
    catchup=False,
    tags=["cf", "sts", "event"],
) as dag:

    wait_for_cf_message = PubSubPullSensor(
        task_id="wait_for_cf_message",
        project_id=PROJECT_ID,
        subscription=SUBSCRIPTION_NAME,
        ack_messages=True,
        max_messages=1,
        timeout=60,
    )

    extract_cf_fields = PythonOperator(
        task_id="extract_cf_fields",
        python_callable=extract_cf_message,
    )

    create_sts_job = CloudDataTransferServiceCreateJobOperator(
        task_id="create_sts_job",
        project_id=PROJECT_ID,
        body={
            "description": "STS job from CF message",
            "status": "ENABLED",
            "projectId": PROJECT_ID,
            "transferSpec": {
                "gcsDataSource": {"bucketName": cdm_source_bucket},
                "gcsDataSink": {"bucketName": DESTINATION_BUCKET},
                "objectConditions": {"includePrefixes": [cdm_file_prefix_pattern]},
                "transferOptions": {
                    "deleteObjectsFromSourceAfterTransfer": delete_source_files_after_transfer
                },
            },
            "schedule": {
                "scheduleStartDate": {"year": 2025, "month": 7, "day": 31},
                "scheduleEndDate": {"year": 2025, "month": 7, "day": 31},
            },
        },
    )

    wait_for_cf_message >> extract_cf_fields >> create_sts_job
