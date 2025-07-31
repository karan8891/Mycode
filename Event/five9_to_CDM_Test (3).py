
from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
import json

# Constants
PROJECT_ID = "your-gcp-project-id"
SUBSCRIPTION = "sts-completion-sub"
DEST_BUCKET = "your-destination-bucket"

with models.DAG(
    dag_id="five9_to_CDM_Test",
    schedule_interval=None,
    start_date=datetime(2025, 7, 30),
    catchup=False,
    tags=["sts", "event", "cf"],
) as dag:

    pull_cf_message = PubSubPullSensor(
        task_id="pull_cf_message",
        project_id=PROJECT_ID,
        subscription=SUBSCRIPTION,
        max_messages=1,
        ack_messages=True,
        timeout=30,
    )

    def parse_and_create_sts_job(messages, **kwargs):
        if not messages:
            raise ValueError("No messages received from Pub/Sub.")

        raw_message = messages[0]
        if not raw_message:
            raise ValueError("Empty message received from Pub/Sub.")

        if isinstance(raw_message, str):
            cf_message = json.loads(raw_message)
        elif isinstance(raw_message, dict):
            cf_message = raw_message
        else:
            raise ValueError("Invalid message format from Pub/Sub.")

        source_project_id = cf_message["cdm_source_project_id"]
        source_bucket = cf_message["cdm_source_bucket"]
        prefix = cf_message["cdm_file_prefix_pattern"]
        delete_flag = cf_message["delete_source_files_after_transfer"]

        return CloudDataTransferServiceCreateJobOperator(
            task_id="create_sts_job",
            project_id=PROJECT_ID,
            body={
                "description": "STS Job from CF message",
                "status": "ENABLED",
                "projectId": source_project_id,
                "transferSpec": {
                    "gcsDataSource": {"bucketName": source_bucket},
                    "gcsDataSink": {"bucketName": DEST_BUCKET},
                    "objectConditions": {"includePrefixes": [prefix]},
                    "transferOptions": {
                        "deleteObjectsFromSourceAfterTransfer": delete_flag
                    },
                },
                "schedule": {
                    "scheduleStartDate": {
                        "year": datetime.utcnow().year,
                        "month": datetime.utcnow().month,
                        "day": datetime.utcnow().day,
                    },
                    "startTimeOfDay": {
                        "hours": datetime.utcnow().hour,
                        "minutes": datetime.utcnow().minute,
                        "seconds": datetime.utcnow().second,
                    },
                },
            },
        )

    from airflow.operators.python import PythonOperator

    create_sts_job = PythonOperator(
        task_id="parse_and_create_sts_job",
        python_callable=parse_and_create_sts_job,
        op_args=["{{ task_instance.xcom_pull(task_ids='pull_cf_message') }}"],
    )

    pull_cf_message >> create_sts_job
