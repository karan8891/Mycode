from airflow import DAG
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

import json
import base64
from datetime import datetime

PROJECT_ID = "your-project-id"
DEST_BUCKET = "your-destination-bucket"
SUBSCRIPTION = "sts-completion-sub"

with DAG(
    dag_id="five9_to_cdm_test",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["sts", "cf", "event"],
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

        payload = messages[0]
        if not payload:
            raise ValueError("Empty Pub/Sub payload.")

        decoded_data = base64.b64decode(payload).decode("utf-8")
        cf_message = json.loads(decoded_data)

        file_event = cf_message["file_events"][0]

        source_project_id = file_event["cdm_source_project_id"]
        source_bucket = file_event["cdm_source_bucket"]
        prefix = file_event["cdm_file_prefix_pattern"]
        delete_flag = file_event["delete_source_files_after_transfer"]

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
        ).execute(context=kwargs)

    create_job_task = PythonOperator(
        task_id="parse_and_create_sts_job",
        python_callable=parse_and_create_sts_job,
        op_args=["{{ task_instance.xcom_pull(task_ids='pull_cf_message') }}"],
    )

    pull_cf_message >> create_job_task
