
from airflow import models
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

import base64
import json
from datetime import datetime, timedelta

PROJECT_ID = "gdw-dev-abcd"
DEST_BUCKET = "cdm-datalake-raw-dev-gcs"
PUBSUB_PROJECT = "gdw-dev-abcd"
SUBSCRIPTION = "sts-completion-sub"

with models.DAG(
    dag_id="sts_cf_inline_pull_v3",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
) as dag:

    pull_cf_message = PubSubPullSensor(
        task_id="pull_cf_message",
        project_id=PUBSUB_PROJECT,
        subscription=SUBSCRIPTION,
        max_messages=1,
        ack_messages=True,
        timeout=30,
    )

    def parse_and_create_sts_job(messages, **kwargs):
        if not messages:
            raise ValueError("No messages received from Pub/Sub.")

        payload = messages[0].get("message", {}).get("data")
        if not payload:
            raise ValueError("No data in Pub/Sub message.")

        decoded_data = base64.b64decode(payload).decode("utf-8")
        cf_message = json.loads(decoded_data)

        file_event = cf_message["file_events"][0]
        source_project_id = file_event["cdm_source_project_id"]
        source_bucket = file_event["cdm_source_bucket"]
        prefix = file_event["cdm_file_prefix_pattern"]
        delete_flag = file_event["delete_source_files_after_transfer"]

        job_operator = CloudDataTransferServiceCreateJobOperator(
            task_id="create_sts_job",
            project_id=PROJECT_ID,
            body={
                "description": "STS Job from CF message",
                "status": "ENABLED",
                "projectId": source_project_id,
                "schedule": {
                    "scheduleStartDate": {
                        "year": datetime.utcnow().year,
                        "month": datetime.utcnow().month,
                        "day": datetime.utcnow().day,
                    },
                    "startTimeOfDay": {
                        "hours": datetime.utcnow().hour,
                        "minutes": datetime.utcnow().minute,
                    },
                },
                "transferSpec": {
                    "gcsDataSource": {"bucketName": source_bucket},
                    "gcsDataSink": {"bucketName": DEST_BUCKET},
                    "objectConditions": {"includePrefixes": [prefix]},
                    "transferOptions": {
                        "deleteObjectsFromSourceAfterTransfer": delete_flag
                    },
                },
            },
            dag=dag,
        )
        return job_operator.execute(context=kwargs)

    create_sts_job = PythonOperator(
        task_id="parse_and_create_sts_job",
        python_callable=parse_and_create_sts_job,
        provide_context=True,
        op_kwargs={"messages": "{{ task_instance.xcom_pull(task_ids='pull_cf_message') }}"},
    )

    pull_cf_message >> create_sts_job
