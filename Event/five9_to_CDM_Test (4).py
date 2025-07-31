
from airflow import DAG
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import base64
import json

PROJECT_ID = "your-gcp-project-id"
DEST_BUCKET = "destination-bucket-name"
SUBSCRIPTION = "sts-completion-sub"

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="five9_to_CDM_Test",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
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
            print("⚠️ No Pub/Sub message received. Using sample message.")
            cf_message = {
                "cdm_source_bucket": "default-source-bucket",
                "cdm_source_project_id": "default-source-project-id",
                "cdm_file_prefix_pattern": "default-prefix/",
                "delete_source_files_after_transfer": False
            }
        else:
            payload = messages[0]
            if not payload:
                raise ValueError("⚠️ Empty Pub/Sub payload.")
            try:
                decoded_data = base64.b64decode(payload).decode("utf-8")
                cf_message = json.loads(decoded_data)
            except Exception as e:
                print(f"⚠️ Failed to decode message. Using sample message. Error: {str(e)}")
                cf_message = {
                    "cdm_source_bucket": "default-source-bucket",
                    "cdm_source_project_id": "default-source-project-id",
                    "cdm_file_prefix_pattern": "default-prefix/",
                    "delete_source_files_after_transfer": False
                }

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
                    "transferOptions": {"deleteObjectsFromSourceAfterTransfer": delete_flag}
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

    from airflow.operators.python import PythonOperator
    parse_and_create_task = PythonOperator(
        task_id="parse_and_create_sts_job",
        python_callable=parse_and_create_sts_job,
        op_args=["{{ task_instance.xcom_pull(task_ids='pull_cf_message') }}"],
    )

    pull_cf_message >> parse_and_create_task
