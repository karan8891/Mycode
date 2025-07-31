from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
from datetime import datetime, timedelta
import json

PROJECT_ID = "your-project-id"
SUBSCRIPTION_NAME = "cf-trigger-dag"
DEST_BUCKET = "destination-bucket-name"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="sts_cf_direct_inline_dag",
    default_args=default_args,
    start_date=datetime(2025, 7, 31),
    schedule_interval=None,
    catchup=False
) as dag:

    # 1. Pull CF message
    wait_for_cf = PubSubPullSensor(
        task_id="wait_for_cf",
        project_id=PROJECT_ID,
        subscription=SUBSCRIPTION_NAME,
        ack_messages=True,
        max_messages=1,
        timeout=60,
    )

    # 2. Parse CF message and create STS job
    def create_sts_job_from_cf_message(ti, **kwargs):
        messages = ti.xcom_pull(task_ids="wait_for_cf", key="messages")
        cf_message = json.loads(messages[0]['message']['data'])

        source_bucket = cf_message['cdm_source_bucket']
        prefix = cf_message['cdm_file_prefix_pattern']
        delete_flag = cf_message['delete_source_files_after_transfer']

        job_body = {
            "description": "CF-triggered STS job",
            "status": "ENABLED",
            "projectId": PROJECT_ID,
            "transferSpec": {
                "gcsDataSource": {
                    "bucketName": source_bucket
                },
                "gcsDataSink": {
                    "bucketName": DEST_BUCKET
                },
                "objectConditions": {
                    "includePrefixes": [prefix]
                },
                "transferOptions": {
                    "deleteObjectsFromSourceAfterTransfer": delete_flag
                }
            },
            "schedule": {
                "scheduleStartDate": {
                    "year": datetime.now().year,
                    "month": datetime.now().month,
                    "day": datetime.now().day
                },
                "scheduleEndDate": {
                    "year": datetime.now().year,
                    "month": datetime.now().month,
                    "day": datetime.now().day
                },
                "startTimeOfDay": {
                    "hours": datetime.now().hour,
                    "minutes": datetime.now().minute + 1
                }
            }
        }

        from google.cloud import storage_transfer_v1
        client = storage_transfer_v1.StorageTransferServiceClient()
        client.create_transfer_job({"transferJob": job_body})

    create_sts = PythonOperator(
        task_id="create_sts_from_cf",
        python_callable=create_sts_job_from_cf_message
    )

    wait_for_cf >> create_sts
