
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from datetime import datetime

# Extracted from CF message (normally from sensor-parsed logic)
source_bucket = "apmf-bucket"
prefix = "path/to/files/"
delete_flag = True

# Constants
PROJECT_ID = "gdw-project"
DEST_BUCKET = "gdw-destination-bucket"
SUBSCRIPTION_NAME = "cf-trigger-sub"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="cf_sts_direct_body_dag",
    default_args=default_args,
    description="Inline body STS job from parsed CF message",
    schedule_interval=None,
    start_date=datetime(2025, 7, 30),
    catchup=False,
    tags=["cf", "sts", "inline"],
) as dag:

    wait_for_cf_message = PubSubPullSensor(
        task_id="wait_for_cf_message",
        project_id=PROJECT_ID,
        subscription=SUBSCRIPTION_NAME,
        ack_messages=True,
        max_messages=1,
        timeout=60,
    )

    create_sts_job = CloudDataTransferServiceCreateJobOperator(
        task_id="create_sts_job",
        project_id=PROJECT_ID,
        body={
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
        },
    )

    wait_for_cf_message >> create_sts_job
