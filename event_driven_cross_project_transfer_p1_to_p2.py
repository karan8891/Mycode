from airflow import models
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
from google.cloud import storage
from datetime import datetime

# Project and bucket config
PROJECT_ID = "project-1-id"  # STS job will be created in this project
SOURCE_BUCKET = "bucket-1"   # GCS bucket in Project 1
DEST_BUCKET = "bucket-2"     # GCS bucket in Project 2
TRANSFER_PREFIX = "new-file-prefix/"  # Adjust as needed

def should_trigger_transfer(**kwargs):
    client = storage.Client()
    bucket = client.get_bucket(SOURCE_BUCKET)
    blobs = list(bucket.list_blobs(prefix=TRANSFER_PREFIX))
    return bool(blobs)

with models.DAG(
    dag_id="event_driven_cross_project_transfer_p1_to_p2",
    start_date=datetime(2023, 1, 1),
    schedule_interval="*/15 * * * *",
    catchup=False,
    tags=["sts", "event-driven", "cross-project"],
) as dag:

    check_for_new_files = PythonOperator(
        task_id="check_for_new_files",
        python_callable=should_trigger_transfer,
    )

    run_transfer = CloudDataTransferServiceCreateJobOperator(
        task_id="run_event_sts_transfer",
        body={
            "description": "Event-driven cross-project transfer: Project 1 → Project 2",
            "status": "ENABLED",
            "projectId": PROJECT_ID,
            "transferSpec": {
                "gcsDataSource": {"bucketName": SOURCE_BUCKET},
                "gcsDataSink": {"bucketName": DEST_BUCKET},
                "objectConditions": {
                    "includePrefixes": [TRANSFER_PREFIX]
                },
                "transferOptions": {
                    "overwriteObjectsAlreadyExistingInSink": False
                },
            },
            "schedule": {
                "scheduleStartDate": {
                    "year": datetime.utcnow().year,
                    "month": datetime.utcnow().month,
                    "day": datetime.utcnow().day
                },
                "scheduleEndDate": {
                    "year": 2099,
                    "month": 12,
                    "day": 31
                },
                "startTimeOfDay": {"hours": 1}
            }
        },
        trigger_rule="all_done"
    )

    check_for_new_files >> run_transfer
