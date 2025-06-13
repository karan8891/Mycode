
from airflow import models
from datetime import datetime
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator

PROJECT_ID = "sandbox-corp-cmp-gdw-sfr-cdb8"
SOURCE_BUCKET = "gdw-sandbox-corp-cmp-gdw-sfr-cdb8"
DEST_PROJECT_ID = "apmf-sandbox-corp-apmf-sfo-q3fd"
DEST_BUCKET = "apmf-sandbox-corp-cmp-apmf-sfo-q3fd"
TRANSFER_JOB_NAME = "transfer_only_new_files"

default_args = {
    "start_date": datetime(2023, 1, 1),
    "retry_delay": timedelta(minutes=10),
}

with models.DAG(
    "gcs_transfer_only_new_files",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    create_transfer_job = CloudDataTransferServiceCreateJobOperator(
        task_id="create_transfer_job",
        project_id=PROJECT_ID,
        transfer_job={
            "description": "Daily transfer from GDW to APMF buckets, only new files",
            "status": "ENABLED",
            "projectId": PROJECT_ID,
            "transferSpec": {
                "gcsDataSource": {"bucketName": SOURCE_BUCKET},
                "gcsDataSink": {"bucketName": DEST_BUCKET},
                "objectConditions": {},
                "transferOptions": {
                    "overwriteObjectsAlreadyExistingInSink": False,
                    "deleteObjectsFromSourceAfterTransfer": False,
                    "deleteObjectsUniqueInSink": False
                },
            },
            "schedule": {
                "scheduleStartDate": {"year": 2025, "month": 6, "day": 13},
                "scheduleEndDate": {"year": 2025, "month": 6, "day": 13},
                "startTimeOfDay": {"hours": 1}
            },
        },
    )

    create_transfer_job
