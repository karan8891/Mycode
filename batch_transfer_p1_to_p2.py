from airflow import models
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
from datetime import datetime

with models.DAG(
    dag_id="batch_transfer_p1_to_p2",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["sts", "batch", "p1_to_p2"],
) as dag:

    transfer_job = CloudDataTransferServiceCreateJobOperator(
        task_id="run_sts_transfer",
        body={
            "description": "One-time transfer from P1 Bucket 1 to P2 Bucket 2",
            "status": "ENABLED",
            "projectId": "project-1-id",
            "transferSpec": {
                "gcsDataSource": {"bucketName": "bucket-1"},
                "gcsDataSink": {"bucketName": "bucket-2"},
                "transferOptions": {
                    "overwriteObjectsAlreadyExistingInSink": False
                },
            },
            "schedule": {
                "scheduleStartDate": {
                    "year": 2025,
                    "month": 6,
                    "day": 13
                },
                "scheduleEndDate": {
                    "year": 2025,
                    "month": 6,
                    "day": 13
                },
                "startTimeOfDay": {"hours": 1}
            }
        },
    )
