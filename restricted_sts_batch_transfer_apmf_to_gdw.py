
from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
from airflow.utils.dates import days_ago

with models.DAG(
    dag_id="restricted_sts_batch_transfer_apmf_to_gdw",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["sts", "gcs", "batch", "restricted"],
) as dag:

    transfer_job = CloudDataTransferServiceCreateJobOperator(
        task_id="create_batch_sts_apmf_to_gdw",
        body={
            "description": "Batch transfer from apmf to gdw bucket (restricted IAM)",
            "status": "ENABLED",
            "projectId": "apmf-data-prd",  # Replace with actual Project ID
            "transferSpec": {
                "gcsDataSource": {
                    "bucketName": "apmf-data-prd-stg-ingress"
                },
                "gcsDataSink": {
                    "bucketName": "gdw-data-prd-stg-ingress"
                },
                "transferOptions": {
                    "overwriteObjectsAlreadyExistingInSink": True
                }
            },
            "schedule": {
                "scheduleStartDate": {
                    "year": datetime.now().year,
                    "month": datetime.now().month,
                    "day": datetime.now().day
                },
                "scheduleEndDate": {
                    "year": datetime.now().year + 1,
                    "month": datetime.now().month,
                    "day": datetime.now().day
                }
            }
        }
    )

    transfer_job
