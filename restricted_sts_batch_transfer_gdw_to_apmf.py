
from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
from airflow.utils.dates import days_ago

# Dummy AWS Hook to satisfy Airflow expectations without amazon provider
class AwsBaseHook:
    def __init__(self, aws_conn_id='aws_default', verify=None):
        pass

with models.DAG(
    dag_id="restricted_sts_batch_transfer_gdw_to_apmf",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["sts", "gcs", "batch", "restricted"],
) as dag:

    transfer_job = CloudDataTransferServiceCreateJobOperator(
        task_id="create_batch_sts_job",
        body={
            "description": "Transfer from gdw to apmf bucket (restricted IAM)",
            "status": "ENABLED",
            "projectId": "project-1-id",  # Replace with actual Project 1 ID
            "transferSpec": {
                "gcsDataSource": {
                    "bucketName": "gdw-bucket"
                },
                "gcsDataSink": {
                    "bucketName": "apmf-bucket"
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
