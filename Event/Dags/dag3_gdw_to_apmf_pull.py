
from airflow import models
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with models.DAG(
    dag_id='gdw_to_apmf_pull_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['sts', 'pull'],
) as dag:

    transfer_job = CloudDataTransferServiceCreateJobOperator(
        task_id='start_gdw_to_apmf_pull_transfer',
        body={
            "description": "Pull STS job from GDW to APMF",
            "status": "ENABLED",
            "projectId": "sandbox-corp-apmf-sfr-83cd",
            "transferSpec": {
                "gcsDataSource": {
                    "bucketName": "gdw-sandbox-bucket-1"
                },
                "gcsDataSink": {
                    "bucketName": "apmf-sandbox-bucket-2"
                },
                "objectConditions": {
                    "includePrefix": ["sts/"],
                },
                "transferOptions": {
                    "deleteObjectsFromSourceAfterTransfer": False,
                    "overwriteObjectsAlreadyExistingInSink": True,
                },
            },
            "schedule": {
                "scheduleStartDate": {
                    "year": 2025,
                    "month": 7,
                    "day": 8
                },
                "scheduleEndDate": {
                    "year": 2030,
                    "month": 12,
                    "day": 31
                },
                "startTimeOfDay": {
                    "hours": 2,
                    "minutes": 0
                }
            }
        }
    )
