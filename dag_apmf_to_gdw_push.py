from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator

with DAG(
    dag_id="apmf_to_gdw_push_event",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["sts", "event"]
) as dag:

    trigger_sts_job = CloudDataTransferServiceCreateJobOperator(
        task_id="trigger_push_sts_apmf",
        body={
            "description": "Push from APMF to GDW",
            "projectId": "apmf-project",
            "transferSpec": {
                "gcsDataSource": {"bucketName": "apmf1"},
                "gcsDataSink": {"bucketName": "gdw1"},
                "objectConditions": {"includePrefixes": ["incoming/"]}
            },
            "status": "ENABLED"
        }
    )
