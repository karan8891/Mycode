from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator

with DAG(
    dag_id="gdw_to_apmf_pull_event",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["sts", "event"]
) as dag:

    trigger_sts_job = CloudDataTransferServiceCreateJobOperator(
        task_id="trigger_pull_sts",
        body={
            "description": "Pull from GDW to APMF",
            "projectId": "apmf-project",
            "transferSpec": {
                "gcsDataSource": {"bucketName": "gdw1"},
                "gcsDataSink": {"bucketName": "apmf1"},
                "objectConditions": {"includePrefixes": ["incoming/"]}
            },
            "status": "ENABLED"
        }
    )
