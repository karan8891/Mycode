from airflow import models
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceRunJobOperator

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
}

with models.DAG(
    dag_id='gdw_to_apmf_push_dag',
    default_args=default_args,
    schedule_interval=None,  # Triggered by Cloud Function via Pub/Sub
    catchup=False,
    tags=['sts', 'push']
) as dag:

    transfer_job = CloudDataTransferServiceRunJobOperator(
        task_id="start_gdw_to_apmf_push_transfer",
        job_name="transferJobs/14720473520330133286",  # <-- Replace with correct job ID
        project_id="sandbox-corp-gdw-sfr-cdb8",
        location="us"  # Optional; default is "us"
    )
    
