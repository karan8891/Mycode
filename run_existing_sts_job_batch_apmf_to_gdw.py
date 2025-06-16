
from datetime import datetime
from airflow import models
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.cloud_storage_transfer_service import CloudDataTransferServiceHook

STS_JOB_NAME = "transferJobs/1659374602007638"  # Replace with your actual STS job ID
PROJECT_ID = "apmf-data-prd"  # Project where the STS job was created

def trigger_sts_job(**context):
    hook = CloudDataTransferServiceHook()
    response = hook.run_transfer_job(
        job_name=STS_JOB_NAME,
        project_id=PROJECT_ID
    )
    print(f"Triggered STS job: {STS_JOB_NAME}")
    return response

with models.DAG(
    dag_id="run_existing_sts_job_batch_apmf_to_gdw",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["sts", "precreated", "batch", "apmf-to-gdw"],
) as dag:

    trigger_task = PythonOperator(
        task_id="trigger_precreated_sts_job",
        python_callable=trigger_sts_job,
        provide_context=True
    )

    trigger_task
