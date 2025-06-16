
from datetime import datetime
from airflow import models
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.cloud_storage_transfer_service import CloudDataTransferServiceHook
from google.cloud import storage

# CONFIGURATION
STS_JOB_NAME = "transferJobs/1659374602007638"  # Pre-created STS job
SOURCE_BUCKET = "apmf-data-prd-stg-ingress"     # Source bucket
TRIGGER_FILE_PREFIX = ""                        # Set if you want to monitor a folder

def check_new_file(**context):
    client = storage.Client()
    bucket = client.bucket(SOURCE_BUCKET)
    blobs = list(bucket.list_blobs(prefix=TRIGGER_FILE_PREFIX))
    if blobs:
        context['ti'].xcom_push(key='trigger_transfer', value=True)
    else:
        context['ti'].xcom_push(key='trigger_transfer', value=False)

def run_existing_sts_job(**context):
    if context['ti'].xcom_pull(key='trigger_transfer'):
        hook = CloudDataTransferServiceHook()
        response = hook.run_transfer_job(
            job_name=STS_JOB_NAME,
            project_id="apmf-data-prd"  # Project where STS job exists
        )
        print(f"Triggered STS job: {STS_JOB_NAME}")
    else:
        print("No new files found, skipping STS trigger.")

with models.DAG(
    dag_id="poll_and_trigger_existing_sts_job_apmf_to_gdw",
    start_date=datetime(2023, 1, 1),
    schedule_interval="*/10 * * * *",  # Poll every 10 minutes
    catchup=False,
    tags=["sts", "poll", "eventless", "trigger"],
) as dag:

    check = PythonOperator(
        task_id="check_new_files_in_apmf",
        python_callable=check_new_file,
        provide_context=True
    )

    trigger = PythonOperator(
        task_id="trigger_precreated_sts_job",
        python_callable=run_existing_sts_job,
        provide_context=True
    )

    check >> trigger
