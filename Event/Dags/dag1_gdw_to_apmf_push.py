from airflow import models
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import storage
from googleapiclient.discovery import build
from google.auth import default
import time

STS_JOB_NAME = "transferJobs/6009691888195171934"
PROJECT_ID = "sandbox-corp-gdw-sfr-cdb8"  # GDW project ID
SOURCE_BUCKET = "your-source-bucket-name"
SOURCE_PREFIX = "folder/"  # optional

# 1. Check if files exist recently
def should_trigger_sts_job():
    client = storage.Client()
    bucket = client.get_bucket(SOURCE_BUCKET)
    now = time.time()
    new_file_found = False

    for blob in bucket.list_blobs(prefix=SOURCE_PREFIX):
        if blob.updated.timestamp() > now - 300:  # last 5 min
            print(f"New file: {blob.name} updated at {blob.updated}")
            new_file_found = True
            break

    if new_file_found:
        credentials, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        service = build("storagetransfer", "v1", credentials=credentials)
        response = service.transferJobs().run(
            jobName=STS_JOB_NAME,
            body={"projectId": PROJECT_ID}
        ).execute()
        print(f"Triggered STS job: {response}")
    else:
        print("No new files found in last 5 minutes. Skipping STS trigger.")

default_args = {
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

with models.DAG(
    dag_id="gdw_to_apmf_push_dag",
    default_args=default_args,
    schedule_interval=None,  # Only triggered by Cloud Function
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=["sts", "push"],
) as dag:

    run_transfer = PythonOperator(
        task_id="conditionally_trigger_sts_push_job",
        python_callable=should_trigger_sts_job,
    )
