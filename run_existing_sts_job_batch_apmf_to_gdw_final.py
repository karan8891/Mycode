
from datetime import datetime
from airflow import models
from airflow.operators.python import PythonOperator
from googleapiclient.discovery import build
from google.auth import default

# CONFIG
STS_JOB_NAME = "transferJobs/1659374602007638"  # Full job name
PROJECT_ID = "apmf-data-prd"

def trigger_sts_job():
    credentials, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    service = build("storagetransfer", "v1", credentials=credentials)

    response = service.transferJobs().run(
        jobName=STS_JOB_NAME,
        body={"projectId": PROJECT_ID}
    ).execute()

    print(f"Triggered STS job: {response}")

with models.DAG(
    dag_id="run_existing_sts_job_batch_apmf_to_gdw_final",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["sts", "precreated", "batch", "apmf-to-gdw"],
) as dag:

    trigger_task = PythonOperator(
        task_id="trigger_sts_job",
        python_callable=trigger_sts_job
    )

    trigger_task
