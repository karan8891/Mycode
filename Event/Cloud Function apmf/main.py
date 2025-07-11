from airflow import models
from airflow.operators.python import PythonOperator
from datetime import datetime
from googleapiclient.discovery import build
from airflow.utils.dates import days_ago
from google.auth import default

# Replace with actual job ID and project ID
STS_JOB_NAME = "transferJobs/5916897567283792656"  # APMF to GDW pull STS job
PROJECT_ID = "sandbox-corp-gdw-sfr-cdb8"           # STS job project (GDW project)

def trigger_sts_pull_job():
    credentials, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    service = build("storagetransfer", "v1", credentials=credentials)

    response = service.transferJobs().run(
        jobName=STS_JOB_NAME,
        body={"projectId": PROJECT_ID}
    ).execute()

    print(f"Triggered STS job: {response}")

default_args = {
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

with models.DAG(
    dag_id="apmf_to_gdw_pull_dag",
    default_args=default_args,
    schedule_interval=None,  # Triggered by Cloud Function
    catchup=False,
    tags=["sts", "pull"],
) as dag:

    run_transfer = PythonOperator(
        task_id="trigger_sts_pull_job",
        python_callable=trigger_sts_pull_job,
    )

    run_transfer
