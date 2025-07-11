from airflow import models
from airflow.operators.python import PythonOperator
from datetime import datetime
from googleapiclient.discovery import build
from google.auth import default

# Replace these with your actual STS job ID and project ID
STS_JOB_NAME = "transferJobs/6009691888195171934"
PROJECT_ID = "sandbox-corp-gdw-sfr-cdb8"  # GDW project ID

def trigger_sts_job():
    credentials, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    service = build("storagetransfer", "v1", credentials=credentials)

    response = service.transferJobs().run(
        jobName=STS_JOB_NAME,
        body={"projectId": PROJECT_ID}
    ).execute()

    print(f"✅ Triggered STS job: {response}")

default_args = {
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

with models.DAG(
    dag_id="gdw_to_apmf_push_dag",
    default_args=default_args,
    schedule_interval=None,  # Only triggered by Cloud Function
    catchup=False,
    tags=["sts", "push"],
) as dag:

    run_transfer = PythonOperator(
        task_id="trigger_sts_push_job",
        python_callable=trigger_sts_job,
    )

    run_transfer
