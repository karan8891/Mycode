
from datetime import datetime, timedelta, timezone
from airflow import models
from airflow.operators.python import PythonOperator
from google.cloud import storage
from googleapiclient.discovery import build
from google.auth import default

# CONFIG
SOURCE_BUCKET = "gdw-data-prd-stg-ingress"
DEST_BUCKET = "apmf-data-prd-stg-ingress"
PROJECT_ID = "gdw-data-prd"
PRE_CREATED_STS_JOB_NAME = "transferJobs/abcd1234"  # Replace with your real job name
SOURCE_PREFIX = "upstream/"
DEST_PREFIX = "downstream/"
CUTOFF_MINUTES = 10

def check_recent_files(**context):
    client = storage.Client()
    bucket = client.bucket(SOURCE_BUCKET)
    blobs = list(bucket.list_blobs(prefix=SOURCE_PREFIX))

    cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=CUTOFF_MINUTES)
    recent_files = [blob.name for blob in blobs if blob.updated >= cutoff_time]

    if recent_files:
        context['ti'].xcom_push(key="trigger_sts", value=True)
        print(f"Recent files in upstream/: {recent_files}")
    else:
        context['ti'].xcom_push(key="trigger_sts", value=False)
        print("No recent files in upstream/.")

def trigger_precreated_sts_job(**context):
    if context['ti'].xcom_pull(key="trigger_sts"):
        credentials, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        service = build("storagetransfer", "v1", credentials=credentials)

        response = service.transferJobs().run(
            jobName=PRE_CREATED_STS_JOB_NAME,
            body={"projectId": PROJECT_ID}
        ).execute()

        print(f"Triggered STS job: {response}")
    else:
        print("No recent files — STS job not triggered.")

with models.DAG(
    dag_id="event_driven_reuse_sts_gdw_to_apmf_with_folders",
    start_date=datetime(2023, 1, 1),
    schedule_interval="*/10 * * * *",
    catchup=False,
    tags=["sts", "event-driven", "gdw-to-apmf", "folders"],
) as dag:

    check_files = PythonOperator(
        task_id="check_recent_files",
        python_callable=check_recent_files
    )

    trigger_job = PythonOperator(
        task_id="trigger_sts_if_needed",
        python_callable=trigger_precreated_sts_job,
        provide_context=True
    )

    check_files >> trigger_job
