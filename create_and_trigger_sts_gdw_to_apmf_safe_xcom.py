
from datetime import datetime, timedelta, timezone
from airflow import models
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
from google.cloud import storage
from googleapiclient.discovery import build
from google.auth import default

# CONFIG
SOURCE_BUCKET = "gdw-data-prd-stg-ingress"
DEST_BUCKET = "apmf-data-prd-stg-ingress"
PROJECT_ID = "gdw-data-prd"
STS_JOB_DESCRIPTION = "Dynamic transfer from GDW to APMF"
TRIGGER_PREFIX = "incoming/"
CUTOFF_MINUTES = 10

def check_recent_files(**context):
    client = storage.Client()
    bucket = client.bucket(SOURCE_BUCKET)
    blobs = list(bucket.list_blobs(prefix=TRIGGER_PREFIX))

    cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=CUTOFF_MINUTES)
    recent_files = [blob.name for blob in blobs if blob.updated >= cutoff_time]

    if recent_files:
        context['ti'].xcom_push(key="trigger_sts", value=True)
        print(f"Recent files found: {recent_files}")
    else:
        context['ti'].xcom_push(key="trigger_sts", value=False)
        print("No recent files found.")

def trigger_sts_if_needed(**context):
    if context['ti'].xcom_pull(key="trigger_sts"):
        # Pull the full response from create_sts_job task
        job_response = context['ti'].xcom_pull(task_ids='create_sts_job')

        if not job_response or "name" not in job_response:
            raise ValueError("No STS job name found in XComs. Check if job was created successfully.")

        job_name = job_response["name"]  # Expected format: transferJobs/...

        credentials, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        service = build("storagetransfer", "v1", credentials=credentials)

        response = service.transferJobs().run(
            jobName=job_name,
            body={"projectId": PROJECT_ID}
        ).execute()

        print(f"Triggered STS job: {response}")
    else:
        print("No recent files — skipping STS trigger.")

with models.DAG(
    dag_id="create_and_trigger_sts_gdw_to_apmf_safe_xcom",
    start_date=datetime(2023, 1, 1),
    schedule_interval="*/10 * * * *",  # Adjust to "*/5 * * * *" for 5 min polling
    catchup=False,
    tags=["sts", "dynamic", "event-driven", "gdw-to-apmf"],
) as dag:

    create_sts_job = CloudDataTransferServiceCreateJobOperator(
        task_id="create_sts_job",
        body={
            "description": STS_JOB_DESCRIPTION,
            "status": "ENABLED",
            "projectId": PROJECT_ID,
            "transferSpec": {
                "gcsDataSource": {"bucketName": SOURCE_BUCKET},
                "gcsDataSink": {"bucketName": DEST_BUCKET},
                "objectConditions": {
                    "includePrefixes": [TRIGGER_PREFIX]
                },
                "transferOptions": {
                    "overwriteObjectsAlreadyExistingInSink": True
                }
            },
            "schedule": {
                "scheduleStartDate": {
                    "year": datetime.now().year,
                    "month": datetime.now().month,
                    "day": datetime.now().day
                },
                "scheduleEndDate": {
                    "year": datetime.now().year + 1,
                    "month": datetime.now().month,
                    "day": datetime.now().day
                }
            }
        },
        do_xcom_push=True
    )

    check_files = PythonOperator(
        task_id="check_recent_files",
        python_callable=check_recent_files
    )

    trigger_job = PythonOperator(
        task_id="trigger_sts_if_needed",
        python_callable=trigger_sts_if_needed,
        provide_context=True
    )

    create_sts_job >> check_files >> trigger_job
