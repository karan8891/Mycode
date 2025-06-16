
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

def check_or_create_sts_job(**context):
    credentials, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    service = build("storagetransfer", "v1", credentials=credentials)

    # Check existing jobs
    request = service.transferJobs().list(
        filter=f'{{"project_id":"{PROJECT_ID}"}}'
    )
    response = request.execute()
    existing_job_name = None

    for job in response.get("transferJobs", []):
        if job.get("description") == STS_JOB_DESCRIPTION:
            existing_job_name = job["name"]
            break

    if existing_job_name:
        print(f"Reusing existing job: {existing_job_name}")
        context['ti'].xcom_push(key="job_name", value=existing_job_name)
    else:
        # Create new job
        create_response = service.transferJobs().create(
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
            }
        ).execute()
        job_name = create_response["name"]
        print(f"Created new STS job: {job_name}")
        context['ti'].xcom_push(key="job_name", value=job_name)

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
        job_name = context['ti'].xcom_pull(task_ids="check_or_create_sts_job", key="job_name")

        if not job_name:
            raise ValueError("No STS job name found in XComs.")

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
    dag_id="deduplicated_create_and_trigger_sts_gdw_to_apmf",
    start_date=datetime(2023, 1, 1),
    schedule_interval="*/10 * * * *",  # Adjust to "*/5 * * * *" for 5 min polling
    catchup=False,
    tags=["sts", "deduplicated", "event-driven", "gdw-to-apmf"],
) as dag:

    check_or_create_sts_job = PythonOperator(
        task_id="check_or_create_sts_job",
        python_callable=check_or_create_sts_job
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

    check_or_create_sts_job >> check_files >> trigger_job
