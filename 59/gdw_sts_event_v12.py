
from datetime import datetime, timedelta
import json
import uuid
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor

PROJECT_ID = "sandbox-corp-gdw-sfr-cdp"
DESTINATION_BUCKET = "gdw_sandbox_corp_gdw_sfr_cdp"
CF_TRIGGER_SUBSCRIPTION = "cf-trigger-dag"
STS_COMPLETION_SUBSCRIPTION = "sts-completion-sub"
CDMNXT_TOPIC = "cdmnxt-trigger-topic"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 7, 30),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def pull_cf_message(**kwargs):
    # Simulate Pub/Sub message structure
    message = {
        "batch_timestamp": "2025-07-31T06:00:00Z",
        "file_events": [
            {
                "cdm_source_bucket": "source-bucket-name",
                "cdm_file_prefix_pattern": "sample/path/prefix/",
                "cdm_source_project_id": "source-project-id",
                "delete_source_files_after_transfer": True,
            }
        ]
    }
    kwargs["ti"].xcom_push(key="cf_message", value=message)

def build_sts_body(**kwargs):
    message = kwargs["ti"].xcom_pull(key="cf_message")
    file = message["file_events"][0]
    job_uid = str(uuid.uuid4()).replace("-", "_")
    job = {
        "description": "Auto STS Job",
        "projectId": PROJECT_ID,
        "transferSpec": {
            "gcsDataSource": {
                "bucketName": file["cdm_source_bucket"],
                "path": file["cdm_file_prefix_pattern"]
            },
            "gcsDataSink": {
                "bucketName": DESTINATION_BUCKET,
            },
            "objectConditions": {},
            "transferOptions": {
                "deleteObjectsFromSourceAfterTransfer": file["delete_source_files_after_transfer"],
            }
        },
        "status": "ENABLED",
        "schedule": {
            "scheduleStartDate": {
                "year": datetime.utcnow().year,
                "month": datetime.utcnow().month,
                "day": datetime.utcnow().day,
            },
            "scheduleEndDate": {
                "year": datetime.utcnow().year,
                "month": datetime.utcnow().month,
                "day": datetime.utcnow().day,
            },
            "startTimeOfDay": {
                "hours": datetime.utcnow().hour,
                "minutes": datetime.utcnow().minute,
                "seconds": datetime.utcnow().second,
            },
        }
    }
    kwargs["ti"].xcom_push(key="sts_body", value=job)
    kwargs["ti"].xcom_push(key="job_uid", value=job_uid)

def resolve_sts_body(**kwargs):
    return kwargs["ti"].xcom_pull(key="sts_body")

with DAG("gdw_sts_event_v12",
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    pull_msg = PythonOperator(
        task_id="pull_cf_message",
        python_callable=pull_cf_message
    )

    build_body = PythonOperator(
        task_id="build_sts_body",
        python_callable=build_sts_body
    )

    create_sts_job = CloudDataTransferServiceCreateJobOperator(
        task_id="create_sts_job",
        body=resolve_sts_body,
    )

    pull_msg >> build_body >> create_sts_job
