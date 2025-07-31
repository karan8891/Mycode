
from datetime import datetime, timedelta
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor

PROJECT_ID = "sandbox-corp-gdw-sfr-cdp"
DESTINATION_BUCKET = "gdw_sandbox-corp-gdw-sfr-cdp"
CF_TRIGGER_SUBSCRIPTION = "cf-trigger-dag-sub"
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

def build_sts_job_body(**kwargs):
    # For now using dummy values to fix parsing
    job_body = {
        "projectId": PROJECT_ID,
        "transferSpec": {
            "gcsDataSource": {
                "bucketName": "apmf-source-bucket"
            },
            "gcsDataSink": {
                "bucketName": DESTINATION_BUCKET
            },
            "objectConditions": {
                "includePrefixes": ["test_prefix/"]
            },
            "transferOptions": {
                "deleteObjectsFromSourceAfterTransfer": False
            }
        },
        "status": "ENABLED",
        "schedule": {
            "scheduleStartDate": {
                "year": 2025,
                "month": 7,
                "day": 30
            },
            "scheduleEndDate": {
                "year": 2025,
                "month": 7,
                "day": 30
            },
            "startTimeOfDay": {
                "hours": 1,
                "minutes": 0,
                "seconds": 0
            }
        }
    }
    kwargs["ti"].xcom_push(key="sts_body", value=job_body)

def _get_sts_body_from_xcom(**kwargs):
    ti = kwargs["ti"]
    return ti.xcom_pull(task_ids="build_sts_body", key="sts_body")

with DAG(
    "gdw_sts_event_v14_fixed",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["sts", "event", "v14", "fix"],
) as dag:

    build_body = PythonOperator(
        task_id="build_sts_body",
        python_callable=build_sts_job_body
    )

    create_job = CloudDataTransferServiceCreateJobOperator(
        task_id="create_sts_job",
        body=_get_sts_body_from_xcom,
        wait_for_completion=False
    )

    build_body >> create_job
