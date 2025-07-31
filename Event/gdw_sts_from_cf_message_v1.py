
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
import json
import base64

PROJECT_ID = "sandbox-corp-gdw-sfr-cdb8"
SUBSCRIPTION_NAME = "cf-trigger-dag"
DEST_BUCKET = "gdw2-sandbox-corp-gdw-sfr-cdb8"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


def extract_cf_message(**kwargs):
    ti = kwargs["ti"]
    messages = ti.xcom_pull(task_ids="wait_for_cf_message")

    if not messages:
        raise ValueError("No messages pulled from Pub/Sub.")

    message = messages[0]
    raw_data = message.get("message", {}).get("data", "")
    if not raw_data:
        raise ValueError("Message has no data field.")

    decoded = base64.b64decode(raw_data).decode("utf-8")
    parsed = json.loads(decoded)

    print("✅ Decoded CF message")
    print(json.dumps(parsed, indent=2))

    file_event = parsed["file_events"][0]

    ti.xcom_push(key="CDM_SOURCE_BUCKET", value=file_event["cdm_source_bucket"])
    ti.xcom_push(key="CDM_PREFIX", value=file_event["cdm_file_prefix_pattern"])
    ti.xcom_push(key="CDM_SOURCE_PROJECT_ID", value=file_event["cdm_source_project_id"])
    ti.xcom_push(key="DELETE_AFTER_TRANSFER", value=file_event["delete_source_files_after_transfer"])


def build_sts_body_from_globals(**kwargs):
    ti = kwargs["ti"]

    source_bucket = ti.xcom_pull(task_ids="extract_cf_fields", key="CDM_SOURCE_BUCKET")
    prefix = ti.xcom_pull(task_ids="extract_cf_fields", key="CDM_PREFIX")
    source_project_id = ti.xcom_pull(task_ids="extract_cf_fields", key="CDM_SOURCE_PROJECT_ID")
    delete_source = ti.xcom_pull(task_ids="extract_cf_fields", key="DELETE_AFTER_TRANSFER")

    job_body = {
        "description": "STS job from CF message",
        "status": "ENABLED",
        "projectId": PROJECT_ID,
        "transferSpec": {
            "gcsDataSource": {
                "bucketName": source_bucket,
            },
            "gcsDataSink": {
                "bucketName": DEST_BUCKET,
            },
            "objectConditions": {
                "includePrefixes": [prefix],
            },
            "transferOptions": {
                "deleteObjectsFromSourceAfterTransfer": delete_source,
            },
        },
        "schedule": {
            "scheduleStartDate": {"year": 2025, "month": 7, "day": 31},
            "scheduleEndDate": {"year": 2025, "month": 7, "day": 31},
        },
    }

    ti.xcom_push(key="sts_body", value=job_body)


def get_sts_body(**kwargs):
    return kwargs["ti"].xcom_pull(task_ids="build_sts_body", key="sts_body")


with DAG(
    dag_id="gdw_sts_from_cf_message",
    default_args=default_args,
    description="Parse CF message and create STS job dynamically",
    schedule_interval=None,
    catchup=False,
    start_date=datetime(2025, 7, 30),
    tags=["cf", "sts", "event"],
) as dag:

    wait_for_cf_message = PubSubPullSensor(
        task_id="wait_for_cf_message",
        project_id=PROJECT_ID,
        subscription=SUBSCRIPTION_NAME,
        ack_messages=True,
        max_messages=1,
        timeout=60,
    )

    extract_cf_fields = PythonOperator(
        task_id="extract_cf_fields",
        python_callable=extract_cf_message,
    )

    build_sts_body = PythonOperator(
        task_id="build_sts_body",
        python_callable=build_sts_body_from_globals,
    )

    create_sts_job = CloudDataTransferServiceCreateJobOperator(
        task_id="create_sts_job",
        project_id=PROJECT_ID,
        body=get_sts_body,
    )

    wait_for_cf_message >> extract_cf_fields >> build_sts_body >> create_sts_job
