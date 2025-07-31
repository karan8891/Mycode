
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceDeleteJobOperator,
)
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.utils.dates import days_ago
import json
from datetime import datetime, timedelta
import uuid
import base64

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="Five9_to_CDM_Test",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["sts", "event-driven"],
) as dag:

    def parse_cf_event(**kwargs):
        message = kwargs["ti"].xcom_pull(task_ids="pull_cf_event")
        if isinstance(message, list):
            message = message[0]

        payload = json.loads(base64.b64decode(message["message"]["data"]).decode("utf-8"))
        delete_flag = payload.get("delete_source_files_after_transfer", False)

        event = {
            "cdm_source_bucket": payload.get("cdm_source_bucket"),
            "cdm_file_prefix_pattern": payload.get("cdm_file_prefix_pattern", ""),
            "cdm_source_project_id": payload.get("cdm_source_project_id"),
            "delete_source_files_after_transfer": delete_flag,
        }

        kwargs["ti"].xcom_push(key="event_payload", value=event)

    def build_sts_job_body(**kwargs):
        event = kwargs["ti"].xcom_pull(key="event_payload", task_ids="parse_cf_event")

        project_id = event["cdm_source_project_id"]
        src_bucket = event["cdm_source_bucket"]
        dst_bucket = "gdw-sandbox-bucket"
        prefix = event["cdm_file_prefix_pattern"]
        delete_flag = event["delete_source_files_after_transfer"]
        job_name = f"transferJobs/{uuid.uuid4()}"

        transfer_job = {
            "description": "Triggered by Cloud Function enriched message",
            "projectId": project_id,
            "transferSpec": {
                "gcsDataSource": {
                    "bucketName": src_bucket,
                    "path": prefix,
                },
                "gcsDataSink": {
                    "bucketName": dst_bucket,
                },
                "objectConditions": {},
                "transferOptions": {
                    "deleteObjectsFromSourceAfterTransfer": delete_flag,
                    "overwriteObjectsAlreadyExistingInSink": True,
                },
            },
            "status": "ENABLED",
        }

        kwargs["ti"].xcom_push(key="sts_job_body", value=transfer_job)
        kwargs["ti"].xcom_push(key="sts_job_name", value=job_name)

    def _pull_sts_body_from_xcom(**kwargs):
        return kwargs["ti"].xcom_pull(key="sts_job_body", task_ids="build_sts_job_body")

    def _pull_sts_job_name(**kwargs):
        return kwargs["ti"].xcom_pull(key="sts_job_name", task_ids="build_sts_job_body")

    pull_cf_event = PubSubPullSensor(
        task_id="pull_cf_event",
        project_id="sandbox-corp-cmp-gdw-0213646-01-sfr",
        subscription="cf-trigger-sub",
        max_messages=1,
        ack_messages=True,
        timeout=30,
    )

    parse_cf_event = PythonOperator(
        task_id="parse_cf_event",
        python_callable=parse_cf_event,
    )

    build_sts_job_body = PythonOperator(
        task_id="build_sts_job_body",
        python_callable=build_sts_job_body,
    )

    create_sts_job = CloudDataTransferServiceCreateJobOperator(
        task_id="create_sts_job",
        body=_pull_sts_body_from_xcom,
    )

    # Additional tasks would be added here: wait_for_completion, TOC creation, delete job, etc.

    pull_cf_event >> parse_cf_event >> build_sts_job_body >> create_sts_job
