
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
from datetime import datetime, timedelta
import json

PROJECT_ID = "sandbox-corp-cmp-gdw-sfr-cdp"
DESTINATION_BUCKET = "gdw_sandbox-corp-cmp-gdw-sfr-cdb"
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

with DAG(
    dag_id="gdw_sts_event_v14_partial",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["sts", "event-driven", "partial"],
) as dag:

    def parse_cf_message(**kwargs):
        sample_msg = {
            "batch_timestamp": "2025-07-31T06:00:00Z",
            "file_events": [
                {
                    "cdm_source_bucket": "apmf-bucket-a",
                    "cdm_file_prefix_pattern": "apmf_file_01.csv",
                    "cdm_source_project_id": "apmf-proj-id",
                    "delete_source_files_after_transfer": True,
                }
            ]
        }
        kwargs["ti"].xcom_push(key="message_dict", value=sample_msg)

    def build_sts_body(**kwargs):
        message_dict = kwargs["ti"].xcom_pull(task_ids="parse_cf", key="message_dict")
        file_event = message_dict["file_events"][0]

        transfer_job_body = {
            "projectId": PROJECT_ID,
            "description": "STS job - delete=True",
            "transferSpec": {
                "gcsDataSource": {
                    "bucketName": file_event["cdm_source_bucket"],
                },
                "gcsDataSink": {
                    "bucketName": DESTINATION_BUCKET,
                },
                "objectConditions": {
                    "includePrefixes": [file_event["cdm_file_prefix_pattern"]],
                },
                "transferOptions": {
                    "deleteObjectsFromSourceAfterTransfer": file_event["delete_source_files_after_transfer"],
                },
            },
            "status": "ENABLED",
        }

        kwargs["ti"].xcom_push(key="sts_body", value=transfer_job_body)

    def get_sts_body_dict(**kwargs):
        ti = kwargs["ti"]
        return ti.xcom_pull(task_ids="build_sts_body", key="sts_body")

    parse_cf = PythonOperator(
        task_id="parse_cf",
        python_callable=parse_cf_message,
    )

    build_sts_body_task = PythonOperator(
        task_id="build_sts_body",
        python_callable=build_sts_body,
    )

    create_sts_job = CloudDataTransferServiceCreateJobOperator(
        task_id="create_sts_job",
        body=get_sts_body_dict,
    )

    parse_cf >> build_sts_body_task >> create_sts_job
