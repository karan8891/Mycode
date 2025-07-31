
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
from airflow.operators.python import PythonOperator
import json
import base64

PROJECT_ID = "gdw-etl-prd-02"
DEST_BUCKET = "cdm-data-prd-lake-landing-dca-gcs"

default_args = {
    "start_date": days_ago(1),
}

with DAG(
    dag_id="sts_cf_inline_final_v2",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["sts", "inline", "cf"],
) as dag:

    def parse_and_create_sts_job(**kwargs):
        message = kwargs["dag_run"].conf.get("message")
        if not message:
            raise ValueError("No message payload received")

        parsed_message = json.loads(base64.b64decode(message).decode("utf-8"))
        file_event = parsed_message["file_events"][0]

        source_project_id = file_event["cdm_source_project_id"]
        source_bucket = file_event["cdm_source_bucket"]
        prefix = file_event["cdm_file_prefix_pattern"]
        delete_flag = file_event["delete_source_files_after_transfer"]

        CloudDataTransferServiceCreateJobOperator(
            task_id="create_sts_job_inline",
            project_id=PROJECT_ID,
            body={
                "description": "STS Job from CF Message",
                "status": "ENABLED",
                "projectId": source_project_id,
                "transferSpec": {
                    "gcsDataSource": {"bucketName": source_bucket},
                    "gcsDataSink": {"bucketName": DEST_BUCKET},
                    "objectConditions": {"includePrefixes": [prefix]},
                    "transferOptions": {
                        "deleteObjectsFromSourceAfterTransfer": delete_flag
                    },
                },
                "schedule": {"scheduleStartDate": {"year": 2024, "month": 1, "day": 1}},
            },
        ).execute(context=kwargs)

    parse_and_create_task = PythonOperator(
        task_id="parse_and_create_sts_job",
        python_callable=parse_and_create_sts_job,
    )
