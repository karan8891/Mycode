
from airflow import DAG
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from google.cloud import storage
import base64
import json

PROJECT_ID = "sandbox-corp-gdw-sfr-cdts"
DEST_BUCKET = "gdw2-sandbox-corp-gdw-sfr-cdts"
SUBSCRIPTION = "cf-trigger-dag"
COMPLETION_SUBSCRIPTION = "sts-completion-sub"

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="Five9_to_CDM_test",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    pull_cf_message = PubSubPullSensor(
        task_id="pull_cf_message",
        project_id=PROJECT_ID,
        subscription=SUBSCRIPTION,
        max_messages=4,
        ack_messages=True,
        timeout=30,
    )

    def parse_and_create_sts_job(messages, **kwargs):
        if not messages:
            print(" No Pub/Sub message received. Using sample message.")
            cf_message = {
                "cdm_source_bucket": "apmf2-sandbox-corp-apmf-oaep-o3fd",
                "cdm_source_project_id": "sandbox-corp-apmf-oaep-o3fd",
                "cdm_file_prefix_pattern": "",
                "delete_source_files_after_transfer": False
            }
        else:
            payload = messages[0]
            if not payload:
                raise ValueError(" Empty Pub/Sub payload.")
            try:
                decoded_data = base64.b64decode(payload).decode("utf-8")
                cf_message = json.loads(decoded_data)
            except Exception as e:
                print(f" Failed to decode message. Using sample message. Error: {str(e)}")
                cf_message = {
                    "cdm_source_bucket": "apmf2-sandbox-corp-apmf-oaep-o3fd",
                    "cdm_source_project_id": "sandbox-corp-apmf-oaep-o3fd",
                    "cdm_file_prefix_pattern": "",
                    "delete_source_files_after_transfer": False
                }

        source_project_id = cf_message["cdm_source_project_id"]
        source_bucket = cf_message["cdm_source_bucket"]
        prefix = cf_message["cdm_file_prefix_pattern"]
        delete_flag = cf_message["delete_source_files_after_transfer"]

        return CloudDataTransferServiceCreateJobOperator(
            task_id="create_sts_job",
            project_id=PROJECT_ID,
            body={
                "description": "STS Job from CF message",
                "status": "ENABLED",
                "projectId": PROJECT_ID,
                "transferSpec": {
                    "gcsDataSource": {"bucketName": source_bucket},
                    "gcsDataSink": {"bucketName": DEST_BUCKET},
                    "objectConditions": {
                        "includePrefixes": [prefix]
                    },
                    "transferOptions": {"deleteObjectsFromSourceAfterTransfer": delete_flag}
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
        ).execute(context=kwargs)

    parse_and_create_task = PythonOperator(
        task_id="parse_and_create_sts_job",
        python_callable=parse_and_create_sts_job,
        op_args=["{{ task_instance.xcom_pull(task_ids='pull_cf_message') }}"],
    )

    wait_for_completion = PubSubPullSensor(
        task_id="wait_for_sts_completion",
        project_id=PROJECT_ID,
        subscription=COMPLETION_SUBSCRIPTION,
        ack_messages=True,
        max_messages=1,
        timeout=60,
    )

    def generate_toc_file(**kwargs):
        message = kwargs['ti'].xcom_pull(task_ids='wait_for_sts_completion')
        if not message:
            raise ValueError("No STS completion message received")

        payload = message[0]
        data = json.loads(base64.b64decode(payload).decode("utf-8"))

        job_name = data.get("jobName", "unknown_job")
        file_names = data.get("manifest", {}).get("fileNames", [])

        toc_content = f"STS Job Name: {job_name}\n\nTransferred Files:\n" + "\n".join(file_names)
        print("TOC file content:\n", toc_content)

        timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%S')
        toc_filename = f"toc_{timestamp}.txt"

        client = storage.Client()
        bucket = client.bucket(DEST_BUCKET)
        blob = bucket.blob(toc_filename)
        blob.upload_from_string(toc_content)

        print(f"TOC uploaded to: gs://{DEST_BUCKET}/{toc_filename}")

    generate_toc_task = PythonOperator(
        task_id="generate_toc",
        python_callable=generate_toc_file,
        provide_context=True
    )

    pull_cf_message >> parse_and_create_task >> wait_for_completion >> generate_toc_task
