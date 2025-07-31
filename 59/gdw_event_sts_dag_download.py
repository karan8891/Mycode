from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceUpdateJobOperator,
    CloudDataTransferServiceDeleteJobOperator,
)
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.models import Variable
from google.cloud import storage, pubsub_v1
import json
import uuid

PROJECT_ID = "sandbox-corp-gdw-sfr-cdb8"
DESTINATION_BUCKET = "gdw2-sandbox-corp-gdw-sfr-cdb8"
CF_TRIGGER_SUBSCRIPTION = "cf-trigger-sub"
STS_COMPLETION_SUBSCRIPTION = "sts-completion-sub"
CDMNXT_TOPIC = "cdmnxt-topic"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 30),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def parse_cf_messages(**kwargs):
    messages = Variable.get("enriched_cf_messages", deserialize_json=True)
    grouped = {"true": [], "false": []}
    for msg in messages:
        grouped[str(msg["delete_after_transfer"]).lower()].append(msg)
    kwargs["ti"].xcom_push(key="grouped_messages", value=grouped)

def build_sts_job_body(delete_flag, **kwargs):
    grouped = kwargs["ti"].xcom_pull(key="grouped_messages", task_ids="parse_cf")
    jobs = []
    for file in grouped.get(delete_flag, []):
        job = {
            "projectId": PROJECT_ID,
            "transferSpec": {
                "gcsDataSource": {"bucketName": file["source_bucket"]},
                "gcsDataSink": {"bucketName": DESTINATION_BUCKET},
                "objectConditions": {"includePrefixes": [file["object_name"]]},
                "transferOptions": {
                    "deleteObjectsFromSourceAfterTransfer": file["delete_after_transfer"]
                },
            },
            "description": f"sts-job-{uuid.uuid4()}",
            "status": "ENABLED",
            "schedule": {
                "scheduleStartDate": {"year": 2025, "month": 7, "day": 30},
                "scheduleEndDate": {"year": 2025, "month": 7, "day": 30},
            },
        }
        jobs.append(job)
    kwargs["ti"].xcom_push(key=f"job_body_{delete_flag}", value=jobs)

def upload_toc_file(delete_flag, **kwargs):
    grouped = kwargs["ti"].xcom_pull(key="grouped_messages", task_ids="parse_cf")
    file_list = grouped.get(delete_flag, [])
    toc_lines = ["source_bucket,object_name"]
    for file in file_list:
        toc_lines.append(f"{file['source_bucket']},{file['object_name']}")
    toc_content = "\n".join(toc_lines)
    filename = f"toc_{delete_flag}_{uuid.uuid4()}.csv"
    bucket = storage.Client().bucket(DESTINATION_BUCKET)
    blob = bucket.blob(filename)
    blob.upload_from_string(toc_content)
    kwargs["ti"].xcom_push(key=f"toc_filename_{delete_flag}", value=filename)

def notify_cdmnxt(delete_flag, **kwargs):
    publisher = pubsub_v1.PublisherClient()
    try:
        filename = kwargs["ti"].xcom_pull(task_ids=f"toc_{delete_flag}", key=f"toc_filename_{delete_flag}")
        topic_path = publisher.topic_path(PROJECT_ID, CDMNXT_TOPIC)
        publisher.publish(topic_path, filename.encode("utf-8"))
    except Exception:
        print(f"[LOG] no topic, message: {filename}, not delivered")

with DAG("gdw_event_sts_dag", default_args=default_args, schedule_interval=None, catchup=False) as dag:

    parse_cf = PythonOperator(
        task_id="parse_cf",
        python_callable=parse_cf_messages,
    )

    for flag in ["true", "false"]:
        build_body = PythonOperator(
            task_id=f"build_body_{flag}",
            python_callable=build_sts_job_body,
            op_kwargs={"delete_flag": flag},
        )

        create_job = CloudDataTransferServiceCreateJobOperator(
            task_id=f"create_sts_job_{flag}",
            body="{{ task_instance.xcom_pull(task_ids='" + f"build_body_{flag}" + "', key='job_body_" + f"{flag}" + "') | first }}",
            project_id=PROJECT_ID,
        )

        update_job = CloudDataTransferServiceUpdateJobOperator(
            task_id=f"run_sts_job_{flag}",
            job_name="{{ task_instance.xcom_pull(task_ids='" + f"create_sts_job_{flag}" + "')['name'] }}",
            body={},
            project_id=PROJECT_ID,
        )

        wait = PubSubPullSensor(
            task_id=f"wait_for_completion_{flag}",
            project_id=PROJECT_ID,
            subscription=STS_COMPLETION_SUBSCRIPTION,
            ack_messages=True,
            max_messages=5,
            timeout=600,
        )

        delete = CloudDataTransferServiceDeleteJobOperator(
            task_id=f"delete_sts_job_{flag}",
            job_name="{{ task_instance.xcom_pull(task_ids='" + f"create_sts_job_{flag}" + "')['name'] }}",
            project_id=PROJECT_ID,
        )

        toc = PythonOperator(
            task_id=f"toc_{flag}",
            python_callable=upload_toc_file,
            op_kwargs={"delete_flag": flag},
        )

        notify = PythonOperator(
            task_id=f"notify_cdmnxt_{flag}",
            python_callable=notify_cdmnxt,
            op_kwargs={"delete_flag": flag},
        )

        parse_cf >> build_body >> create_job >> update_job >> wait >> delete >> toc >> notify
