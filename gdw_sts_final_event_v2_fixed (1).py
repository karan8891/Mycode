
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceDeleteJobOperator,
)
from airflow.models import Variable
import json
import uuid
from google.cloud import pubsub_v1

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
        flag = str(msg.get("delete_after_transfer", False)).lower()
        grouped[flag].append(msg)
    kwargs["ti"].xcom_push(key="grouped_messages", value=grouped)

def build_sts_job_body(delete_flag, **kwargs):
    grouped = kwargs["ti"].xcom_pull(task_ids="parse_cf", key="grouped_messages")
    if not grouped or delete_flag not in grouped:
        raise ValueError("Missing grouped messages for delete_flag: " + delete_flag)
    job_bodies = []
    for file in grouped[delete_flag]:
        job = {
            "projectId": PROJECT_ID,
            "transferSpec": {
                "gcsDataSource": {"bucketName": file["source_bucket"]},
                "gcsDataSink": {"bucketName": DESTINATION_BUCKET},
                "objectConditions": {"includePrefixes": [file["object_name"]]},
                "transferOptions": {"deleteObjectsFromSourceAfterTransfer": file["delete_after_transfer"]},
            },
            "description": f"sts-job-{uuid.uuid4()}",
            "status": "ENABLED",
            "schedule": {
                "scheduleStartDate": {"year": 2025, "month": 7, "day": 30},
                "scheduleEndDate": {"year": 2025, "month": 7, "day": 30},
            },
        }
        job_bodies.append(job)
    kwargs["ti"].xcom_push(key=f"job_body_{delete_flag}", value=job_bodies)

def get_sts_job_body(delete_flag):
    def _get(**kwargs):
        return kwargs["ti"].xcom_pull(task_ids=f"build_job_body_{delete_flag}", key=f"job_body_{delete_flag}")
    return _get

def simulate_pubsub_sensor(**kwargs):
    import time
    time.sleep(10)
    return True

def generate_toc_file(**kwargs):
    grouped = kwargs["ti"].xcom_pull(task_ids="parse_cf", key="grouped_messages")
    toc = {}
    for flag, files in grouped.items():
        toc[flag] = [{"source_bucket": f["source_bucket"], "object_name": f["object_name"]} for f in files]
    toc_file = f"toc_{uuid.uuid4()}.json"
    kwargs["ti"].xcom_push(key="toc_file", value=toc_file)

def notify_or_log(**kwargs):
    toc_file = kwargs["ti"].xcom_pull(task_ids="generate_toc", key="toc_file")
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, CDMNXT_TOPIC)
        publisher.publish(topic_path, data=toc_file.encode("utf-8"))
    except Exception:
        print(f"no topic, message: {toc_file}, not delivered")

with DAG("gdw_sts_final_event_v2",
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    parse_cf = PythonOperator(
        task_id="parse_cf",
        python_callable=parse_cf_messages,
    )

    build_job_true = PythonOperator(
        task_id="build_job_body_true",
        python_callable=build_sts_job_body,
        op_kwargs={"delete_flag": "true"},
    )

    build_job_false = PythonOperator(
        task_id="build_job_body_false",
        python_callable=build_sts_job_body,
        op_kwargs={"delete_flag": "false"},
    )

    create_job_true = CloudDataTransferServiceCreateJobOperator(
        task_id="create_job_true",
        body=get_sts_job_body("true"),
    )

    create_job_false = CloudDataTransferServiceCreateJobOperator(
        task_id="create_job_false",
        body=get_sts_job_body("false"),
    )

    wait_true = PythonOperator(
        task_id="wait_for_completion_true",
        python_callable=simulate_pubsub_sensor,
    )

    wait_false = PythonOperator(
        task_id="wait_for_completion_false",
        python_callable=simulate_pubsub_sensor,
    )

    delete_job_true = CloudDataTransferServiceDeleteJobOperator(
        task_id="delete_job_true",
        job_name="{{ task_instance.xcom_pull(task_ids='create_job_true') }}",
    )

    delete_job_false = CloudDataTransferServiceDeleteJobOperator(
        task_id="delete_job_false",
        job_name="{{ task_instance.xcom_pull(task_ids='create_job_false') }}",
    )

    generate_toc = PythonOperator(
        task_id="generate_toc",
        python_callable=generate_toc_file,
    )

    notify = PythonOperator(
        task_id="notify_or_log",
        python_callable=notify_or_log,
    )

    parse_cf >> [build_job_true, build_job_false]
    build_job_true >> create_job_true >> wait_true >> delete_job_true
    build_job_false >> create_job_false >> wait_false >> delete_job_false
    [delete_job_true, delete_job_false] >> generate_toc >> notify
