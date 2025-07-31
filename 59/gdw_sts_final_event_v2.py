from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceDeleteJobOperator,
)
from google.cloud import pubsub_v1
from datetime import datetime, timedelta
import json
import logging
import uuid

PROJECT_ID = "gdw-project"
DAG_ID = "gdw_sts_final_event_v2"
CF_SUBSCRIPTION = "cf-trigger-sub"
STS_SUBSCRIPTION = "sts-completion-sub"
CDMNXT_TOPIC = "cdmnxt-topic"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 7, 30),
    "retries": 0,
}

def pull_cf_messages(**kwargs):
    subscriber = pubsub_v1.SubscriberClient()
    sub_path = subscriber.subscription_path(PROJECT_ID, CF_SUBSCRIPTION)
    response = subscriber.pull(subscription=sub_path, max_messages=10)
    messages = []
    for msg in response.received_messages:
        messages.append(json.loads(msg.message.data.decode("utf-8")))
        subscriber.acknowledge(subscription=sub_path, ack_ids=[msg.ack_id])
    grouped = {"true": [], "false": []}
    for m in messages:
        key = str(m.get("delete_after_transfer", False)).lower()
        grouped[key].append(m)
    kwargs["ti"].xcom_push(key="grouped_messages", value=grouped)

def build_job_body(delete_flag, **kwargs):
    grouped = kwargs["ti"].xcom_pull(task_ids="pull_messages", key="grouped_messages")
    group = grouped.get(str(delete_flag).lower(), [])
    if not group:
        return None
    first = group[0]
    prefix_list = [obj["object_name"] for obj in group]
    return {
        "projectId": PROJECT_ID,
        "transferJob": {
            "description": f"event_job_{delete_flag}_{uuid.uuid4()}",
            "status": "ENABLED",
            "transferSpec": {
                "gcsDataSource": {"bucketName": first["source_bucket"]},
                "gcsDataSink": {"bucketName": first["destination_bucket"]},
                "objectConditions": {"includePrefixes": prefix_list},
                "transferOptions": {"deleteObjectsFromSourceAfterTransfer": delete_flag},
            },
            "schedule": {"scheduleStartDate": {"year": 2025, "month": 7, "day": 30},
                         "scheduleEndDate": {"year": 2025, "month": 7, "day": 30}},
        }
    }

def extract_job_name(**kwargs):
    job = kwargs["ti"].xcom_pull(task_ids=kwargs["params"]["task"], key="job_response")
    return job["name"]

def wait_for_completion(**kwargs):
    job_name = kwargs["ti"].xcom_pull(task_ids=kwargs["params"]["get_job_name_task"])
    subscriber = pubsub_v1.SubscriberClient()
    sub_path = subscriber.subscription_path(PROJECT_ID, STS_SUBSCRIPTION)
    while True:
        response = subscriber.pull(subscription=sub_path, max_messages=5)
        for msg in response.received_messages:
            data = json.loads(msg.message.data.decode("utf-8"))
            if data.get("jobName") == job_name and data.get("eventType") == "TRANSFER_OPERATION_SUCCESS":
                subscriber.acknowledge(subscription=sub_path, ack_ids=[msg.ack_id])
                return

def generate_toc(delete_flag, **kwargs):
    grouped = kwargs["ti"].xcom_pull(task_ids="pull_messages", key="grouped_messages")
    group = grouped.get(str(delete_flag).lower(), [])
    if not group:
        return None
    destination_bucket = group[0]["destination_bucket"]
    toc = "\n".join([f"{obj['object_name']} - {obj['source_bucket']} -> {destination_bucket}" for obj in group])
    filename = f"toc_{delete_flag}_{uuid.uuid4()}.txt"
    with open(f"/tmp/{filename}", "w") as f:
        f.write(toc)
    # Normally upload to GCS, here we just simulate
    kwargs["ti"].xcom_push(key=f"toc_filename_{delete_flag}", value=filename)

def notify_cdmnxt(delete_flag, **kwargs):
    filename = kwargs["ti"].xcom_pull(task_ids=f"generate_toc_{delete_flag}", key=f"toc_filename_{delete_flag}")
    if not filename:
        return
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, CDMNXT_TOPIC)
        publisher.publish(topic_path, data=filename.encode("utf-8"))
    except Exception:
        logging.info(f"no topic, message: {filename}, not delivered")

with DAG(DAG_ID, default_args=default_args, schedule_interval=None, catchup=False) as dag:
    pull_messages = PythonOperator(task_id="pull_messages", python_callable=pull_cf_messages)

    for flag in [True, False]:
        build = PythonOperator(task_id=f"build_body_{flag}", python_callable=build_job_body, op_kwargs={"delete_flag": flag})
        create = CloudDataTransferServiceCreateJobOperator(task_id=f"create_sts_job_{flag}", body=lambda **kwargs: kwargs["ti"].xcom_pull(task_ids=f"build_body_{flag}"))
        wait = PythonOperator(task_id=f"wait_for_completion_{flag}", python_callable=wait_for_completion, params={"get_job_name_task": f"create_sts_job_{flag}"})
        delete = CloudDataTransferServiceDeleteJobOperator(task_id=f"delete_sts_job_{flag}", job_name=extract_job_name, params={"task": f"create_sts_job_{flag}"})
        toc = PythonOperator(task_id=f"generate_toc_{flag}", python_callable=generate_toc, op_kwargs={"delete_flag": flag})
        notify = PythonOperator(task_id=f"notify_cdmnxt_{flag}", python_callable=notify_cdmnxt, op_kwargs={"delete_flag": flag})

        pull_messages >> build >> create >> wait >> delete >> toc >> notify