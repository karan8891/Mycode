
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceDeleteJobOperator,
)
from airflow.models import Variable
from google.cloud import storage
from google.cloud import pubsub_v1
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

def pull_cf_messages(**kwargs):
    client = pubsub_v1.SubscriberClient()
    subscription_path = client.subscription_path(PROJECT_ID, CF_TRIGGER_SUBSCRIPTION)
    response = client.pull(subscription=subscription_path, max_messages=10)
    messages = []
    for msg in response.received_messages:
        payload = json.loads(msg.message.data.decode("utf-8"))
        messages.append(payload)
        client.acknowledge(subscription=subscription_path, ack_ids=[msg.ack_id])
    grouped = {"true": [], "false": []}
    for msg in messages:
        flag = str(msg.get("delete_after_transfer", False)).lower()
        grouped[flag].append(msg)
    kwargs["ti"].xcom_push(key="grouped_messages", value=grouped)

def build_sts_job_body(delete_flag, **kwargs):
    grouped = kwargs["ti"].xcom_pull(task_ids="pull_cf", key="grouped_messages")
    messages = grouped.get(delete_flag, [])
    if not messages:
        return []
    job_name_prefix = f"sts-job-{delete_flag}-{str(uuid.uuid4())}"
    src_bucket = messages[0]["cdm_source_bucket"]
    dest_bucket = DESTINATION_BUCKET
    object_prefixes = [msg["cdm_file_prefix_pattern"] for msg in messages]
    body = {
        "projectId": PROJECT_ID,
        "transferSpec": {
            "gcsDataSource": {"bucketName": src_bucket},
            "gcsDataSink": {"bucketName": dest_bucket},
            "objectConditions": {"includePrefixes": object_prefixes},
            "transferOptions": {"deleteObjectsFromSourceAfterTransfer": delete_flag == "true"},
        },
        "description": job_name_prefix,
        "status": "ENABLED",
        "schedule": {"scheduleStartDate": {"year": 2025, "month": 7, "day": 30}},
    }
    kwargs["ti"].xcom_push(key=f"job_body_{delete_flag}", value=body)

def get_sts_body(delete_flag, **kwargs):
    return kwargs["ti"].xcom_pull(task_ids=f"build_body_{delete_flag}", key=f"job_body_{delete_flag}")

def generate_toc_and_notify(delete_flag, **kwargs):
    grouped = kwargs["ti"].xcom_pull(task_ids="pull_cf", key="grouped_messages")
    messages = grouped.get(delete_flag, [])
    if not messages:
        return
    toc_data = {"files": messages}
    filename = f"toc_{delete_flag}_{str(uuid.uuid4())}.json"
    client = storage.Client()
    bucket = client.get_bucket(DESTINATION_BUCKET)
    blob = bucket.blob(filename)
    blob.upload_from_string(json.dumps(toc_data), content_type="application/json")
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, CDMNXT_TOPIC)
    try:
        publisher.publish(topic_path, filename.encode("utf-8"))
    except Exception:
        print(f"no topic, message: {filename}, not delivered")

with DAG("gdw_sts_final_event_v2", default_args=default_args, schedule_interval=None, catchup=False) as dag:
    pull_cf = PythonOperator(
        task_id="pull_cf",
        python_callable=pull_cf_messages,
        provide_context=True,
    )

    build_body_true = PythonOperator(
        task_id="build_body_true",
        python_callable=build_sts_job_body,
        op_kwargs={"delete_flag": "true"},
        provide_context=True,
    )

    build_body_false = PythonOperator(
        task_id="build_body_false",
        python_callable=build_sts_job_body,
        op_kwargs={"delete_flag": "false"},
        provide_context=True,
    )

    create_job_true = CloudDataTransferServiceCreateJobOperator(
        task_id="create_sts_job_true",
        body=get_sts_body("true"),
    )

    create_job_false = CloudDataTransferServiceCreateJobOperator(
        task_id="create_sts_job_false",
        body=get_sts_body("false"),
    )

    toc_true = PythonOperator(
        task_id="generate_toc_true",
        python_callable=generate_toc_and_notify,
        op_kwargs={"delete_flag": "true"},
        provide_context=True,
    )

    toc_false = PythonOperator(
        task_id="generate_toc_false",
        python_callable=generate_toc_and_notify,
        op_kwargs={"delete_flag": "false"},
        provide_context=True,
    )

    pull_cf >> [build_body_true, build_body_false]
    build_body_true >> create_job_true >> toc_true
    build_body_false >> create_job_false >> toc_false
