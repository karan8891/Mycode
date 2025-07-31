from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator, CloudDataTransferServiceDeleteJobOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from google.cloud import storage
from datetime import datetime, timedelta
import json
import uuid
import os

# DAG Config
PROJECT_ID = "sandbox-corp-gdw-sfr-cdb8"
DEST_BUCKET = "gdw2_sandbox-corp-gdw-sfr-cdb8"
CF_TRIGGER_SUBSCRIPTION = "cf-trigger-dag"
STS_COMPLETION_SUBSCRIPTION = "sts-completion-sub"
CDMNXT_TOPIC = "cdmnxt-trigger-topic"
REGION = "us-central1"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="gdw_sts_event_v3",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["sts", "event-driven"],
) as dag:

    def pull_cf_message(**kwargs):
        from google.cloud import pubsub_v1
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(PROJECT_ID, CF_TRIGGER_SUBSCRIPTION)
        response = subscriber.pull(subscription=subscription_path, max_messages=1, timeout=10)
        if not response.received_messages:
            raise ValueError("No messages received")
        msg = response.received_messages[0]
        ack_id = msg.ack_id
        message_data = json.loads(msg.message.data.decode("utf-8"))
        subscriber.acknowledge(subscription=subscription_path, ack_ids=[ack_id])
        kwargs["ti"].xcom_push(key="cf_message", value=message_data)

    def create_sts_job_body(**kwargs):
        cf_message = kwargs["ti"].xcom_pull(key="cf_message", task_ids="pull_cf_message")
        file_events = cf_message["file_events"]
        delete_flag = file_events[0].get("delete_source_files_after_transfer", False)
        src_bucket = file_events[0]["cdm_source_bucket"]
        src_project = file_events[0]["cdm_source_project_id"]
        prefixes = [f["cdm_file_prefix_pattern"] for f in file_events]

        job_name = f"transferJob-{uuid.uuid4()}"
        job_body = {
            "description": job_name,
            "status": "ENABLED",
            "projectId": PROJECT_ID,
            "transferSpec": {
                "gcsDataSource": {
                    "bucketName": src_bucket
                },
                "gcsDataSink": {
                    "bucketName": DEST_BUCKET
                },
                "objectConditions": {
                    "includePrefixes": prefixes
                },
                "transferOptions": {
                    "deleteObjectsFromSourceAfterTransfer": delete_flag
                }
            },
            "schedule": {
                "scheduleStartDate": {
                    "year": datetime.utcnow().year,
                    "month": datetime.utcnow().month,
                    "day": datetime.utcnow().day
                },
                "scheduleEndDate": {
                    "year": datetime.utcnow().year,
                    "month": datetime.utcnow().month,
                    "day": datetime.utcnow().day
                }
            }
        }
        kwargs["ti"].xcom_push(key="sts_job_body", value=job_body)
        kwargs["ti"].xcom_push(key="sts_job_name", value=job_name)

    def get_sts_job_body(**kwargs):
        return kwargs["ti"].xcom_pull(key="sts_job_body", task_ids="create_sts_body")

    def wait_for_completion(**kwargs):
        from google.cloud import pubsub_v1
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(PROJECT_ID, STS_COMPLETION_SUBSCRIPTION)
        response = subscriber.pull(subscription=subscription_path, max_messages=1, timeout=60)
        if not response.received_messages:
            raise ValueError("No STS completion message received")
        msg = response.received_messages[0]
        subscriber.acknowledge(subscription=subscription_path, ack_ids=[msg.ack_id])

    def create_toc_file(**kwargs):
        cf_message = kwargs["ti"].xcom_pull(key="cf_message", task_ids="pull_cf_message")
        file_events = cf_message["file_events"]
        filenames = [f["cdm_file_prefix_pattern"] for f in file_events]
        toc_content = "\n".join(filenames)
        toc_name = f"toc_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.txt"
        client = storage.Client()
        bucket = client.bucket(DEST_BUCKET)
        blob = bucket.blob(toc_name)
        blob.upload_from_string(toc_content)
        kwargs["ti"].xcom_push(key="toc_file", value=toc_name)
        kwargs["ti"].xcom_push(key="toc_content", value=toc_content)

    def notify_or_log(**kwargs):
        toc_name = kwargs["ti"].xcom_pull(key="toc_file", task_ids="create_toc")
        toc_content = kwargs["ti"].xcom_pull(key="toc_content", task_ids="create_toc")
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, CDMNXT_TOPIC)
        try:
            publisher.publish(topic_path, toc_name.encode("utf-8"))
            print(f"[INFO] TOC filename published to CDMNXT_TOPIC: {toc_name}")
        except:
            print(f"[INFO] no topic, message: {toc_name}, not delivered")
        print(f"[TOC CONTENT]\n{toc_content}")

    pull_cf_message = PythonOperator(
        task_id="pull_cf_message",
        python_callable=pull_cf_message,
        provide_context=True
    )

    create_sts_body = PythonOperator(
        task_id="create_sts_body",
        python_callable=create_sts_job_body,
        provide_context=True
    )

    create_sts_job = CloudDataTransferServiceCreateJobOperator(
        task_id="create_sts_job",
        body=get_sts_job_body,
        project_id=PROJECT_ID
    )

    wait_for_sts = PythonOperator(
        task_id="wait_for_sts",
        python_callable=wait_for_completion,
        provide_context=True
    )

    create_toc = PythonOperator(
        task_id="create_toc",
        python_callable=create_toc_file,
        provide_context=True
    )

    notify = PythonOperator(
        task_id="notify",
        python_callable=notify_or_log,
        provide_context=True
    )

    pull_cf_message >> create_sts_body >> create_sts_job >> wait_for_sts >> create_toc >> notify
