from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
from datetime import datetime, timedelta
from google.cloud import pubsub_v1, storage
import uuid
import json

PROJECT_ID = "sandbox-corp-gdw-sfr-cdb8"
DEST_BUCKET = "gdw2_sandbox-corp-gdw-sfr-cdb8"
CF_TRIGGER_SUBSCRIPTION = "cf-trigger-dag"
STS_COMPLETION_SUBSCRIPTION = "sts-completion-sub"
CDMNXT_TOPIC = "cdmnxt-trigger-topic"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="Five9_to_CDM_Test",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["sts", "event-driven"]
) as dag:

    def pull_cf_message(**kwargs):
        subscriber = pubsub_v1.SubscriberClient()
        sub_path = subscriber.subscription_path(PROJECT_ID, CF_TRIGGER_SUBSCRIPTION)
        resp = subscriber.pull(subscription=sub_path, max_messages=1, timeout=10)
        if not resp.received_messages:
            raise ValueError("No messages received from CF subscription")
        msg = resp.received_messages[0]
        message_data = json.loads(msg.message.data.decode("utf-8"))
        subscriber.acknowledge(subscription=sub_path, ack_ids=[msg.ack_id])
        kwargs["ti"].xcom_push(key="cf_message", value=message_data)

    def build_sts_body(**kwargs):
        msg = kwargs["ti"].xcom_pull(key="cf_message", task_ids="pull_cf_message")
        events = msg["file_events"]
        delete_flag = events[0].get("delete_source_files_after_transfer", False)
        src_bucket = events[0]["cdm_source_bucket"]
        prefixes = [f["cdm_file_prefix_pattern"] for f in events]

        job_name = f"transferJob-{uuid.uuid4()}"
        body = {
            "description": job_name,
            "status": "ENABLED",
            "projectId": PROJECT_ID,
            "transferSpec": {
                "gcsDataSource": {"bucketName": src_bucket},
                "gcsDataSink": {"bucketName": DEST_BUCKET},
                "objectConditions": {"includePrefixes": prefixes},
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
        kwargs["ti"].xcom_push(key="sts_job_body", value=body)

    def get_runtime_sts_body(**kwargs):
        return kwargs["ti"].xcom_pull(task_ids="build_sts_body", key="sts_job_body")

    def wait_for_completion(**kwargs):
        subscriber = pubsub_v1.SubscriberClient()
        sub_path = subscriber.subscription_path(PROJECT_ID, STS_COMPLETION_SUBSCRIPTION)
        response = subscriber.pull(subscription=sub_path, max_messages=1, timeout=60)
        if not response.received_messages:
            raise ValueError("No completion message received")
        msg = response.received_messages[0]
        subscriber.acknowledge(subscription=sub_path, ack_ids=[msg.ack_id])

    def create_toc_file(**kwargs):
        msg = kwargs["ti"].xcom_pull(key="cf_message", task_ids="pull_cf_message")
        filenames = [f["cdm_file_prefix_pattern"] for f in msg["file_events"]]
        toc = "\n".join(filenames)
        name = f"toc_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.txt"
        client = storage.Client()
        bucket = client.bucket(DEST_BUCKET)
        blob = bucket.blob(name)
        blob.upload_from_string(toc)
        kwargs["ti"].xcom_push(key="toc_file", value=name)
        kwargs["ti"].xcom_push(key="toc_content", value=toc)

    def notify(**kwargs):
        toc_name = kwargs["ti"].xcom_pull(task_ids="create_toc", key="toc_file")
        toc_content = kwargs["ti"].xcom_pull(task_ids="create_toc", key="toc_content")
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, CDMNXT_TOPIC)
        try:
            publisher.publish(topic_path, toc_name.encode("utf-8"))
            print(f"[INFO] TOC filename published to CDMNXT_TOPIC: {toc_name}")
        except Exception:
            print(f"[INFO] no topic, message: {toc_name}, not delivered")
        print(f"[TOC CONTENT]\n{toc_content}")

    t1 = PythonOperator(task_id="pull_cf_message", python_callable=pull_cf_message, provide_context=True)
    t2 = PythonOperator(task_id="build_sts_body", python_callable=build_sts_body, provide_context=True)
    t3 = CloudDataTransferServiceCreateJobOperator(
        task_id="create_sts_job",
        project_id=PROJECT_ID,
        body=get_runtime_sts_body
    )
    t4 = PythonOperator(task_id="wait_for_sts", python_callable=wait_for_completion, provide_context=True)
    t5 = PythonOperator(task_id="create_toc", python_callable=create_toc_file, provide_context=True)
    t6 = PythonOperator(task_id="notify", python_callable=notify, provide_context=True)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
