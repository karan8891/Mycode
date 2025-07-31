
from datetime import datetime, timedelta
import json
import uuid

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from google.cloud import pubsub_v1, storage

PROJECT_ID = "sandbox-corp-gdw-sfr-cdb"
DESTINATION_BUCKET = "gdw2_sandbox-corp-gdw-sfr-cdb"
CF_TRIGGER_SUBSCRIPTION = "cf-trigger-dag"
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
    dag_id="gdw_sts_event_v7_final",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["event-driven", "sts"],
) as dag:

    def pull_cf_message(**kwargs):
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(PROJECT_ID, CF_TRIGGER_SUBSCRIPTION)
        response = subscriber.pull(subscription=subscription_path, max_messages=1)
        ack_ids = []
        for msg in response.received_messages:
            kwargs["ti"].xcom_push(key="cf_msg", value=msg.message.data.decode("utf-8"))
            ack_ids.append(msg.ack_id)
        if ack_ids:
            subscriber.acknowledge(subscription=subscription_path, ack_ids=ack_ids)

    def build_sts_body(**kwargs):
        import time
        msg = kwargs["ti"].xcom_pull(key="cf_msg")
        cf_message = json.loads(msg)
        file_event = cf_message["file_events"][0]
        source_bucket = file_event["cdm_source_bucket"]
        prefix = file_event["cdm_file_prefix_pattern"]
        source_project = file_event["cdm_source_project_id"]
        delete_flag = file_event["delete_source_files_after_transfer"]
        job_name = f"transfer_job_{uuid.uuid4().hex}"

        transfer_job = {
            "description": f"Transfer from {source_bucket} to {DESTINATION_BUCKET}",
            "status": "ENABLED",
            "projectId": PROJECT_ID,
            "transferSpec": {
                "gcsDataSource": {
                    "bucketName": source_bucket,
                },
                "gcsDataSink": {
                    "bucketName": DESTINATION_BUCKET,
                },
                "objectConditions": {
                    "includePrefixes": [prefix]
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
                "startTimeOfDay": {
                    "hours": datetime.utcnow().hour,
                    "minutes": datetime.utcnow().minute,
                    "seconds": datetime.utcnow().second
                }
            }
        }
        kwargs["ti"].xcom_push(key="sts_body", value=transfer_job)
        kwargs["ti"].xcom_push(key="toc_file_prefix", value=prefix)

    def get_sts_body(**kwargs):
        return kwargs["ti"].xcom_pull(task_ids="build_sts_body", key="sts_body")

    def create_toc_file(**kwargs):
        prefix = kwargs["ti"].xcom_pull(key="toc_file_prefix")
        storage_client = storage.Client()
        blobs = storage_client.list_blobs(DESTINATION_BUCKET, prefix=prefix)
        filenames = [blob.name for blob in blobs]
        toc_filename = f"toc_{uuid.uuid4().hex}.txt"
        toc_blob = storage_client.bucket(DESTINATION_BUCKET).blob(toc_filename)
        toc_blob.upload_from_string("\n".join(filenames))
        kwargs["ti"].xcom_push(key="toc_filename", value=toc_filename)
        kwargs["ti"].xcom_push(key="toc_file_content", value="\n".join(filenames))

    def wait_for_completion(**kwargs):
        sensor = PubSubPullSensor(
            task_id="wait_for_completion_sensor",
            project_id=PROJECT_ID,
            subscription=STS_COMPLETION_SUBSCRIPTION,
            max_messages=1,
            ack_messages=True,
            timeout=60
        )
        sensor.execute(context=kwargs)

    def notify(**kwargs):
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, CDMNXT_TOPIC)
        toc_name = kwargs["ti"].xcom_pull(key="toc_filename")
        content = kwargs["ti"].xcom_pull(key="toc_file_content")
        try:
            publisher.publish(topic_path, toc_name.encode("utf-8"))
            print(f"[INFO] TOC file published to CDMNXT_TOPIC: {toc_name}")
        except Exception:
            print(f"[INFO] no topic, message: {toc_name}, not delivered")
        print(f"[TOC CONTENT]\n{content}")

    t1 = PythonOperator(task_id="pull_cf_message", python_callable=pull_cf_message)
    t2 = PythonOperator(task_id="build_sts_body", python_callable=build_sts_body)
    t3 = CloudDataTransferServiceCreateJobOperator(
        task_id="create_sts_job",
        project_id=PROJECT_ID,
        body=get_sts_body(),
    )
    t4 = PythonOperator(task_id="wait_for_completion", python_callable=wait_for_completion)
    t5 = PythonOperator(task_id="create_toc", python_callable=create_toc_file)
    t6 = PythonOperator(task_id="notify", python_callable=notify)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
