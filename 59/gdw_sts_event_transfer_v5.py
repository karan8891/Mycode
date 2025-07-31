
import json
import base64
import uuid
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceDeleteJobOperator
)
from google.cloud import storage, pubsub_v1

PROJECT_ID = "sandbox-corp-gdw-sfr-cdb8"
TOPIC_NAME = "sts-completion-topic"
CF_SUBSCRIPTION = "cf-trigger-dag"
STS_COMPLETION_SUBSCRIPTION = "sts-completion-sub"
DESTINATION_BUCKET = "gdw2_sandbox-corp-gdw-sfr-cdb8"

default_args = {
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="Five9_to_CDM_Test",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["sts", "event-driven"],
) as dag:

    def parse_cf_message(**kwargs):
        ti = kwargs["ti"]
        messages = ti.xcom_pull(task_ids="pull_cf_event")
        enriched_events = []
        for msg in messages:
            try:
                raw_data = msg.get("message", {}).get("data", "")
                if not raw_data:
                    continue
                decoded = base64.b64decode(raw_data).decode("utf-8").strip()
                if not decoded:
                    continue
                payload = json.loads(decoded)
                enriched_events.append({
                    "cdm_source_bucket": payload.get("cdm_source_bucket"),
                    "cdm_file_prefix_pattern": payload.get("cdm_file_prefix_pattern", ""),
                    "cdm_source_project_id": payload.get("cdm_source_project_id"),
                    "delete_source_files_after_transfer": payload.get("delete_source_files_after_transfer", False)
                })
            except Exception as e:
                print(f"[ERROR] Skipping malformed message: {e}")
        ti.xcom_push(key="enriched_events", value=enriched_events)

    def create_sts_body(**kwargs):
        ti = kwargs["ti"]
        enriched_events = ti.xcom_pull(task_ids="parse_cf_event", key="enriched_events")
        if not enriched_events:
            raise ValueError("No enriched events to process.")

        delete_flag = enriched_events[0].get("delete_source_files_after_transfer", False)
        job_id = f"sts-job-{uuid.uuid4().hex[:8]}"
        source_bucket = enriched_events[0]["cdm_source_bucket"]

        prefixes = list({e["cdm_file_prefix_pattern"] for e in enriched_events if e["cdm_file_prefix_pattern"]})

        body = {
            "description": f"Transfer {len(prefixes)} objects with delete={delete_flag}",
            "projectId": PROJECT_ID,
            "transferSpec": {
                "gcsDataSource": {"bucketName": source_bucket},
                "gcsDataSink": {"bucketName": DESTINATION_BUCKET},
                "objectConditions": {"includePrefixes": prefixes},
                "transferOptions": {
                    "deleteObjectsFromSourceAfterTransfer": delete_flag,
                    "overwriteObjectsAlreadyExistingInSink": True,
                },
            },
            "schedule": {
                "scheduleStartDate": {"year": 2025, "month": 1, "day": 1},
                "scheduleEndDate": {"year": 2099, "month": 12, "day": 31}
            },
            "notificationConfig": {
                "pubsubTopic": f"projects/{PROJECT_ID}/topics/{TOPIC_NAME}",
                "eventTypes": ["TRANSFER_OPERATION_SUCCESS"],
                "payloadFormat": "JSON",
            },
            "status": "ENABLED",
        }

        ti.xcom_push(key="sts_body", value=body)
        ti.xcom_push(key="job_name", value=f"transferJobs/{PROJECT_ID}/{job_id}")
        ti.xcom_push(key="delete_flag", value=delete_flag)
        ti.xcom_push(key="destination_bucket", value=DESTINATION_BUCKET)
        ti.xcom_push(key="prefixes", value=prefixes)

    def get_sts_body(**kwargs):
        return kwargs["ti"].xcom_pull(task_ids="build_sts_job", key="sts_body")

    def get_sts_job_name(**kwargs):
        return kwargs["ti"].xcom_pull(task_ids="build_sts_job", key="job_name")

    pull_cf_event = PubSubPullSensor(
        task_id="pull_cf_event",
        project_id=PROJECT_ID,
        subscription=CF_SUBSCRIPTION,
        max_messages=10,
        ack_messages=True,
        timeout=30,
    )

    parse_cf_event = PythonOperator(
        task_id="parse_cf_event",
        python_callable=parse_cf_message,
    )

    build_sts_job = PythonOperator(
        task_id="build_sts_job",
        python_callable=create_sts_body,
    )

    create_sts_job = CloudDataTransferServiceCreateJobOperator(
        task_id="create_sts_job",
        body=get_sts_body,
    )

    wait_sts_completion = PubSubPullSensor(
        task_id="wait_sts_completion",
        project_id=PROJECT_ID,
        subscription=STS_COMPLETION_SUBSCRIPTION,
        ack_messages=True,
        timeout=600,
    )

    def create_toc_file(**kwargs):
        ti = kwargs["ti"]
        prefixes = ti.xcom_pull(task_ids="build_sts_job", key="prefixes")
        destination_bucket = ti.xcom_pull(task_ids="build_sts_job", key="destination_bucket")
        toc_contents = "\n".join(prefixes)
        toc_file_name = f"toc_files/TOC_{datetime.utcnow().isoformat()}.txt"

        client = storage.Client()
        bucket = client.bucket(destination_bucket)
        blob = bucket.blob(toc_file_name)
        blob.upload_from_string(toc_contents)

        ti.xcom_push(key="toc_file_name", value=toc_file_name)

    upload_toc = PythonOperator(
        task_id="upload_toc",
        python_callable=create_toc_file,
    )

    def publish_toc(**kwargs):
        ti = kwargs["ti"]
        toc_file_name = ti.xcom_pull(task_ids="upload_toc", key="toc_file_name")
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
        try:
            publisher.publish(topic_path, data=toc_file_name.encode("utf-8"))
        except Exception:
            print(f"TOC file published: {toc_file_name} (no topic, not delivered)")

    notify_cdmnxt = PythonOperator(
        task_id="notify_cdmnxt",
        python_callable=publish_toc,
    )

    delete_sts_job = CloudDataTransferServiceDeleteJobOperator(
        task_id="delete_sts_job",
        job_name=get_sts_job_name,
        project_id=PROJECT_ID,
    )

    (
        pull_cf_event
        >> parse_cf_event
        >> build_sts_job
        >> create_sts_job
        >> wait_sts_completion
        >> upload_toc
        >> notify_cdmnxt
        >> delete_sts_job
    )
