from airflow import models
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceUpdateJobOperator,
    CloudDataTransferServiceDeleteJobOperator,
)
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from google.cloud import pubsub_v1, storage
import json
import uuid
from datetime import datetime
import base64

PROJECT_ID = "sandbox-corp-gdw-sfr-cdb8"
DEST_BUCKET = "gdw2_sandbox-corp-gdw-sfr-cdb8"
COMPLETION_SUB = f"projects/{{PROJECT_ID}}/subscriptions/sts-completion-sub"
CDMNXT_TOPIC = f"projects/{{PROJECT_ID}}/topics/cdmnxt-trigger-topic"

def parse_apmf_event(**kwargs):
    ti = kwargs["ti"]
    raw = ti.xcom_pull(task_ids="wait_for_apmf_message")
    try:
        parsed = raw[0]  # first element from the pulled list
        message_data_b64 = parsed["message"]["data"]  # base64-encoded string
        message_data = json.loads(base64.b64decode(message_data_b64).decode("utf-8"))  # Decode and parse
        file_events = message_data["file_events"]
    except Exception as e:
        raise ValueError(f"[ERROR] Failed to parse message: {raw}, error: {e}")

    grouped = {True: [], False: []}
    for event in file_events:
        flag = event.get("delete_source_files_after_transfer", False)
        grouped[flag].append(event)

    ti.xcom_push(key="grouped_events", value=grouped)

def build_transfer_payload(**kwargs):
    grouped = kwargs["ti"].xcom_pull(key="grouped_events", task_ids="parse_apmf_event")
    outputs = []

    for delete_flag, events in grouped.items():
        if not events:
            continue

        job_id = f"del-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}" if delete_flag else f"keep-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        filenames = [e.get("cdm_file_prefix_pattern", "") for e in events]

        toc_data = {
            "source_bucket": "cdm_source_bucket",
            "source_objects": filenames,
            "delete_after_transfer": delete_flag,
            "files": filenames
        }
        toc_filename = f"{job_id}/toc.json"
        storage.Client().bucket(DEST_BUCKET).blob(toc_filename).upload_from_string(
            json.dumps(toc_data, indent=2), content_type="application/json"
        )
        print(f"[INFO] Uploaded TOC: gs://{DEST_BUCKET}/{toc_filename}")

        outputs.append({
            "job_id": job_id,
            "toc_file": toc_filename,
            "delete_flag": delete_flag,
            "conf": {
                "cdm_source_bucket": "cdm_source_bucket",
                "cdm_source_project_id": "cdm_source_project_id",
                "cdm_file_prefix_pattern": filenames,
            },
        })

    for i, job in enumerate(outputs):
        kwargs["ti"].xcom_push(key=f"transfer_job_{i}", value=job)

    kwargs["ti"].xcom_push(key="transfer_jobs", value=outputs)

def create_sts_job(conf, job_id, delete_flag):
    prefix = conf.get("cdm_file_prefix_pattern", "")
    return {
        "description": f"{conf['cdm_source_bucket']} to gdw delete={delete_flag}",
        "project_id": PROJECT_ID,
        "transfer_spec": {
            "gcs_data_source": {"bucket_name": conf["cdm_source_bucket"]},
            "gcs_data_sink": {"bucket_name": DEST_BUCKET},
            "object_conditions": {"include_prefixes": prefix},
            "transfer_options": {"delete_objects_from_source_after_transfer": delete_flag},
        },
        "notification_config": {
            "pubsub_topic": f"projects/{PROJECT_ID}/topics/sts-completion-topic",
            "event_types": ["TRANSFER_OPERATION_SUCCESS"],
            "payload_format": "JSON",
        },
    }

def notify_cdmnxt(**kwargs):
    transfers = kwargs["ti"].xcom_pull(key="transfer_jobs", task_ids="build_transfer_payload")
    publisher = pubsub_v1.PublisherClient()

    for t in transfers:
        msg = {
            "toc_file": t["toc_file"],
            "source_bucket": t["conf"]["cdm_source_bucket"],
            "source_project": t["conf"]["cdm_source_project_id"],
            "delete_after_transfer": t["delete_flag"],
        }
        try:
            publisher.publish(CDMNXT_TOPIC, json.dumps(msg).encode("utf-8")).result()
            print(f"[SUCCESS] Published to CDMNXT: {msg}")
        except Exception:
            print(f"[INFO] no topic, message: {{t['toc_file']}}, not delivered")

with models.DAG(
    dag_id="Five9_to_CDM_Test",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["apmf", "gdw", "sts", "event-driven"],
) as dag:

    wait_for_apmf_message = PubSubPullSensor(
        task_id="wait_for_apmf_message",
        project_id=PROJECT_ID,
        subscription="cf-trigger-dag",
        ack_messages=True,
        max_messages=1,
        timeout=300,
    )

    parse_apmf_event_task = PythonOperator(
        task_id="parse_apmf_event",
        python_callable=parse_apmf_event,
        provide_context=True,
    )

    build_transfer_payload_task = PythonOperator(
        task_id="build_transfer_payload",
        python_callable=build_transfer_payload,
        provide_context=True,
    )

    notify_cdmnxt_task = PythonOperator(
        task_id="notify_cdmnxt",
        python_callable=notify_cdmnxt,
        provide_context=True,
    )

    def create_update_run_tasks(task_group_name, idx):
        return [
            CloudDataTransferServiceCreateJobOperator(
                task_id=f"create_sts_job_{idx}",
                body='{{ task_instance.xcom_pull(task_ids="build_transfer_payload", key="transfer_jobs")[' + str(idx) + '] | tojson }}',
                do_xcom_push=True,
            ),
            CloudDataTransferServiceUpdateJobOperator(
                task_id=f"run_sts_job_{idx}",
                job_name='{{ task_instance.xcom_pull(task_ids="create_sts_job_' + str(idx) + '")["name"] }}',
                project_id=PROJECT_ID,
                body={"status": "ENABLED"},
            ),
            PubSubPullSensor(
                task_id=f"wait_for_completion_{idx}",
                project_id=PROJECT_ID,
                subscription=COMPLETION_SUB,
                ack_messages=True,
                max_messages=5,
                timeout=600,
            ),
            CloudDataTransferServiceDeleteJobOperator(
                task_id=f"delete_sts_job_{idx}",
                job_name='{{ task_instance.xcom_pull(task_ids="create_sts_job_' + str(idx) + '")["name"] }}',
                project_id=PROJECT_ID,
            ),
        ]

    wait_for_apmf_message >> parse_apmf_event_task >> build_transfer_payload_task

    for i in range(2):
        tasks = create_update_run_tasks("sts_group", i)
        build_transfer_payload_task >> tasks[0] >> tasks[1] >> tasks[2] >> tasks[3] >> notify_cdmnxt_task
