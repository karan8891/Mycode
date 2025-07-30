
from airflow import models
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceUpdateJobOperator,
    CloudDataTransferServiceDeleteJobOperator,
)
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from google.cloud import pubsub_v1, storage
import json, base64
from datetime import datetime

PROJECT_ID = "sandbox-corp-gdw-sfr-cd08"
DEST_BUCKET = "gdw2_sandbox-corp-gdw-sfr-cd08"
COMPLETION_SUB = f"projects/{PROJECT_ID}/subscriptions/sts-completion-sub"
CDMNXT_TOPIC = f"projects/{PROJECT_ID}/topics/cdmnxt-trigger-topic"

default_args = {"start_date": days_ago(1)}

with models.DAG(
    "gdw_sts_event_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    def parse_apmf_event(**kwargs):
        ti = kwargs["ti"]
        raw = ti.xcom_pull(task_ids="wait_for_apmf_message")
        parsed = raw[0]
        message_data_b64 = parsed["message"]["data"]
        message_data = json.loads(base64.b64decode(message_data_b64).decode("utf-8"))
        file_events = message_data["file_events"]
        grouped = {True: [], False: []}
        for event in file_events:
            flag = event.get("delete_source_files_after_transfer", False)
            grouped[flag].append(event)
        ti.xcom_push(key="grouped_events", value=grouped)

    def build_transfer_payload(**kwargs):
        ti = kwargs["ti"]
        grouped = ti.xcom_pull(key="grouped_events", task_ids="parse_apmf_event")
        outputs = []
        for delete_flag, events in grouped.items():
            if not events:
                continue
            timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            job_id = f"del_{timestamp}" if delete_flag else f"keep_{timestamp}"
            filenames = [e.get("cdm_file_prefix_pattern", "") for e in events]
            toc_data = {
                "source_bucket": events[0]["cdm_source_bucket"],
                "source_objects": filenames,
                "delete_after_transfer": delete_flag,
                "files": filenames,
            }
            toc_filename = f"{job_id}/toc.json"
            storage.Client().bucket(DEST_BUCKET).blob(toc_filename).upload_from_string(
                json.dumps(toc_data, indent=2), content_type="application/json"
            )
            outputs.append({
                "job_id": job_id,
                "toc_file": toc_filename,
                "delete_flag": delete_flag,
                "conf": events[0],
            })
        for i, job in enumerate(outputs):
            ti.xcom_push(key=f"transfer_job_{i}", value=job)
        ti.xcom_push(key="transfer_jobs", value=outputs)

    def _get_job_body(idx, **kwargs):
        ti = kwargs["ti"]
        return ti.xcom_pull(task_ids="build_transfer_payload", key=f"transfer_job_{idx}")

    def _get_job_name(idx, **kwargs):
        ti = kwargs["ti"]
        job = ti.xcom_pull(task_ids=f"create_sts_job_{idx}")
        return job["name"]

    def notify_cdmnxt(**kwargs):
        ti = kwargs["ti"]
        transfers = ti.xcom_pull(key="transfer_jobs", task_ids="build_transfer_payload")
        publisher = pubsub_v1.PublisherClient()
        for t in transfers:
            msg = {
                "toc_file": t["toc_file"],
                "source_bucket": t["conf"]["cdm_source_bucket"],
                "source_project": t["conf"]["cdm_source_project_id"],
                "delete_after_transfer": t["delete_flag"]
            }
            try:
                publisher.publish(CDMNXT_TOPIC, json.dumps(msg).encode("utf-8")).result()
                print(f"[SUCCESS] Published to CDMNXT: {msg}")
            except:
                print(f"[INFO] no topic, message: {t['toc_file']}, not delivered")

    wait_for_apmf_message = PubSubPullSensor(
        task_id="wait_for_apmf_message",
        project_id=PROJECT_ID,
        subscription="projects/sandbox-corp-gdw-sfr-cd08/subscriptions/apmf-event-sub",
        ack_messages=True,
        max_messages=1,
        timeout=60,
    )

    parse_event = PythonOperator(
        task_id="parse_apmf_event",
        python_callable=parse_apmf_event,
    )

    build_payload = PythonOperator(
        task_id="build_transfer_payload",
        python_callable=build_transfer_payload,
    )

    task_chain = []
    for i in range(2):
        prepare_body = PythonOperator(
            task_id=f"prepare_body_{i}",
            python_callable=lambda **kwargs: _get_job_body(i, **kwargs),
        )

        create_job = CloudDataTransferServiceCreateJobOperator(
            task_id=f"create_sts_job_{i}",
            body=_get_job_body(i),
            project_id=PROJECT_ID,
        )

        update_job = CloudDataTransferServiceUpdateJobOperator(
            task_id=f"run_sts_job_{i}",
            job_name=_get_job_name(i),
            body={},
            project_id=PROJECT_ID,
        )

        wait_task = PubSubPullSensor(
            task_id=f"wait_for_completion_{i}",
            project_id=PROJECT_ID,
            subscription=COMPLETION_SUB,
            ack_messages=True,
            max_messages=5,
            timeout=600,
        )

        delete_job = CloudDataTransferServiceDeleteJobOperator(
            task_id=f"delete_sts_job_{i}",
            job_name=_get_job_name(i),
            project_id=PROJECT_ID,
        )

        task_chain += [prepare_body >> create_job >> update_job >> wait_task >> delete_job]

    notify = PythonOperator(
        task_id="notify_cdmnxt",
        python_callable=notify_cdmnxt,
    )

    wait_for_apmf_message >> parse_event >> build_payload >> task_chain >> notify
