
from datetime import datetime, timedelta
import json
import uuid
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceDeleteJobOperator,
)

PROJECT_ID = "sandbox-corp-gdw-sfr-cdb8"
DESTINATION_BUCKET = "gdw2-sandbox-corp-gdw-sfr-cdb8"
CF_TRIGGER_SUBSCRIPTION = "cf-trigger-dag"
STS_COMPLETION_SUBSCRIPTION = "sts-completion-sub"
CDMNXT_TOPIC = "cdmnxt-topic"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 30),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "gdw_sts_final_event_v2",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    def parse_cf_messages(**kwargs):
        messages = Variable.get("enriched_cf_messages", deserialize_json=True)
        grouped = {"true": [], "false": []}
        for msg in messages:
            key = str(msg["delete_after_transfer"]).lower()
            grouped[key].append(msg)
        kwargs["ti"].xcom_push(key="grouped_messages", value=grouped)

    def build_sts_body(delete_flag, **kwargs):
        grouped = kwargs["ti"].xcom_pull(task_ids="parse_cf_messages", key="grouped_messages")
        job_bodies = []
        for file in grouped.get(delete_flag, []):
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
            }
            job_bodies.append(job)
        kwargs["ti"].xcom_push(key=f"job_bodies_{delete_flag}", value=job_bodies)

    def get_sts_body(delete_flag, **kwargs):
        return kwargs["ti"].xcom_pull(task_ids=f"build_sts_job_body_{delete_flag}", key=f"job_bodies_{delete_flag}")[0]

    def generate_toc_file(delete_flag, **kwargs):
        grouped = kwargs["ti"].xcom_pull(task_ids="parse_cf_messages", key="grouped_messages")
        file_list = grouped.get(delete_flag, [])
        toc_name = f"toc_{delete_flag}_{str(uuid.uuid4())}.txt"
        with open(f"/tmp/{toc_name}", "w") as f:
            for file in file_list:
                f.write(json.dumps(file) + "\n")
        kwargs["ti"].xcom_push(key=f"toc_filename_{delete_flag}", value=toc_name)

    def notify_or_log(delete_flag, **kwargs):
        filename = kwargs["ti"].xcom_pull(task_ids=f"generate_toc_{delete_flag}", key=f"toc_filename_{delete_flag}")
        try:
            # simulate publish to Pub/Sub
            print(f"TOC file published: {filename}")
        except Exception:
            print(f"no topic, message: {filename}, not delivered")

    parse_cf = PythonOperator(
        task_id="parse_cf_messages",
        python_callable=parse_cf_messages,
    )

    for delete_flag in ["true", "false"]:
        build_body = PythonOperator(
            task_id=f"build_sts_job_body_{delete_flag}",
            python_callable=build_sts_body,
            op_kwargs={"delete_flag": delete_flag},
        )

        create_job = CloudDataTransferServiceCreateJobOperator(
            task_id=f"create_sts_job_{delete_flag}",
            body=lambda **kwargs: get_sts_body(delete_flag, **kwargs),
        )

        wait_for_completion = PubSubPullSensor(
            task_id=f"wait_sts_completion_{delete_flag}",
            project_id=PROJECT_ID,
            subscription=STS_COMPLETION_SUBSCRIPTION,
            ack_messages=True,
            timeout=600,
        )

        delete_job = CloudDataTransferServiceDeleteJobOperator(
            task_id=f"delete_sts_job_{delete_flag}",
            job_name="{{ task_instance.xcom_pull(task_ids='create_sts_job_" + delete_flag + "', key='job_name') }}",
            project_id=PROJECT_ID,
        )

        generate_toc = PythonOperator(
            task_id=f"generate_toc_{delete_flag}",
            python_callable=generate_toc_file,
            op_kwargs={"delete_flag": delete_flag},
        )

        notify_cdmnxt = PythonOperator(
            task_id=f"notify_cdmnxt_{delete_flag}",
            python_callable=notify_or_log,
            op_kwargs={"delete_flag": delete_flag},
        )

        parse_cf >> build_body >> create_job >> wait_for_completion >> delete_job >> generate_toc >> notify_cdmnxt
