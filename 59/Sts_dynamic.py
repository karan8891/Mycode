from datetime import datetime, timedelta import json from airflow import models from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import ( CloudDataTransferServiceCreateJobOperator, CloudDataTransferServiceRunJobOperator, CloudDataTransferServiceDeleteJobOperator, ) from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor from airflow.operators.python import PythonOperator from airflow.utils.trigger_rule import TriggerRule

Configurable Variables

PROJECT_ID = "your-project-id" TRANSFER_JOB_LOCATION = "us" SOURCE_BUCKET = "client-source-bucket" DEST_BUCKET = "our-destination-bucket" CF_TRIGGER_TOPIC = "client-topic-trigger" STS_COMPLETION_TOPIC = "sts-completion-topic"

def build_transfer_job(**context): message = context['dag_run'].conf or {} file_prefix = message.get("prefix", "") delete_after_transfer = message.get("delete_after_transfer", "no").lower() == "yes"

job_name = f"transfer_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

job = {
    "description": "Client-triggered recurring transfer job",
    "status": "ENABLED",
    "projectId": PROJECT_ID,
    "transferSpec": {
        "gcsDataSource": {"bucketName": SOURCE_BUCKET},
        "gcsDataSink": {"bucketName": DEST_BUCKET},
        "objectConditions": {"includePrefixes": [file_prefix]} if file_prefix else {},
        "transferOptions": {
            "deleteObjectsFromSourceAfterTransfer": delete_after_transfer,
            "overwriteObjectsAlreadyExistingInSink": True,
        },
    },
    "schedule": {"scheduleStartDate": {"year": 2023, "month": 1, "day": 1}},
}
context['ti'].xcom_push(key="job_config", value=job)
context['ti'].xcom_push(key="job_name", value=job_name)

def parse_completion_message(messages, **context): for message in messages: data = json.loads(message['message']['data']) if data.get("transferJobName") == context['ti'].xcom_pull(key="job_name"): return True return False

def extract_toc_or_next_action(**context): # Placeholder for logic to extract TOC or further route print("Processing completion message")

def cleanup_sts_job(**context): return context['ti'].xcom_pull(key="job_name")

def get_created_job(**context): return context['ti'].xcom_pull(key="job_name")

def get_job_body(**context): return context['ti'].xcom_pull(key="job_config")

with models.DAG( "client_pubsub_sts_dag", schedule_interval=None, start_date=datetime(2023, 1, 1), catchup=False, tags=["sts", "event-driven", "client"], default_args={"retries": 1, "retry_delay": timedelta(minutes=2)} ) as dag:

build_job = PythonOperator(
    task_id="build_transfer_job",
    python_callable=build_transfer_job,
    provide_context=True,
)

create_sts_job = CloudDataTransferServiceCreateJobOperator(
    task_id="create_sts_job",
    body=get_job_body,
    do_xcom_push_job_name=True,
)

run_sts_job = CloudDataTransferServiceRunJobOperator(
    task_id="run_sts_job",
    job_name=get_created_job,
)

wait_for_completion = PubSubPullSensor(
    task_id="wait_for_completion",
    project_id=PROJECT_ID,
    subscription="sts-completion-subscription",
    ack_messages=True,
    messages_callback=parse_completion_message,
    timeout=600,
    poke_interval=30,
    provide_context=True,
)

process_toc_or_next = PythonOperator(
    task_id="process_completion_logic",
    python_callable=extract_toc_or_next_action,
)

delete_sts_job = CloudDataTransferServiceDeleteJobOperator(
    task_id="delete_sts_job",
    job_name=cleanup_sts_job,
    trigger_rule=TriggerRule.ALL_DONE,
)

build_job >> create_sts_job >> run_sts_job >> wait_for_completion >> process_toc_or_next >> delete_sts_job
