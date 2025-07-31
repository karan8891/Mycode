from datetime import datetime, timedelta import json import base64 from airflow import DAG from airflow.operators.python import PythonOperator from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor from google.cloud import storage_transfer_v1

Constants

PROJECT_ID = "cdm-gcp-data-dev" DEST_BUCKET = "cdm-gcp-data-destination-dev" SUBSCRIPTION_NAME = "cf-trigger-dag" TOPIC_PROJECT = "cdm-gcp-data-dev"

Default args

default_args = { 'owner': 'airflow', 'start_date': datetime(2023, 1, 1), 'retries': 1, 'retry_delay': timedelta(minutes=5) }

def extract_cf_message(**kwargs): messages = kwargs['ti'].xcom_pull(task_ids='wait_for_cf_message') decoded = base64.b64decode(messages[0]['message']['data']).decode('utf-8') parsed = json.loads(decoded) file_event = parsed['file_events'][0]

source_bucket = file_event['cdm_source_bucket']
source_project_id = file_event['cdm_source_project_id']
prefix = file_event['cdm_file_prefix_pattern']
delete_flag = file_event['delete_source_files_after_transfer']

kwargs['ti'].xcom_push(key='source_bucket', value=source_bucket)
kwargs['ti'].xcom_push(key='source_project_id', value=source_project_id)
kwargs['ti'].xcom_push(key='prefix', value=prefix)
kwargs['ti'].xcom_push(key='delete_flag', value=delete_flag)

def create_sts_job_from_values(**kwargs): ti = kwargs['ti'] source_bucket = ti.xcom_pull(key='source_bucket', task_ids='extract_cf_message') prefix = ti.xcom_pull(key='prefix', task_ids='extract_cf_message') delete_flag = ti.xcom_pull(key='delete_flag', task_ids='extract_cf_message')

job_body = {
    "description": "Dynamically created STS job",
    "status": "ENABLED",
    "projectId": PROJECT_ID,
    "transferSpec": {
        "gcsDataSource": {
            "bucketName": source_bucket
        },
        "gcsDataSink": {
            "bucketName": DEST_BUCKET
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
            "year": datetime.now().year,
            "month": datetime.now().month,
            "day": datetime.now().day
        },
        "scheduleEndDate": {
            "year": datetime.now().year,
            "month": datetime.now().month,
            "day": datetime.now().day
        },
        "startTimeOfDay": {
            "hours": datetime.now().hour,
            "minutes": datetime.now().minute + 1
        }
    }
}

client = storage_transfer_v1.StorageTransferServiceClient()
client.create_transfer_job(job_body)

with DAG( dag_id="sts_create_job_from_cf", default_args=default_args, schedule_interval=None, catchup=False, tags=["sts", "event", "cf"], ) as dag:

wait_for_cf_message = PubSubPullSensor(
    task_id="wait_for_cf_message",
    project_id=TOPIC_PROJECT,
    subscription=SUBSCRIPTION_NAME,
    max_messages=1,
    ack_messages=True,
    timeout=30
)

extract_cf_fields = PythonOperator(
    task_id="extract_cf_message",
    python_callable=extract_cf_message
)

create_sts_job = PythonOperator(
    task_id="create_sts_job",
    python_callable=create_sts_job_from_values
)

wait_for_cf_message >> extract_cf_fields >> create_sts_job

