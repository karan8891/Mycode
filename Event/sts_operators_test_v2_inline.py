from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import base64
import json

PROJECT_ID = "sandbox-corp-gdw-sfr-cdb8"
DEST_BUCKET = "gdw2-sandbox-corp-gdw-sfr-cdb8"
SUBSCRIPTION_NAME = "cf-trigger-dag"

default_args = {{
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}}

def extract_cf_message(**kwargs):
    messages = kwargs["ti"].xcom_pull(task_ids="wait_for_cf_message", key="messages")
    if not messages or len(messages) == 0:
        raise ValueError("No messages received from CF")

    message_data = messages[0].get("message", {{}}).get("data")
    if not message_data:
        raise ValueError("Message has no data field")

    decoded = base64.b64decode(message_data).decode("utf-8")
    parsed = json.loads(decoded)

    kwargs["ti"].xcom_push(key="source_bucket", value=parsed["cdm_source_bucket"])
    kwargs["ti"].xcom_push(key="prefix", value=parsed["cdm_file_prefix_pattern"])
    kwargs["ti"].xcom_push(key="delete_flag", value=parsed["delete_source_files_after_transfer"])

with DAG(
    dag_id="Five9_to_CDM_Test",
    default_args=default_args,
    description="Inline body STS job from parsed CF message",
    schedule_interval=None,
    start_date=datetime(2025, 7, 30),
    catchup=False,
    tags=["cf", "sts", "inline"],
) as dag:

    wait_for_cf_message = PubSubPullSensor(
        task_id="wait_for_cf_message",
        project_id=PROJECT_ID,
        subscription=SUBSCRIPTION_NAME,
        ack_messages=True,
        max_messages=1,
        timeout=60,
    )

    extract_cf = PythonOperator(
        task_id="extract_cf",
        python_callable=extract_cf_message,
    )

    create_sts_job = CloudDataTransferServiceCreateJobOperator(
        task_id="create_sts_job",
        project_id=PROJECT_ID,
        body={{
            "description": "CF-triggered STS job",
            "status": "ENABLED",
            "projectId": PROJECT_ID,
            "transferSpec": {{
                "gcsDataSource": {{
                    "bucketName": "{{{{ ti.xcom_pull(task_ids='extract_cf', key='source_bucket') }}}}"
                }},
                "gcsDataSink": {{
                    "bucketName": DEST_BUCKET
                }},
                "objectConditions": {{
                    "includePrefixes": ["{{{{ ti.xcom_pull(task_ids='extract_cf', key='prefix') }}}}"]
                }},
                "transferOptions": {{
                    "deleteObjectsFromSourceAfterTransfer": "{{{{ ti.xcom_pull(task_ids='extract_cf', key='delete_flag') }}}}"
                }}
            }},
            "schedule": {{
                "scheduleStartDate": {{
                    "year": datetime.now().year,
                    "month": datetime.now().month,
                    "day": datetime.now().day
                }},
                "scheduleEndDate": {{
                    "year": datetime.now().year,
                    "month": datetime.now().month,
                    "day": datetime.now().day
                }},
                "startTimeOfDay": {{
                    "hours": datetime.now().hour,
                    "minutes": datetime.now().minute + 1
                }}
            }}
        }},
    )

    wait_for_cf_message >> extract_cf >> create_sts_job