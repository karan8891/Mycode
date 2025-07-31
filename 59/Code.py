from airflow import DAG
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import base64
import json

PROJECT_ID = "sandbox-corp-cmp-gdw-sfr-cdp"
CF_TRIGGER_SUBSCRIPTION = "cf-trigger-sub"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 7, 31),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="log_cf_message_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["debug", "cf", "apmf"],
) as dag:

    wait_for_cf_message = PubSubPullSensor(
        task_id="wait_for_cf_message",
        project_id=PROJECT_ID,
        subscription=CF_TRIGGER_SUBSCRIPTION,
        ack_messages=True,
        max_messages=1,
        timeout=60,
    )

    def print_cf_payload(message, **kwargs):
        if not message or not isinstance(message, list):
            print("No message received.")
            return

        # decode base64 data
        raw_data = message[0].get("message", {}).get("data", "")
        if not raw_data:
            print("Message received but no data found.")
            return

        decoded_bytes = base64.b64decode(raw_data)
        decoded_str = decoded_bytes.decode("utf-8")
        try:
            json_data = json.loads(decoded_str)
            print("✅ Decoded CF Message:")
            print(json.dumps(json_data, indent=2))
        except Exception as e:
            print("⚠️ Failed to parse message as JSON:")
            print(decoded_str)
            raise e

    log_message = PythonOperator(
        task_id="log_message",
        python_callable=print_cf_payload,
        op_kwargs={"message": "{{ ti.xcom_pull(task_ids='wait_for_cf_message') }}"},
    )

    wait_for_cf_message >> log_message
