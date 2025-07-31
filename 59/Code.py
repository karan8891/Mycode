from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.operators.python import PythonOperator

import json
import base64

PROJECT_ID = "sandbox-corp-gdw-sfr-cdp"
SUBSCRIPTION_NAME = "cf-trigger-dag"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="log_cf_message_only_v3",
    default_args=default_args,
    description="Print APMF CF message from PubSub",
    schedule_interval=None,
    catchup=False,
    start_date=days_ago(1),
    tags=["cf", "pubsub", "debug"],
) as dag:

    wait_for_cf_message = PubSubPullSensor(
        task_id="wait_for_cf_message",
        project_id=PROJECT_ID,
        subscription=SUBSCRIPTION_NAME,
        ack_messages=True,
        max_messages=1,  # ✅ Use this instead of messages_count
        timeout=60,
    )

    def print_cf_payload(**kwargs):
        messages = kwargs["ti"].xcom_pull(task_ids="wait_for_cf_message")
        if not messages:
            print("⚠️ No message received.")
            return
        for msg in messages:
            print("📨 Raw Pub/Sub Message:", msg)
            try:
                raw_data = msg.get("message", {}).get("data", "")
                decoded = base64.b64decode(raw_data).decode("utf-8")
                parsed = json.loads(decoded)
                print("✅ Decoded CF Message:")
                print(json.dumps(parsed, indent=2))
            except Exception as e:
                print("❌ Failed to decode or parse message:", str(e))
                print("Original raw data:", raw_data)

    log_message = PythonOperator(
        task_id="log_message",
        python_callable=print_cf_payload,
    )

    wait_for_cf_message >> log_message
