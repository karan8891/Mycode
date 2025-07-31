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
    dag_id="log_cf_message_only_v2",
    default_args=default_args,
    description="Pull from cf-trigger-dag and print message body",
    schedule_interval=None,
    catchup=False,
    start_date=days_ago(1),
    tags=["test", "pubsub", "cf"],
) as dag:

    wait_for_cf_message = PubSubPullSensor(
        task_id="wait_for_cf_message",
        project_id=PROJECT_ID,
        subscription=SUBSCRIPTION_NAME,
        ack_messages=True,
        max_messages=1,  # ✅ Correct param for v10.1.1
        timeout=60,
    )

    def _print_cf_message(**kwargs):
        messages = kwargs["ti"].xcom_pull(task_ids="wait_for_cf_message")
        if not messages:
            print("⚠️ No messages received.")
            return
        for msg in messages:
            print("📨 Raw message:", msg)
            try:
                decoded = base64.b64decode(msg["message"]["data"]).decode("utf-8")
                print("✅ Decoded message:")
                print(json.dumps(json.loads(decoded), indent=2))
            except Exception as e:
                print(f"❌ Failed to decode: {e}")

    print_cf = PythonOperator(
        task_id="print_cf_message",
        python_callable=_print_cf_message,
    )

    wait_for_cf_message >> print_cf
