
from datetime import datetime
from airflow import DAG

try:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
    dummy_status = "AwsBaseHook loaded from plugin"
except Exception as e:
    dummy_status = f"Error loading AwsBaseHook: {e}"

def print_status():
    print(dummy_status)

with DAG(
    dag_id="test_dummy_aws_hook_plugin",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    schedule_interval=None,
) as dag:
    from airflow.operators.python import PythonOperator

    test_task = PythonOperator(
        task_id="print_plugin_status",
        python_callable=print_status
    )
