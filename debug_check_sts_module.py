
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def check_sts_module_import():
    try:
        from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator
        print("✅ Module is available and import worked.")
    except Exception as e:
        print("❌ Import failed:", e)
        raise

with DAG(
    dag_id="debug_check_sts_module",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["debug", "sts"],
) as dag:

    check_import_task = PythonOperator(
        task_id="check_sts_import",
        python_callable=check_sts_module_import,
    )
