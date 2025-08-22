from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

def check_condition(**kwargs):
    # Your custom logic
    condition_met = True  # Replace with real check
    if condition_met:
        return "task2"
    else:
        return "task3"

with DAG(
    "conditional_branching_dag",
    start_date=datetime(2025, 8, 22),
    schedule_interval=None,
    catchup=False
) as dag:

    task1 = BranchPythonOperator(
        task_id="task1_check_condition",
        python_callable=check_condition,
        provide_context=True
    )

    task2 = EmptyOperator(task_id="task2")
    task3 = EmptyOperator(task_id="task3")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    # Set branching dependencies
    task1 >> task2 >> end
    task1 >> task3 >> end
