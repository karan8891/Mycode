
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagRun
from airflow.utils.state import State
from airflow.utils.session import provide_session
from sqlalchemy import and_

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 15),
}

DAG_ID = "gdw_to_apmf_push_dag"

@provide_session
def clear_queued_dag_runs(session=None, **kwargs):
    queued_runs = session.query(DagRun).filter(
        DagRun.dag_id == DAG_ID,
        DagRun.state == State.QUEUED
    ).all()

    for run in queued_runs:
        session.delete(run)
    session.commit()
    print(f"Deleted {len(queued_runs)} queued DAG runs for '{DAG_ID}'")

with DAG(
    dag_id="cleanup_queued_gdw_to_apmf_push_dag",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["cleanup", "queued", "gdw"],
) as dag:

    cleanup_task = PythonOperator(
        task_id="clear_queued_runs",
        python_callable=clear_queued_dag_runs,
        provide_context=True,
    )
