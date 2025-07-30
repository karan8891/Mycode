from airflow import models
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceUpdateJobOperator,
    CloudDataTransferServiceDeleteJobOperator,
)
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor

PROJECT_ID = "your-gcp-project-id"
COMPLETION_SUB = "your-completion-sub"

def create_update_run_tasks(task_group_name, idx):
    def _get_job_body(ti, **kwargs):
        return ti.xcom_pull(
            task_ids='build_transfer_payload',
            key=f'transfer_job_{idx}'
        )

    def _get_job_name(ti, **kwargs):
        job = ti.xcom_pull(task_ids=f'create_sts_job_{idx}')
        return job["name"]

    create_body_task = PythonOperator(
        task_id=f"prepare_body_{idx}",
        python_callable=_get_job_body,
    )

    create_job = CloudDataTransferServiceCreateJobOperator(
        task_id=f"create_sts_job_{idx}",
        body=_get_job_body,
        project_id=PROJECT_ID,
    )

    update_job = CloudDataTransferServiceUpdateJobOperator(
        task_id=f"run_sts_job_{idx}",
        job_name=_get_job_name,
        body={},
        project_id=PROJECT_ID,
    )

    wait_task = PubSubPullSensor(
        task_id=f"wait_for_completion_{idx}",
        project_id=PROJECT_ID,
        subscription=COMPLETION_SUB,
        ack_messages=True,
        max_messages=5,
        timeout=600,
    )

    delete_task = CloudDataTransferServiceDeleteJobOperator(
        task_id=f"delete_sts_job_{idx}",
        job_name=_get_job_name,
        project_id=PROJECT_ID,
    )

    return create_body_task >> create_job >> update_job >> wait_task >> delete_task