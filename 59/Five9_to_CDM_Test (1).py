from airflow import models
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceDeleteJobOperator,
)
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
import json
import uuid

default_args = {
    'start_date': days_ago(1)
}

with models.DAG(
    dag_id='Five9_to_CDM_Test',
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=['sts', 'event-driven'],
) as dag:

    def pull_cf_event(**kwargs):
        message = {
            "project_id": "source-project",
            "source_bucket": "source-bucket",
            "destination_bucket": "destination-bucket",
            "object_prefixes": ["file1.txt", "file2.csv"],
            "delete_after_transfer": True,
            "cdmnxt_topic": "projects/project-id/topics/cdmnxt-trigger-topic"
        }
        kwargs['ti'].xcom_push(key='cf_event', value=message)

    def build_sts_body(**kwargs):
        msg = kwargs['ti'].xcom_pull(key='cf_event', task_ids='pull_cf_event')
        transfer_job = {
            "projectId": msg["project_id"],
            "transferSpec": {
                "gcsDataSource": {
                    "bucketName": msg["source_bucket"]
                },
                "gcsDataSink": {
                    "bucketName": msg["destination_bucket"]
                },
                "objectConditions": {
                    "includePrefixes": msg["object_prefixes"]
                },
                "transferOptions": {
                    "deleteObjectsFromSourceAfterTransfer": msg["delete_after_transfer"]
                }
            },
            "status": "ENABLED",
            "description": "One-time transfer from Five9 to CDM"
        }
        kwargs['ti'].xcom_push(key='sts_body', value=transfer_job)

    def _pull_sts_body_from_xcom(**kwargs):
        return kwargs["ti"].xcom_pull(key='sts_body', task_ids='build_sts_job')

    def _generate_toc(**kwargs):
        msg = kwargs['ti'].xcom_pull(key='cf_event', task_ids='pull_cf_event')
        object_list = msg["object_prefixes"]
        toc = "\n".join(object_list)
        filename = f"toc_{uuid.uuid4().hex}.txt"
        bucket_name = msg["destination_bucket"]

        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(filename)
        blob.upload_from_string(toc)

        kwargs['ti'].xcom_push(key='toc_file', value=filename)

    pull_cf_event = PythonOperator(
        task_id="pull_cf_event",
        python_callable=pull_cf_event
    )

    build_sts_job = PythonOperator(
        task_id="build_sts_job",
        python_callable=build_sts_body
    )

    create_sts_job = CloudDataTransferServiceCreateJobOperator(
        task_id="create_sts_job",
        body=_pull_sts_body_from_xcom
    )

    wait_sts_completion = PubSubPullSensor(
        task_id="wait_sts_completion",
        project_id="destination-project-id",
        subscription="sts-completion-sub",
        timeout=600,
        ack_messages=True
    )

    upload_toc = PythonOperator(
        task_id="upload_toc",
        python_callable=_generate_toc
    )

    notify_cdmnxt = PubSubPublishMessageOperator(
        task_id="notify_cdmnxt",
        project_id="destination-project-id",
        topic="cdmnxt-trigger-topic",
        messages=[{"data": "{{ ti.xcom_pull(task_ids='upload_toc', key='toc_file') }}"}]
    )

    delete_sts_job = CloudDataTransferServiceDeleteJobOperator(
        task_id="delete_sts_job",
        job_name="{{ ti.xcom_pull(task_ids='create_sts_job') }}",
        project_id="source-project"
    )

    (
        pull_cf_event
        >> build_sts_job
        >> create_sts_job
        >> wait_sts_completion
        >> upload_toc
        >> notify_cdmnxt
        >> delete_sts_job
    )
