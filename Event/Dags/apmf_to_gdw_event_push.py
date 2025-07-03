from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator

with models.DAG(
    dag_id="apmf_to_gdw_event_push",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sts", "event", "push"],
) as dag:

    trigger_sts_job = CloudDataTransferServiceCreateJobOperator(
        task_id="trigger_sts_job",
        body={
            "description": "Event-driven STS job",
            "status": "ENABLED",
            "projectId": "apmf-sandbox-project",
            "transferSpec": {
                "gcsDataSource": {
                    "bucketName": "apmf-source-bucket"
                },
                "gcsDataSink": {
                    "bucketName": "gdw-destination-bucket"
                },
                "transferOptions": {
                    "overwriteObjectsAlreadyExistingInSink": True
                }
            },
            "notificationConfig": {
                "pubsubTopic": "projects/apmf-sandbox-project/topics/sts-job-complete",
                "eventTypes": ["TRANSFER_OPERATION_SUCCESS"],
                "payloadFormat": "JSON"
            }
        },
    )
