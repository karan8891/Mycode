from airflow import DAG from airflow.operators.python import PythonOperator from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceCreateJobOperator from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator from airflow.utils.dates import days_ago from google.cloud import storage import json import base64

def pull_cf_message(**kwargs): from google.cloud import pubsub_v1

subscription_path = kwargs["params"]["cf_subscription"]
project_id = subscription_path.split("/")[1]
sub_id = subscription_path.split("/")[-1]

subscriber = pubsub_v1.SubscriberClient()
response = subscriber.pull(
    request={
        "subscription": subscription_path,
        "max_messages": 1,
    }
)

ack_ids = []
for received_message in response.received_messages:
    msg = base64.b64decode(received_message.message.data).decode("utf-8")
    ack_ids.append(received_message.ack_id)
    kwargs["ti"].xcom_push(key="cf_message", value=json.loads(msg))

if ack_ids:
    subscriber.acknowledge(
        request={"subscription": subscription_path, "ack_ids": ack_ids}
    )

def extract_fields(**kwargs): msg = kwargs["ti"].xcom_pull(task_ids="pull_cf_message", key="cf_message")

sts_body = msg.get("sts_body")
delete_after_transfer = msg.get("delete_after_transfer")
completion_subscription = msg.get("completion_subscription")
cdmnxt_topic = msg.get("cdmnxt_topic")
project_id = msg.get("project_id")

if not sts_body or not completion_subscription or not cdmnxt_topic:
    raise ValueError("Missing required fields in CF message")

# Extract .toc filename
toc_filename = sts_body["transferJob"]["transferSpec"]["gcsDataSink"]["path"].rstrip("/") + ".toc"
bucket_name = sts_body["transferJob"]["transferSpec"]["gcsDataSink"]["bucketName"]

kwargs["ti"].xcom_push(key="sts_body", value=sts_body)
kwargs["ti"].xcom_push(key="delete_after_transfer", value=delete_after_transfer)
kwargs["ti"].xcom_push(key="completion_subscription", value=completion_subscription)
kwargs["ti"].xcom_push(key="cdmnxt_topic", value=cdmnxt_topic)
kwargs["ti"].xcom_push(key="project_id", value=project_id)
kwargs["ti"].xcom_push(key="toc_file", value=toc_filename)
kwargs["ti"].xcom_push(key="bucket_name", value=bucket_name)

def get_sts_body(**kwargs): return kwargs["ti"].xcom_pull(task_ids="extract_fields", key="sts_body")

def get_completion_subscription(**kwargs): return kwargs["ti"].xcom_pull(task_ids="extract_fields", key="completion_subscription")

def get_toc_json(**kwargs): bucket = kwargs["ti"].xcom_pull(task_ids="extract_fields", key="bucket_name") toc_file = kwargs["ti"].xcom_pull(task_ids="extract_fields", key="toc_file")

client = storage.Client()
content = client.bucket(bucket).blob(toc_file).download_as_text()
return json.loads(content)

def get_cdmnxt_topic(**kwargs): return kwargs["ti"].xcom_pull(task_ids="extract_fields", key="cdmnxt_topic")

def get_project_id(**kwargs): return kwargs["ti"].xcom_pull(task_ids="extract_fields", key="project_id")

def get_job_name(**kwargs): body = kwargs["ti"].xcom_pull(task_ids="extract_fields", key="sts_body") return body.get("transferJob", {}).get("name")

def log_to_bq_placeholder(**kwargs): # Placeholder for logging STS results to BigQuery # from google.cloud import bigquery # client = bigquery.Client() # table_id = "your-project.dataset.table" # row = {...} # errors = client.insert_rows_json(table_id, [row]) # if errors: #     raise Exception(f"BQ log insert failed: {errors}") print("[INFO] STS result would be logged to BigQuery here")

def notify_cdmnxt(**kwargs): topic = get_cdmnxt_topic(**kwargs) project = get_project_id(**kwargs) message = get_toc_json(**kwargs)

from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project, topic.split("/")[-1])
publisher.publish(topic_path, json.dumps(message).encode("utf-8"))

with DAG( dag_id="gdw_sts_event_final", default_args={"owner": "airflow"}, start_date=days_ago(1), schedule_interval=None, catchup=False, max_active_runs=1, ) as dag:

pull_cf = PythonOperator(
    task_id="pull_cf_message",
    python_callable=pull_cf_message,
    provide_context=True,
    params={"cf_subscription": "projects/sandbox-corp-gdw-sfr-cdts/subscriptions/cf-trigger-dag"},
)

extract = PythonOperator(
    task_id="extract_fields",
    python_callable=extract_fields,
    provide_context=True,
)

create_sts_job = CloudDataTransferServiceCreateJobOperator(
    task_id="create_sts_job",
    body=get_sts_body,
    project_id=get_project_id,
)

wait_for_completion = PubSubPullSensor(
    task_id="wait_for_completion",
    project_id=get_project_id,
    subscription=get_completion_subscription,
    ack_messages=True,
    messages_callback=lambda msgs: print(f"Received: {msgs}"),
    timeout=600,
    poke_interval=30,
)

notify = PythonOperator(
    task_id="notify_cdmnxt",
    python_callable=notify_cdmnxt,
    provide_context=True,
)

# delete_job = CloudDataTransferServiceDeleteJobOperator(
#     task_id="delete_sts_job",
#     job_name=get_job_name,
#     project_id=get_project_id,
# )

# log_result = PythonOperator(
#     task_id="log_to_bq",
#     python_callable=log_to_bq_placeholder,
#     provide_context=True,
# )

pull_cf >> extract >> create_sts_job >> wait_for_completion >> notify
# wait_for_completion >> delete_job >> log_result

