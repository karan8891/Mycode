from airflow import models
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import random
import string

# Configuration
BUCKET_NAME = "your-target-gcs-bucket"
FILES_TOTAL = 50000
BATCH_SIZE = 5000  # Process in chunks
FILE_PREFIX = "dummy_data/file_"

def generate_random_content(size_kb=25):
    chars = string.ascii_letters + string.digits
    size = size_kb * 1024
    return ''.join(random.choices(chars, k=size))

def upload_files_to_gcs(batch_start, batch_end, **kwargs):
    hook = GCSHook()
    for i in range(batch_start, batch_end):
        file_name = f"{FILE_PREFIX}{i:05d}.txt"
        content = generate_random_content(random.randint(20, 30))
        hook.upload(bucket_name=BUCKET_NAME, object_name=file_name, data=content)
        if i % 100 == 0:
            print(f"Uploaded file {i}")

with models.DAG(
    dag_id="create_50000_dummy_files_to_gcs",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["gcs", "testdata", "bulk-upload"],
) as dag:

    for start in range(0, FILES_TOTAL, BATCH_SIZE):
        end = min(start + BATCH_SIZE, FILES_TOTAL)
        PythonOperator(
            task_id=f"upload_files_{start}_{end}",
            python_callable=upload_files_to_gcs,
            op_kwargs={"batch_start": start, "batch_end": end},
        )
