from airflow import models
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import random
import string

# Configuration
BUCKET_NAME = "your-target-gcs-bucket"
FILES_TOTAL = 180  # ~10GB per file = ~1.8TB total
FILE_SIZE_MB = 10240  # 10 GB per file
FILE_PREFIX = "huge_files/file_"

def generate_random_content(size_mb=10):
    chars = string.ascii_letters + string.digits
    size = size_mb * 1024 * 1024
    return ''.join(random.choices(chars, k=size))

def upload_large_files_to_gcs(**kwargs):
    hook = GCSHook()
    for i in range(FILES_TOTAL):
        file_name = f"{FILE_PREFIX}{i:04d}.txt"
        content_chunks = []
        for _ in range(FILE_SIZE_MB // 10):  # Build file in 10MB chunks
            content_chunks.append(generate_random_content(10))
        content = ''.join(content_chunks)
        hook.upload(bucket_name=BUCKET_NAME, object_name=file_name, data=content)
        print(f"Uploaded: {file_name}")

with models.DAG(
    dag_id="upload_huge_files_under_2tb",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["gcs", "huge-files", "test-upload"],
) as dag:
    upload_task = PythonOperator(
        task_id="upload_huge_files",
        python_callable=upload_large_files_to_gcs,
    )
