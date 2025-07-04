import os
from airflow import models
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from pathlib import Path
import random
import string

# Configuration
BUCKET_NAME = "your-target-gcs-bucket"
FILES_TOTAL = 180
FILE_SIZE_MB = 10240  # 10 GB per file
FILE_PREFIX = "huge_files/file_"
TEMP_DIR = "/tmp/gcs_large_files"

Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)

def generate_file_on_disk(file_index, **kwargs):
    file_path = os.path.join(TEMP_DIR, f"file_{file_index:04d}.txt")
    with open(file_path, "w") as f:
        for _ in range(FILE_SIZE_MB // 10):
            content = ''.join(random.choices(string.ascii_letters + string.digits, k=10 * 1024 * 1024))
            f.write(content)
    return file_path

def upload_file_from_disk(file_index, **kwargs):
    file_path = generate_file_on_disk(file_index)
    blob_name = f"{FILE_PREFIX}{file_index:04d}.txt"
    hook = GCSHook()
    hook.upload(bucket_name=BUCKET_NAME, object_name=blob_name, filename=file_path)
    print(f"Uploaded: {blob_name}")
    os.remove(file_path)

with models.DAG(
    dag_id="upload_huge_files_parallel_streamed",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["gcs", "huge-files", "parallel"],
) as dag:
    for i in range(FILES_TOTAL):
        PythonOperator(
            task_id=f"upload_file_{i}",
            python_callable=upload_file_from_disk,
            op_kwargs={"file_index": i},
        )
