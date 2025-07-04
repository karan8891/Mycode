import os
from airflow import models
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from pathlib import Path
import random
import string
from datetime import timedelta

# Configuration
BUCKET_NAME = "your-target-gcs-bucket"
FILES_TOTAL = 4
FILE_SIZE_GB = 10  # Adjust per file size
CHUNK_MB = 100     # Write in 100MB chunks
FILE_PREFIX = "reliable_upload/file_"
TEMP_DIR = "/tmp/reliable_upload_files"

Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)

def generate_chunk(size_mb=100):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size_mb * 1024 * 1024))

def generate_file_streaming(file_index):
    file_path = os.path.join(TEMP_DIR, f"file_{file_index:04d}.txt")
    total_chunks = (FILE_SIZE_GB * 1024) // CHUNK_MB
    with open(file_path, "w") as f:
        for _ in range(total_chunks):
            f.write(generate_chunk(CHUNK_MB))
    return file_path

def upload_huge_file_to_gcs(file_index, **kwargs):
    hook = GCSHook()
    file_path = generate_file_streaming(file_index)
    object_name = f"{FILE_PREFIX}{file_index:04d}.txt"
    hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=object_name,
        filename=file_path,
        resumable=True
    )
    print(f"Uploaded: {object_name}")
    os.remove(file_path)

with models.DAG(
    dag_id="upload_huge_files_reliable",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["gcs", "huge-files", "resumable"],
) as dag:
    for i in range(FILES_TOTAL):
        PythonOperator(
            task_id=f"upload_huge_file_{i}",
            python_callable=upload_huge_file_to_gcs,
            op_kwargs={"file_index": i},
            execution_timeout=timedelta(hours=3),
        )
