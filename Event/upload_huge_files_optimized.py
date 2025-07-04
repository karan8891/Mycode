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
BUCKET_NAME = "your-gcs-bucket-name"
FILES_TOTAL = 2        # Safer for testing
FILE_SIZE_GB = 5       # Smaller file size (max 10GB safe in Composer)
CHUNK_MB = 100
FILE_PREFIX = "reliable_upload/files"
TEMP_DIR = Path("/tmp/reliable_upload_files")
TEMP_DIR.mkdir(parents=True, exist_ok=True)

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
    file_path = None
    try:
        hook = GCSHook()
        file_path = generate_file_streaming(file_index)
        object_name = f"{FILE_PREFIX}{file_index:04d}.txt"
        hook.upload(
            bucket_name=BUCKET_NAME,
            object_name=object_name,
            filename=file_path,
            resumable=True
        )
        print(f"✅ Uploaded: {object_name}")
    except Exception as e:
        print(f"❌ Failed to upload file {file_index}: {e}")
        raise
    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)

with models.DAG(
    dag_id="upload_huge_files_optimized",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["gcs", "huge-files", "reliable"],
) as dag:
    for i in range(FILES_TOTAL):
        PythonOperator(
            task_id=f"upload_huge_file_{i}",
            python_callable=upload_huge_file_to_gcs,
            op_kwargs={"file_index": i},
            execution_timeout=timedelta(hours=2),
        )
