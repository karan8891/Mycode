from airflow import models
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Configuration
BUCKET_NAME = "your-target-gcs-bucket"
PARTS_PREFIX = "parts/part_"
FINAL_OBJECT_NAME = "concatenated/huge_1_8tb_file.txt"
INTERMEDIATE_PREFIX = "concatenated/intermediate_"

with models.DAG(
    dag_id="compose_256_parts_to_gcs",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["gcs", "compose", "1.8tb", "256parts"],
) as dag:

    intermediate_tasks = []
    for j in range(8):  # 256 / 32 = 8 intermediate groups
        part_start = j * 32
        part_end = part_start + 32
        task = BashOperator(
            task_id=f"compose_group_{j}",
            bash_command=(
                "gsutil compose " +
                " ".join([f"gs://{BUCKET_NAME}/{PARTS_PREFIX}{i:03d}.txt" for i in range(part_start, part_end)]) +
                f" gs://{BUCKET_NAME}/{INTERMEDIATE_PREFIX}{j:02d}.txt"
            )
        )
        intermediate_tasks.append(task)

    # Final compose of all intermediate objects
    compose_final = BashOperator(
        task_id="compose_final_256",
        bash_command=(
            "gsutil compose " +
            " ".join([f"gs://{BUCKET_NAME}/{INTERMEDIATE_PREFIX}{j:02d}.txt" for j in range(8)]) +
            f" gs://{BUCKET_NAME}/{FINAL_OBJECT_NAME}"
        )
    )

    for task in intermediate_tasks:
        task >> compose_final
