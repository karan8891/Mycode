CloudDataTransferServiceCreateJobOperator(
    task_id=f"create_sts_job_{idx}",
    body=PythonOperator(
        python_callable=lambda ti, **_: json.loads(ti.xcom_pull(task_ids='build_transfer_payload', key=f'transfer_job_{idx}')),
        task_id=f'prepare_body_{idx}',
    ),
    project_id=PROJECT_ID,
),
