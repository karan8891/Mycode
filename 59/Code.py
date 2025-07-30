def _get_job_body(ti, **_):
    return ti.xcom_pull(task_ids=f'build_transfer_payload', key=f'transfer_job_{idx}')

create_body_task = PythonOperator(
    task_id=f'prepare_body_{idx}',
    python_callable=_get_job_body,
)

create_job = CloudDataTransferServiceCreateJobOperator(
    task_id=f'create_sts_job_{idx}',
    body=XComArg(create_body_task),
    project_id=PROJECT_ID,
)
