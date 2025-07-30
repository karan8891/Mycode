def create_update_run_tasks(task_group_name, idx):
    def _get_job_body(ti, **kwargs):
        return ti.xcom_pull(
            task_ids='build_transfer_payload',
            key=f'transfer_job_{idx}'
        )

    def _get_job_name(ti, **kwargs):
        job = ti.xcom_pull(task_ids=f"create_sts_job_{idx}")
        return job["name"]

    create_body_task = PythonOperator(
        task_id=f"prepare_body_{idx}",
        python_callable=_get_job_body,
    )

    create_job = CloudDataTransferServiceCreateJobOperator(
        task_id=f"create_sts_job_{idx}",
        body=_get_job_body(ti=None),  # We'll patch it below
        project_id=PROJECT_ID,
    )

    # Patch `body` using `provide_context=True`
    def patch_create_job_body(**kwargs):
        return _get_job_body(kwargs['ti'])

    create_job.body = patch_create_job_body

    update_job = CloudDataTransferServiceUpdateJobOperator(
        task_id=f"run_sts_job_{idx}",
        job_name=_get_job_name,  # Callable, no Jinja
        body={},
        project_id=PROJECT_ID,
    )

    wait_task = PubSubPullSensor(
        task_id=f"wait_for_completion_{idx}",
        project_id=PROJECT_ID,
        subscription=COMPLETION_SUB,
        ack_messages=True,
        max_messages=5,
        timeout=600,
    )

    delete_task = CloudDataTransferServiceDeleteJobOperator(
        task_id=f"delete_sts_job_{idx}",
        job_name=_get_job_name,  # Again, no Jinja
        project_id=PROJECT_ID,
    )

    return [
        create_body_task >> create_job >> update_job >> wait_task >> delete_task
    ]
