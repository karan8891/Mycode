def create_update_run_tasks(task_group_name, idx):
    def _get_job_body(ti, **_):
        return json.loads(
            ti.xcom_pull(task_ids='build_transfer_payload', key=f'transfer_job_{idx}')
        )

    create_body_task = PythonOperator(
        task_id=f'prepare_body_{idx}',
        python_callable=_get_job_body,
    )

    create_job = CloudDataTransferServiceCreateJobOperator(
        task_id=f"create_sts_job_{idx}",
        body="{{ task_instance.xcom_pull(task_ids='prepare_body_" + str(idx) + "') }}",
        project_id=PROJECT_ID,
    )

    run_job = CloudDataTransferServiceUpdateJobOperator(
        task_id=f"run_sts_job_{idx}",
        job_name="{{ task_instance.xcom_pull(task_ids='create_sts_job_" + str(idx) + "')['name'] }}",
        body="{}",
        project_id=PROJECT_ID,
    )

    wait_completion = PubSubPullSensor(
        task_id=f"wait_for_completion_{idx}",
        project_id=PROJECT_ID,
        subscription=COMPLETION_SUB,
        ack_messages=True,
        max_messages=5,
        timeout=600,
    )

    delete_job = CloudDataTransferServiceDeleteJobOperator(
        task_id=f"delete_sts_job_{idx}",
        job_name="{{ task_instance.xcom_pull(task_ids='create_sts_job_" + str(idx) + "')['name'] }}",
        project_id=PROJECT_ID,
    )

    return [create_body_task, create_job, run_job, wait_completion, delete_job]
