# This stays inside create_update_run_tasks()
def patch_create_job_body(**kwargs):  # DO NOT CALL WITH ti=None
    ti = kwargs["ti"]
    return ti.xcom_pull(
        task_ids='build_transfer_payload',
        key=f"transfer_job_{idx}"
    )
create_job = CloudDataTransferServiceCreateJobOperator(
    task_id=f"create_sts_job_{idx}",
    body=patch_create_job_body,  # ← PASS THE FUNCTION, don't call it
    project_id=PROJECT_ID,
    provide_context=True,        # ← REQUIRED to pass `ti`
)
