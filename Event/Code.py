def _restart_missing_job_sync(**kwargs):
    """
    After the first STS run finished, check destination for gaps.
    If any includePrefixes are missing, submit a new STS job with only the
    missing ones and wait for its completion.
    """
    ti = kwargs["ti"]
    conf = _conf(ti)
    base_body = conf["sts_body"]
    completion_sub = conf["completion_subscription"]

    # find what didn't land
    missing = _find_missing_in_dest(base_body)
    if not missing:
        print("No missing files detected; no restart needed.")
        ti.xcom_push(key="restarted", value=False)
        return

    # clone and narrow includePrefixes
    from copy import deepcopy
    new_body = deepcopy(base_body)
    job = new_body["transferJob"]
    spec = job["transferSpec"]
    if "objectConditions" not in spec:
        spec["objectConditions"] = {}
    spec["objectConditions"]["includePrefixes"] = missing

    # ---- valid STS resource name: transferJobs/<id> ----
    import re, uuid
    from datetime import datetime, timedelta

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    base = f"{conf['job_name']}_retry_{ts}_{uuid.uuid4().hex[:6]}"
    job_id = re.sub(r"[^A-Za-z0-9\-_]", "-", base)[:128]
    job["name"] = f"transferJobs/{job_id}"

    # ensure notifications (FQN)
    import os
    if "notificationConfig" not in job and os.getenv("NOTIFICATION_TOPIC_FQN"):
        job["notificationConfig"] = {
            "pubsubTopic": os.getenv("NOTIFICATION_TOPIC_FQN"),
            "eventTypes": ["TRANSFER_OPERATION_SUCCESS", "TRANSFER_OPERATION_FAILED"],
            "payloadFormat": "JSON",
        }

    # make sure there is an open schedule window (starts ~now)
    start = datetime.utcnow() - timedelta(minutes=2)
    end = start + timedelta(days=1)
    job["schedule"] = {
        "scheduleStartDate": {"year": start.year, "month": start.month, "day": start.day},
        "startTimeOfDay": {"hours": start.hour, "minutes": start.minute},
        "scheduleEndDate": {"year": end.year, "month": end.month, "day": end.day},
    }

    # create the restart job
    url = "https://storagetransfer.googleapis.com/v1/transferJobs"
    headers = {
        "Authorization": f"Bearer {_access_token()}",
        "Content-Type": "application/json",
        "X-Goog-User-Project": conf["project_id"],
    }
    import requests, json
    resp = requests.post(url, headers=headers, json=new_body["transferJob"], timeout=60)
    if resp.status_code not in (200, 201, 409):  # 409 = already exists (idempotent)
        raise RuntimeError(f"STS restart failed: {resp.status_code} {resp.text}")

    # kick it off immediately (important for on-demand)
    run_resp = requests.post(
        "https://storagetransfer.googleapis.com/v1/transferJobs:run",
        headers=headers,
        json={"projectId": conf["project_id"], "jobName": job["name"]},
        timeout=60,
    )
    if run_resp.status_code not in (200, 204):
        raise RuntimeError(f"STS restart run failed: {run_resp.status_code} {run_resp.text}")

    print(f"Restarted STS for {len(missing)} missing object(s); job: {job['name']}")

    # wait for completion of the restarted job
    _wait_for_job_completion_sync(completion_sub, job["name"])

    ti.xcom_push(key="restarted", value=True)
    ti.xcom_push(key="restart_job_name", value=job["name"])
