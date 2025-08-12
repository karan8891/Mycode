import time
from copy import deepcopy
from google.cloud import pubsub_v1


def _normalize_path(*parts: str) -> str:
    return "/".join(s.strip("/") for s in parts if s).strip("/")

def _find_missing_in_dest(sts_body: dict) -> list[str]:
    """Compare includePrefixes against sink; return those not present."""
    spec = sts_body["transferJob"]["transferSpec"]
    dest_bucket = spec["gcsDataSink"]["bucketName"]
    dest_path   = spec["gcsDataSink"].get("path", "")
    prefixes    = spec["objectConditions"].get("includePrefixes", [])

    client = storage.Client()
    bucket = client.bucket(dest_bucket)

    missing: list[str] = []
    for p in prefixes:
        dest_obj = _normalize_path(dest_path, p) if dest_path else p
        if not bucket.blob(dest_obj).exists():
            missing.append(p)
    return missing

def _wait_for_job_completion_sync(subscription_fqn: str, job_name: str,
                                  timeout_sec: int = 30*60, batch: int = 10, sleep_s: int = 20):
    """Block until we see a Pub/Sub notification for the specific job_name."""
    sub = pubsub_v1.SubscriberClient()
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        resp = sub.pull(request={"subscription": subscription_fqn, "max_messages": batch}, timeout=10)
        ack_ids = []
        for rm in resp.received_messages:
            try:
                payload = json.loads(rm.message.data.decode("utf-8"))
                job = payload.get("jobName", "")
                if job.endswith(job_name) or job == job_name:
                    ack_ids.append(rm.ack_id)
            except Exception:
                # ignore junk; don't ack
                pass
        if ack_ids:
            sub.acknowledge(request={"subscription": subscription_fqn, "ack_ids": ack_ids})
            return
        time.sleep(sleep_s)
    raise RuntimeError("Timed out waiting for restarted STS job completion")

def _restart_missing_job_sync(**kwargs):
    """
    After the first STS run finished, check destination for gaps.
    If any of the includePrefixes are missing, submit a new STS job
    with only the missing ones and wait for its completion.
    """
    ti = kwargs["ti"]
    conf = _conf(ti)                       # reuse your existing safe loader
    body = conf["sts_body"]                # original STS body from CF/DAG conf
    completion_sub = conf["completion_subscription"]

    missing = _find_missing_in_dest(body)
    if not missing:
        print("No missing files detected; no restart needed.")
        ti.xcom_push(key="restarted", value=False)
        return

    # clone original body and narrow includePrefixes to the missing set
    new_body = deepcopy(body)
    job = new_body["transferJob"]
    spec = job["transferSpec"]
    if "objectConditions" not in spec:
        spec["objectConditions"] = {}
    spec["objectConditions"]["includePrefixes"] = missing

    # new unique job name
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    job["name"] = f'{conf["job_name"]}_retry_{ts}_{uuid.uuid4().hex[:6]}'

    # ensure notifications are present (FQN required)
    if "notificationConfig" not in job and os.getenv("NOTIFICATION_TOPIC_FQN"):
        job["notificationConfig"] = {
            "pubsubTopic": os.getenv("NOTIFICATION_TOPIC_FQN"),
            "eventTypes": ["TRANSFER_OPERATION_SUCCESS", "TRANSFER_OPERATION_FAILED"],
            "payloadFormat": "JSON",
        }

    # submit via REST (same pattern as your create step)
    url = "https://storagetransfer.googleapis.com/v1/transferJobs"
    headers = {
        "Authorization": f"Bearer {_access_token()}",
        "Content-Type": "application/json",
        "X-Goog-User-Project": conf["project_id"],
    }
    resp = requests.post(url, headers=headers, json=new_body, timeout=60)
    if resp.status_code not in (200, 201, 409):
        raise RuntimeError(f"STS restart failed: {resp.status_code} {resp.text}")

    print(f"Restarted STS for {len(missing)} missing object(s); job: {job['name']}")

    # wait for the restarted job to complete on the same completion subscription
    _wait_for_job_completion_sync(completion_sub, job["name"])

    ti.xcom_push(key="restarted", value=True)
    ti.xcom_push(key="restart_job_name", value=job["name"])



restart_missing_if_needed = PythonOperator(
    task_id="restart_missing_if_needed",
    python_callable=_restart_missing_job_sync,
    provide_context=True,
)

# current chain (append the new task)
create_sts_job >> wait_for_sts_completion >> restart_missing_if_needed >> publish_toc_to_cdmnxt
