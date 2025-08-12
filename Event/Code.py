
# Use the server job name if available; fall back to the one we set (on 409)
if resp.status_code in (200, 201):
    job_name = resp.json().get("name")              # e.g. "transferJobs/1234567890"
else:  # 409 already exists
    job_name = body["transferJob"]["name"]

run_url  = f"https://storagetransfer.googleapis.com/v1/{job_name}:run"
run_body = {"projectId": project_id}

run_resp = requests.post(run_url, headers=headers, json=run_body, timeout=60)
if run_resp.status_code != 200:
    raise RuntimeError(f"STS run failed: {run_resp.status_code} {run_resp.text}")
