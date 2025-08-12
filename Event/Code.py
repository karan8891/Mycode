# --- right after successful create 
run_url = "https://storagetransfer.googleapis.com/v1/transferJobs:run"
run_body = {
    "projectId": project_id,
    "jobName": body["transferJob"]["name"],  # "transferJobs/<id>"
}
run_resp = requests.post(run_url, headers=headers, json=run_body, timeout=60)
if run_resp.status_code not in (200, 204):
    raise RuntimeError(f"STS run failed: {run_resp.status_code} {run_resp.text}")
