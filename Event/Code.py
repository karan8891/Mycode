job_id  = re.sub(r'[^A-Za-z0-9_-]', '', f"event_sts_job_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}")[:128]
body["transferJob"]["name"] = f"transferJobs/{job_id}"
