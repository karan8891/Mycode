from datetime import datetime
import uuid

job_name = f"event-sts-job-{datetime.utcnow().strftime('%Y%m%dT%H%M%S%f')}-{uuid.uuid4().hex[:8]}"
body["transferJob"]["name"] = job_name
