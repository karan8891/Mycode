import re, uuid
from datetime import datetime

job_name = f"event_sts_job_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}"
job_name = re.sub(r'[^a-zA-Z0-9_-]', '_', job_name)[:128]
body["transferJob"]["name"] = job_name
