import base64
import json
import requests
from google.auth.transport.requests import Request
from google.auth import default

COMPOSER_ENVIRONMENT_URL = "https://<composer-rest-endpoint>"
DAG_ID = "your_dag_id"

def trigger_dag(data, context):
    credentials, _ = default()
    credentials.refresh(Request())

    payload = {
        "conf": {
            "bucket": data.get("bucket"),
            "name": data.get("name")
        }
    }

    dag_trigger_url = f"{COMPOSER_ENVIRONMENT_URL}/dags/{DAG_ID}/dagRuns"

    response = requests.post(
        dag_trigger_url,
        headers={"Authorization": f"Bearer {credentials.token}"},
        json=payload
    )

    if response.status_code != 200:
        raise Exception(f"Failed to trigger DAG: {response.text}")
    return "Triggered"
