import base64
import json
import requests
from google.auth.transport.requests import Request
from google.auth import default

COMPOSER_ENVIRONMENT_URL = "https://<composer-rest-endpoint>"

# Mapping based on (source, destination) bucket pairs
bucket_pair_to_dag_map = {
    ("gdw1", "apmf1"): "gdw_to_apmf_push_event",
    ("apmf1", "gdw1"): "apmf_to_gdw_push_event",
    # Add pull DAGs as well if routing includes destination awareness
    ("gdw1", "apmf1_pull"): "gdw_to_apmf_pull_event",
    ("apmf1", "gdw1_pull"): "apmf_to_gdw_pull_event"
}

def trigger_dag(event, context):
    credentials, _ = default()
    credentials.refresh(Request())

    data = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(data)

    bucket = data.get("bucket")
    name = data.get("name", "")

    # Derive destination or prefix logic (adjustable)
    if bucket == "gdw1":
        destination = "apmf1"
    elif bucket == "apmf1":
        destination = "gdw1"
    else:
        raise Exception(f"No mapping for bucket: {bucket}")

    # Optional: Use suffix "_pull" if implementing pull logic
    pair_key = (bucket, destination)
    dag_id = bucket_pair_to_dag_map.get(pair_key)

    if not dag_id:
        raise Exception(f"No DAG mapped for pair: {pair_key}")

    payload = {
        "conf": {
            "bucket": bucket,
            "name": name
        }
    }

    dag_trigger_url = f"{COMPOSER_ENVIRONMENT_URL}/dags/{dag_id}/dagRuns"

    response = requests.post(
        dag_trigger_url,
        headers={"Authorization": f"Bearer {credentials.token}"},
        json=payload
    )

    if response.status_code != 200:
        raise Exception(f"Failed to trigger DAG: {response.text}")
    return "Triggered"
