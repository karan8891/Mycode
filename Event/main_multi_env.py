import base64
import json
import requests
from google.auth.transport.requests import Request
from google.auth import default

# Mapping of (source_bucket, destination_bucket) to DAG ID and Composer Environment URL
bucket_pair_to_config = {
    ("gdw1", "apmf1"): {
        "dag_id": "gdw_to_apmf_push_event",
        "composer_url": "https://composer.googleapis.com/v1/projects/gdw-project/locations/us-central1/environments/gdw-composer"
    },
    ("apmf1", "gdw1"): {
        "dag_id": "apmf_to_gdw_push_event",
        "composer_url": "https://composer.googleapis.com/v1/projects/apmf-project/locations/us-central1/environments/apmf-composer"
    },
    ("gdw1", "apmf1_pull"): {
        "dag_id": "gdw_to_apmf_pull_event",
        "composer_url": "https://composer.googleapis.com/v1/projects/apmf-project/locations/us-central1/environments/apmf-composer"
    },
    ("apmf1", "gdw1_pull"): {
        "dag_id": "apmf_to_gdw_pull_event",
        "composer_url": "https://composer.googleapis.com/v1/projects/gdw-project/locations/us-central1/environments/gdw-composer"
    }
}

def trigger_dag(event, context):
    credentials, _ = default()
    credentials.refresh(Request())

    data = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(data)

    bucket = data.get("bucket")
    name = data.get("name", "")

    # Determine destination based on source bucket
    if bucket == "gdw1":
        destination = "apmf1"
    elif bucket == "apmf1":
        destination = "gdw1"
    else:
        raise Exception(f"Unknown source bucket: {bucket}")

    # Determine if it's a pull or push (you may enhance with more logic)
    if name.startswith("pull_trigger/"):
        destination += "_pull"

    pair_key = (bucket, destination)
    config = bucket_pair_to_config.get(pair_key)

    if not config:
        raise Exception(f"No config found for {pair_key}")

    dag_id = config["dag_id"]
    composer_url = config["composer_url"]
    dag_trigger_url = f"{composer_url}/dags/{dag_id}/dagRuns"

    payload = {
        "conf": {
            "bucket": bucket,
            "name": name
        }
    }

    response = requests.post(
        dag_trigger_url,
        headers={"Authorization": f"Bearer {credentials.token}"},
        json=payload
    )

    if response.status_code != 200:
        raise Exception(f"Failed to trigger DAG: {response.text}")
    return "Triggered"
