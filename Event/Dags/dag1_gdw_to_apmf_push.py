import base64
import json
import os
import requests
from google.auth.transport.requests import Request
from google.oauth2 import id_token

COMPOSER_ENV = os.getenv("COMPOSER_ENV")
COMPOSER_LOCATION = os.getenv("COMPOSER_LOCATION")
PROJECT_ID = os.getenv("PROJECT_ID")

DAG_MAPPING = {
    "gdw1_sandbox-corp-gdw-sfr-cdb8": "gdw_to_apmf_push_dag",
    "apmf1_sandbox-corp-apmf-oaep-d3f4": "gdw_to_apmf_pull_dag"
}

def trigger_dag(dag_id):
    url = (
        f"https://composer.googleapis.com/v1/projects/{PROJECT_ID}/locations/"
        f"{COMPOSER_LOCATION}/environments/{COMPOSER_ENV}/dagExecutions"
    )

    token = id_token.fetch_id_token(Request(), url)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {"dagId": dag_id}
    response = requests.post(url, headers=headers, json=payload)
    print(f"Triggered DAG '{dag_id}', response: {response.text}")
    return response.status_code

def main(event, context):
    if "data" not in event:
        print("No data in Pub/Sub message")
        return

    payload = base64.b64decode(event["data"]).decode("utf-8")
    data = json.loads(payload)

    bucket_name = data.get("bucket", "").lower()
    print(f"Received GCS bucket: {bucket_name}")

    dag_id = DAG_MAPPING.get(bucket_name)
    if not dag_id:
        print("No matching DAG for bucket")
        return

    trigger_dag(dag_id)
