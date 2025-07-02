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
    "push": "dag_id_push_transfer",
    "pull": "dag_id_pull_transfer",
}

def trigger_dag(dag_id):
    url = (
        f"https://composer.googleapis.com/v1/projects/{PROJECT_ID}/locations/{COMPOSER_LOCATION}"
        f"/environments/{COMPOSER_ENV}/dagExecutions"
    )
    token = id_token.fetch_id_token(Request(), url)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {"dagId": dag_id}
    response = requests.post(url, headers=headers, json=payload)
    print(f"Triggered DAG {dag_id}, response: {response.text}")
    return response.status_code

def determine_type(data):
    job_name = data.get("name", "")
    if "push" in job_name.lower():
        return "push"
    elif "pull" in job_name.lower():
        return "pull"
    else:
        return None

def main(event, context):
    if "data" not in event:
        print("No data in Pub/Sub message")
        return
    payload = base64.b64decode(event["data"]).decode("utf-8")
    data = json.loads(payload)
    print("Received STS event:", json.dumps(data, indent=2))
    job_type = determine_type(data)
    if job_type not in DAG_MAPPING:
        print("No matching DAG for job type")
        return
    dag_id = DAG_MAPPING[job_type]
    trigger_dag(dag_id)
