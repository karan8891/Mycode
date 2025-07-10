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
    "apmf_to_gdw_event_push": "apmf_to_gdw_event_push",
    "apmf_to_gdw_event_pull": "apmf_to_gdw_event_pull",
    "dag1_gdw_to_apmf_push_dag": "gdw_to_apmf_push_dag",
    "dag3_gdw_to_apmf_pull_dag": "gdw_to_apmf_pull_dag"
}

def trigger_dag(dag_id):
    url = f"https://composer.googleapis.com/v1/projects/{PROJECT_ID}/locations/{COMPOSER_LOCATION}/environments/{COMPOSER_ENV}/dagExecutions"

    token = id_token.fetch_id_token(Request(), url)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {"dagId": dag_id}
    response = requests.post(url, headers=headers, json=payload)
    print(f"Triggered DAG {dag_id}, response: {response.text}")
    return response.status_code

def determine_type(data):
    job_name = data.get("name", "")
    for key in DAG_MAPPING:
        if key in job_name.lower():
            return DAG_MAPPING[key]
    return None

def main(event, context):
    if "data" not in event:
        print("No data in Pub/Sub message")
        return

    payload = base64.b64decode(event["data"]).decode("utf-8")
    data = json.loads(payload)

    print("Received STS event:", json.dumps(data, indent=2))
    dag_id = determine_type(data)
    if not dag_id:
        print("No matching DAG for job name")
        return

    trigger_dag(dag_id)
