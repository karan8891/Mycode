import functions_framework
import requests
import google.auth
import google.auth.transport.requests
import os
import json
import base64

@functions_framework.cloud_event
def main(cloud_event):
    # Step 1: Get env vars
    dag_id = "apmf_to_gdw_pull_dag"  # Change if needed
    composer_env_url = os.environ.get("COMPOSER_WEBSERVER_URL")
    project_id = os.environ.get("PROJECT_ID", "")

    if not composer_env_url:
        print("COMPOSER_WEBSERVER_URL env variable not found.")
        return

    # Step 2: Extract GCS bucket + file name from Pub/Sub message
    try:
        pubsub_message = cloud_event.data["message"]["data"]
        decoded_data = base64.b64decode(pubsub_message).decode("utf-8")
        event_data = json.loads(decoded_data)
    except Exception as e:
        print(f"Failed to decode or parse message: {e}")
        return

    bucket = event_data.get("bucket")
    name = event_data.get("name")

    if not name:
        print("No object name in event.")
        return

    print(f"📦 Received file '{name}' in bucket '{bucket}' -> triggering DAG: {dag_id}")

    # Step 3: Prepare Airflow trigger endpoint
    endpoint = f"{composer_env_url}/api/v1/dags/{dag_id}/dagRuns"

    # Step 4: Auth token
    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    token = credentials.token

    # Step 5: Trigger DAG
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    payload = {
        "conf": {
            "bucket": bucket,
            "name": name
        }
    }

    response = requests.post(endpoint, headers=headers, json=payload)

    if response.status_code == 200:
        print(f"✅ DAG '{dag_id}' triggered successfully.")
    else:
        print(f"❌ Failed to trigger DAG: {response.status_code}")
        print(response.text)
