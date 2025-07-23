import base64
import json
import google.auth
import google.auth.transport.requests
import requests

def main(event, context):
    if 'data' not in event:
        raise ValueError("No data field in the Pub/Sub message")

    # Decode Pub/Sub message
    message_data = base64.b64decode(event['data']).decode('utf-8')
    message = json.loads(message_data)

    bucket = message.get("bucket")
    object_name = message.get("object")
    project_id = message.get("project_id")
    delete_after_transfer = message.get("delete_after_transfer", False)

    if not bucket or not object_name or not project_id:
        raise ValueError("Missing required fields in Pub/Sub message")

    # Prepare Composer DAG trigger URL
    dag_url = "https://<COMPOSER-WEBSERVER-URL>/api/experimental/dags/event_sts_transfer_dag/dag_runs"

    dag_conf = {
        "bucket": bucket,
        "object": object_name,
        "project_id": project_id,
        "delete_after_transfer": delete_after_transfer
    }

    # Get ID token for Composer Webserver
    credentials, _ = google.auth.default()
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    id_token = credentials.token

    headers = {
        "Authorization": f"Bearer {id_token}",
        "Content-Type": "application/json"
    }

    response = requests.post(dag_url, headers=headers, json={"conf": dag_conf})

    if response.status_code not in [200, 201]:
        raise RuntimeError(f"Failed to trigger DAG: {response.status_code} {response.text}")

    print("Triggered DAG successfully.")
