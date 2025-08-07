import os
import json
import base64
import uuid
from datetime import datetime
from google.cloud import pubsub_v1

# ENVIRONMENT VARIABLES
DEST_PROJECT_ID = os.getenv("DEST_PROJECT_ID")
DEST_BUCKET = os.getenv("DEST_BUCKET")
DEST_PATH = os.getenv("DEST_PATH")
SOURCE_PROJECT_ID = os.getenv("SOURCE_PROJECT_ID")
SOURCE_BUCKET = os.getenv("SOURCE_BUCKET")
NOTIFICATION_PROJECT_ID = os.getenv("NOTIFICATION_PROJECT_ID")
NOTIFICATION_TOPIC = os.getenv("NOTIFICATION_TOPIC")
STS_COMPLETION_TOPIC = os.getenv("STS_COMPLETION_TOPIC")
DAG_TRIGGER_TOPIC = os.getenv("DAG_TRIGGER_TOPIC")

publisher = pubsub_v1.PublisherClient()

# REQUIRED FIELDS FOR TOC VALIDATION
REQUIRED_TOC_KEYS = [
    "cdm_process_id",
    "app_id",
    "drt_id",
    "data_pipeline_type",
    "data_movement_pattern_type",
    "cdm_object_mapping",
    "type",
    "businessProcessingDateTime",
    "totalNumberOfRecords",
    "numberOfColumns"
]

REQUIRED_MAPPING_KEYS = [
    "object_name",
    "cdm_object_format",
    "cdm_landing_type",
    "cdm_landing_path",
    "cdm_target_type",
    "cdm_target_path",
    "cdm_object_identifier",
    "write_disposition",
    "dataobject"
]

def validate_toc(toc: dict):
    # Top-level fields
    for key in REQUIRED_TOC_KEYS:
        if key not in toc:
            return False, f"Missing top-level TOC key: {key}"

    mappings = toc.get("cdm_object_mapping", [])
    if not isinstance(mappings, list) or not mappings:
        return False, "cdm_object_mapping must be a non-empty list"

    # Each mapping block
    for mapping in mappings:
        for key in REQUIRED_MAPPING_KEYS:
            if key not in mapping:
                return False, f"Missing key in cdm_object_mapping: {key}"

    return True, "Valid TOC schema"

def build_sts_job(toc: dict, delete_after_transfer: bool):
    file_prefixes = [entry["name"] for entry in toc["cdm_object_mapping"][0]["dataobject"]]

    return {
        "projectId": DEST_PROJECT_ID,
        "transferJob": {
            "description": f"event-sts-job-{uuid.uuid4().hex[:8]}",
            "status": "ENABLED",
            "transferSpec": {
                "gcsDataSource": {
                    "bucketName": SOURCE_BUCKET
                },
                "gcsDataSink": {
                    "bucketName": DEST_BUCKET,
                    "path": DEST_PATH
                },
                "objectConditions": {
                    "includePrefixes": file_prefixes
                },
                "transferOptions": {
                    "overwriteObjectsAlreadyExistingInSink": True,
                    "deleteObjectsFromSourceAfterTransfer": delete_after_transfer
                }
            },
            "notificationConfig": {
                "pubsubTopic": f"projects/{NOTIFICATION_PROJECT_ID}/topics/{NOTIFICATION_TOPIC}",
                "eventTypes": ["TRANSFER_OPERATION_SUCCESS", "TRANSFER_OPERATION_FAILED"],
                "payloadFormat": "JSON"
            },
            "schedule": {
                "scheduleStartDate": {
                    "year": datetime.utcnow().year,
                    "month": datetime.utcnow().month,
                    "day": datetime.utcnow().day
                },
                "startTimeOfDay": {
                    "hours": datetime.utcnow().hour,
                    "minutes": datetime.utcnow().minute,
                    "seconds": datetime.utcnow().second
                }
            }
        }
    }

def publish_to_topic(topic_name, message: dict):
    topic_path = publisher.topic_path(DEST_PROJECT_ID, topic_name)
    publisher.publish(topic_path, json.dumps(message).encode("utf-8")).result()

def main(event, context):
    try:
        # Parse and decode message
        raw_data = base64.b64decode(event["data"]).decode("utf-8")
        msg = json.loads(raw_data)

        toc = msg.get("toc")
        delete_after_transfer = msg.get("delete_after_transfer", False)

        if not toc:
            raise ValueError("Missing 'toc' field in message")

        # Validate TOC schema
        valid, reason = validate_toc(toc)
        if not valid:
            raise ValueError(f"TOC schema invalid: {reason}")

        # Enrich STS body using ENV values only
        sts_body = build_sts_job(toc, delete_after_transfer)

        # Final enriched payload
        enriched_payload = {
            "toc": toc,
            "sts_body": sts_body,
            "delete_after_transfer": delete_after_transfer
        }

        # Publish to sts-completion-topic
        publish_to_topic(STS_COMPLETION_TOPIC, enriched_payload)
        print("Published enriched message to STS_COMPLETION_TOPIC")

        # Trigger DAG
        dag_trigger_payload = {
            "toc_id": toc["cdm_process_id"],
            "timestamp": datetime.utcnow().isoformat("T") + "Z"
        }
        publish_to_topic(DAG_TRIGGER_TOPIC, dag_trigger_payload)
        print("DAG triggered via DAG_TRIGGER_TOPIC")

    except Exception as e:
        print(f"Error: {str(e)}")
