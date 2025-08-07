resource "google_storage_bucket" "cf_code" {
  name     = "${var.project_id}-cf-code"
  location = var.region
}

resource "google_storage_bucket_object" "cf_zip" {
  name   = "cf_sts_toc.zip"
  bucket = google_storage_bucket.cf_code.name
  source = "cf_sts_toc.zip" # Create this zip with main.py + requirements.txt
}

resource "google_cloudfunctions_function" "cf_sts_toc" {
  name        = "cf-sts-toc"
  runtime     = "python310"
  entry_point = "main"
  trigger_topic = google_pubsub_topic.object_finalize_topic.name
  source_archive_bucket = google_storage_bucket.cf_code.name
  source_archive_object = google_storage_bucket_object.cf_zip.name
  available_memory_mb = 512
  timeout = 60
  project = var.project_id
  region = var.region

  environment_variables = {
    APP_ID                = "BMG"
    DRT_ID                = "drttest"
    PIPELINE_TYPE         = "INGRESS_INCREMENTAL_C2C"
    MOVEMENT_PATTERN      = "FILE_STS"
    CDM_OBJECT_FORMAT     = "parquet"
    CDM_LANDING_TYPE      = "GCS"
    CDM_TARGET_TYPE       = "GCS"
    WRITE_DISPOSITION     = "WRITE_TRUNCATE"
    CDM_TARGET_PATH       = "gs://<dest-bucket>/target-path"
    CDM_OBJECT_IDENTIFIER = "projects/<project>/datasets/<dataset>/tables/<table>"
    TOC_BUCKET            = "<source-bucket>"
    PUBSUB_TOPIC          = "sts-completion-topic"
    PROJECT_ID            = var.project_id
  }
}



google-cloud-storage==2.16.0
google-cloud-pubsub==2.21.1



import os
import json
import uuid
import base64
from datetime import datetime
from google.cloud import storage, pubsub_v1

# ENV Vars
APP_ID = os.getenv("APP_ID")
DRT_ID = os.getenv("DRT_ID")
PIPELINE_TYPE = os.getenv("PIPELINE_TYPE")
MOVEMENT_PATTERN = os.getenv("MOVEMENT_PATTERN")
CDM_OBJECT_FORMAT = os.getenv("CDM_OBJECT_FORMAT")
CDM_LANDING_TYPE = os.getenv("CDM_LANDING_TYPE")
CDM_TARGET_TYPE = os.getenv("CDM_TARGET_TYPE")
WRITE_DISPOSITION = os.getenv("WRITE_DISPOSITION")
CDM_TARGET_PATH = os.getenv("CDM_TARGET_PATH")
CDM_OBJECT_IDENTIFIER = os.getenv("CDM_OBJECT_IDENTIFIER")
TOC_BUCKET = os.getenv("TOC_BUCKET")
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC")
PROJECT_ID = os.getenv("PROJECT_ID")

publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client()

def generate_toc(event):
    file_name = event["name"]
    bucket_name = event["bucket"]

    toc = {
        "cdm_process_id": str(uuid.uuid4()),
        "app_id": APP_ID,
        "drt_id": DRT_ID,
        "data_pipeline_type": PIPELINE_TYPE,
        "data_movement_pattern_type": MOVEMENT_PATTERN,
        "cdm_object_mapping": [
            {
                "object_name": file_name.split(".")[0],
                "cdm_object_format": CDM_OBJECT_FORMAT,
                "cdm_landing_type": CDM_LANDING_TYPE,
                "cdm_landing_path": f"gs://{bucket_name}",
                "cdm_target_type": CDM_TARGET_TYPE,
                "cdm_target_path": CDM_TARGET_PATH,
                "cdm_object_identifier": CDM_OBJECT_IDENTIFIER,
                "write_disposition": WRITE_DISPOSITION,
                "dataobject": [
                    {
                        "name": file_name,
                        "numberOfRecords": 100,  # Placeholder
                        "numberOfColumns": 5    # Placeholder
                    }
                ]
            }
        ],
        "type": "STRUCTURED_DATA_FILE",
        "businessProcessingDateTime": datetime.utcnow().isoformat("T") + "Z",
        "totalNumberOfRecords": 100,
        "numberOfColumns": 5
    }

    return toc

def upload_toc_to_gcs(toc_json):
    filename = f"sts_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}.toc"
    bucket = storage_client.bucket(TOC_BUCKET)
    blob = bucket.blob(f"toc-files/{filename}")
    blob.upload_from_string(json.dumps(toc_json), content_type="application/json")
    return f"gs://{TOC_BUCKET}/toc-files/{filename}", filename

def publish_to_pubsub(event, toc_json, toc_path):
    message = {
        "bucket": event["bucket"],
        "name": event["name"],
        "project_id": PROJECT_ID,
        "delete_after_transfer": True,
        "toc_file": toc_path,
        "toc": toc_json
    }

    topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)
    publisher.publish(topic_path, json.dumps(message).encode("utf-8")).result()

def main(event, context):
    payload = base64.b64decode(event["data"]).decode("utf-8")
    gcs_event = json.loads(payload)

    toc_json = generate_toc(gcs_event)
    toc_path, _ = upload_toc_to_gcs(toc_json)
    publish_to_pubsub(gcs_event, toc_json, toc_path)
  
