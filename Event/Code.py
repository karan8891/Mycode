import base64, json, os, time, uuid
from google.cloud import bigquery
from google.cloud import pubsub_v1

# Env vars
BQ_PROJECT    = os.getenv("BQ_PROJECT")       # admin project for logs
BQ_DATASET    = os.getenv("BQ_DATASET", "cdm_logs")
BQ_TABLE      = os.getenv("BQ_TABLE", "run_events")
END_TOPIC     = os.getenv("END_TOPIC")        # "projects/<proj>/topics/awg-end-topic"
SOURCE_NAME   = os.getenv("SOURCE_NAME", "cdmnxtsmoketestcf")

bq = bigquery.Client(project=BQ_PROJECT)
publisher = pubsub_v1.PublisherClient()

def _insert_bq_row(row: dict):
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    table = bq.get_table(table_id)
    errors = bq.insert_rows_json(table, [row])
    if errors:
        raise RuntimeError(str(errors))

def _safe_json(obj):
    try:
        return json.loads(json.dumps(obj))  # ensure JSON-serializable
    except Exception:
        return None

def _decode_pubsub(event):
    data_raw = event.get("data")
    payload = {}
    if data_raw:
        payload = json.loads(base64.b64decode(data_raw).decode("utf-8"))
    attributes = event.get("attributes") or {}
    return payload, attributes

def cdmnxtsmoketestcf(event, context):
    # Decode incoming
    payload, attributes = _decode_pubsub(event)
    message_id = getattr(context, "event_id", str(uuid.uuid4()))
    run_id = payload.get("run_id") or attributes.get("run_id") or f"run-{int(time.time())}"

    # 1) Log to BQ (stage = smoke)
    row = {
        "run_id": run_id,
        "stage": "smoke",
        "event_id": message_id,
        "source_cf": SOURCE_NAME,
        "status": "ok",
        "details": "smoke-stage received and logged",
        "payload_json": _safe_json(payload),
        "attributes_json": _safe_json(attributes),
    }
    _insert_bq_row(row)

    # 2) Publish END message forward
    end_payload = {
        "run_id": run_id,
        "prev_event_id": message_id,
        "note": "smoke complete; forwarding to end stage"
    }
    future = publisher.publish(
        END_TOPIC,
        json.dumps(end_payload).encode("utf-8"),
        run_id=run_id,
        stage="end"
    )
    future.result(timeout=30)  # raise if publish fails

    return



google-cloud-bigquery==3.25.0
google-cloud-pubsub==2.21.5



import base64, json, os, time, uuid
from google.cloud import bigquery

BQ_PROJECT    = os.getenv("BQ_PROJECT")
BQ_DATASET    = os.getenv("BQ_DATASET", "cdm_logs")
BQ_TABLE      = os.getenv("BQ_TABLE", "run_events")
SOURCE_NAME   = os.getenv("SOURCE_NAME", "awgendcf")

bq = bigquery.Client(project=BQ_PROJECT)

def _insert_bq_row(row: dict):
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    table = bq.get_table(table_id)
    errors = bq.insert_rows_json(table, [row])
    if errors:
        raise RuntimeError(str(errors))

def _safe_json(obj):
    try:
        return json.loads(json.dumps(obj))
    except Exception:
        return None

def _decode_pubsub(event):
    data_raw = event.get("data")
    payload = {}
    if data_raw:
        payload = json.loads(base64.b64decode(data_raw).decode("utf-8"))
    attributes = event.get("attributes") or {}
    return payload, attributes

def awgendcf(event, context):
    payload, attributes = _decode_pubsub(event)
    message_id = getattr(context, "event_id", str(uuid.uuid4()))
    run_id = payload.get("run_id") or attributes.get("run_id") or f"run-{int(time.time())}"

    row = {
        "run_id": run_id,
        "stage": "end",
        "event_id": message_id,
        "source_cf": SOURCE_NAME,
        "status": "ok",
        "details": "end-stage received and logged",
        "payload_json": _safe_json(payload),
        "attributes_json": _safe_json(attributes),
    }
    _insert_bq_row(row)
    return



google-cloud-bigquery==3.25.0
