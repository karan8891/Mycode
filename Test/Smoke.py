# CF2: Orchestrator/validator that consumes CF1's outgoing message.
# - Decodes CF1 Pub/Sub payload
# - Locates the SOR bucket + {object}_sor_config_file.json and {object}_sor_db_conn.json
# - Loads and validates both JSONs
# - Extracts "source" info (db type/name/table/query) and connection info (driver/user/jceks/etc)
# - Mocks DAG1, DAG2, DAG3, writing status artifacts back to the SOR bucket so we can audit
# - Optionally forwards a compact message to the next topic (for CF3) if NEXT_TOPIC is set

import base64
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Tuple, Optional

from google.cloud import storage
from google.cloud import pubsub_v1

logger = logging.getLogger("cf2_orchestrator")
logger.setLevel(logging.INFO)

_storage = storage.Client()
_pub = None  # lazily init if NEXT_TOPIC is set

# --------- utils ---------
def _b64_json(event: Dict[str, Any]) -> Dict[str, Any]:
    data = event.get("data")
    if not data:
        raise ValueError("Missing Pub/Sub 'data'")
    if isinstance(data, bytes):
        raw = base64.b64decode(data)
    else:
        raw = base64.b64decode(data.encode("utf-8"))
    txt = raw.decode("utf-8")
    logger.info("Decoded Pub/Sub message: %s", txt[:512])
    return json.loads(txt)

def _split_gs_path(bucket_or_gs: str) -> Tuple[str, str]:
    """Accepts 'gs://bucket/optional/prefix' or plain 'bucket' and
    returns (bucket, prefix_with_trailing_slash_or_empty)."""
    s = bucket_or_gs.strip()
    if s.startswith("gs://"):
        s = s[5:]
    if "/" in s:
        bkt, prefix = s.split("/", 1)
        prefix = prefix.rstrip("/") + "/"
        return bkt, prefix
    return s, ""

def _read_json_from_gcs(bucket: str, name: str) -> Dict[str, Any]:
    blob = _storage.bucket(bucket).blob(name)
    if not blob.exists():
        raise FileNotFoundError(f"gs://{bucket}/{name} not found")
    content = blob.download_as_bytes()
    return json.loads(content.decode("utf-8"))

def _write_json_to_gcs(bucket: str, name: str, obj: Dict[str, Any]) -> None:
    blob = _storage.bucket(bucket).blob(name)
    blob.upload_from_string(json.dumps(obj, indent=2), content_type="application/json")

def _ensure_pub() -> pubsub_v1.PublisherClient:
    global _pub
    if _pub is None:
        _pub = pubsub_v1.PublisherClient()
    return _pub

def _first_key_scan(d: Any, keys: Tuple[str, ...]) -> Dict[str, Any]:
    """DFS search for the first occurrence of desired keys; returns {key:value} for those found."""
    out = {}
    stack = [d]
    while stack and len(out) < len(keys):
        node = stack.pop()
        if isinstance(node, dict):
            for k, v in node.items():
                if k in keys and k not in out:
                    out[k] = v
            for v in node.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(node, list):
            for v in node:
                if isinstance(v, (dict, list)):
                    stack.append(v)
    return out

def _extract_source_info(cfg: Dict[str, Any]) -> Dict[str, Any]:
    # Look for a structure like: dataPipeline.ingestions[0].source.sourceDetails.{...}
    keys = ("databaseType", "databaseName", "tableName", "sourceSQLQuery", "whereCondition")
    details = _first_key_scan(cfg, keys)
    # Also capture any "target" details if handy
    tkeys = ("databaseType", "databaseName", "tableName")
    target = _first_key_scan({"target": cfg}, tkeys)  # cheap reuse
    src = {
        "database_type": details.get("databaseType"),
        "database_name": details.get("databaseName"),
        "table_name": details.get("tableName"),
        "query": details.get("sourceSQLQuery"),
        "where": details.get("whereCondition"),
        "target_db": target.get("databaseType"),
        "target_dataset": target.get("databaseName"),
        "target_table": target.get("tableName"),
    }
    return src

def _extract_conn_info(conn: Dict[str, Any]) -> Dict[str, Any]:
    keys = ("jdbcDriver", "jdbcUserName", "jceksFilePath", "jceksServiceId", "sourceDbUrl")
    found = _first_key_scan(conn, keys)
    return {
        "jdbc_driver": found.get("jdbcDriver"),
        "jdbc_user": found.get("jdbcUserName"),
        "jceks_path": found.get("jceksFilePath"),
        "jceks_service": found.get("jceksServiceId"),
        "jdbc_url": found.get("sourceDbUrl"),
    }

def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

# --------- DAG mocks ---------
def _dag1_ingest(context: Dict[str, Any], source: Dict[str, Any]) -> Dict[str, Any]:
    # pretend to build a Spark job submission, but just echo back what we'd do
    logger.info("DAG1 (ingest) - planning import for %s.%s", source.get("database_name"), source.get("table_name"))
    return {
        "dag": "dag1_ingest",
        "planned_action": "read_from_source_and_write_raw",
        "source": {
            "db_type": source.get("database_type"),
            "db_name": source.get("database_name"),
            "table": source.get("table_name"),
            "where": source.get("where"),
        },
        "raw_target": context.get("cdm_landing_path"),
        "ts": _now_iso(),
        "status": "SUCCESS",
    }

def _dag2_quality(context: Dict[str, Any], source: Dict[str, Any]) -> Dict[str, Any]:
    # pretend to run data checks
    logger.info("DAG2 (quality) - running lightweight checks for object %s", context.get("object_name"))
    metrics = {
        "row_count_sample": 100000,  # mock value
        "column_count":  len([k for k in ("database_type","database_name","table_name","query","where") if source.get(k)]),
        "null_check_pass": True,
        "schema_check_pass": True,
    }
    return {
        "dag": "dag2_quality",
        "object": context.get("object_name"),
        "metrics": metrics,
        "ts": _now_iso(),
        "status": "SUCCESS",
    }

def _dag3_publish(context: Dict[str, Any]) -> Dict[str, Any]:
    # pretend to publish/curate
    logger.info("DAG3 (publish) - curating object %s -> %s", context.get("object_name"), context.get("cdm_target_path"))
    return {
        "dag": "dag3_publish",
        "object": context.get("object_name"),
        "publish_target": context.get("cdm_target_path"),
        "ts": _now_iso(),
        "status": "SUCCESS",
    }

# --------- main handler ---------
def cf2_orchestrator(event, context) -> None:
    """
    Trigger: Pub/Sub (topics from CF1's output_pubsub_message.config)
    Env (optional):
      NEXT_TOPIC = projects/<proj>/topics/<name>  (if you also want to signal CF3)
      STATUS_PREFIX = runs/                       (where to write status artifacts; default 'runs/')
    """
    msg = _b64_json(event)

    # expected from CF1
    spark = msg.get("sparkflow_parms", {})
    cdm_maps = msg.get("cdm_object_mapping") or msg.get("cdm_object_mappings") or []
    if not isinstance(cdm_maps, list) or not cdm_maps:
        raise ValueError("Expected non-empty 'cdm_object_mapping' array in message")

    sor_bucket_spec = spark.get("sor_bucket_path") or spark.get("bucket_name")
    if not sor_bucket_spec:
        raise ValueError("Missing 'sor_bucket_path' in 'sparkflow_parms'")

    bucket_name, base_prefix = _split_gs_path(sor_bucket_spec)
    status_prefix = os.getenv("STATUS_PREFIX", "runs/").rstrip("/") + "/"

    next_topic = os.getenv("NEXT_TOPIC")

    for mapping in cdm_maps:
        object_name = mapping.get("object_name") or mapping.get("name")
        if not object_name:
            raise ValueError("Each cdm_object_mapping item must include 'object_name'")

        sor_cfg_file = spark.get("sor_config_file")
        sor_conn_file = spark.get("sor_db_conn")
        if not sor_cfg_file or not sor_conn_file:
            raise ValueError("sparkflow_parms must include 'sor_config_file' and 'sor_db_conn'")

        # The artifacts are written by CF1 at the top of the bucket (no prefix) unless you chose one.
        cfg_blob = base_prefix + sor_cfg_file
        conn_blob = base_prefix + sor_conn_file

        # 1) Read + validate both files exist & parse
        logger.info("Fetching SOR config: gs://%s/%s", bucket_name, cfg_blob)
        cfg_json = _read_json_from_gcs(bucket_name, cfg_blob)

        logger.info("Fetching SOR DB conn: gs://%s/%s", bucket_name, conn_blob)
        conn_json = _read_json_from_gcs(bucket_name, conn_blob)

        # 2) Extract important bits for audit
        source_info = _extract_source_info(cfg_json)
        conn_info = _extract_conn_info(conn_json)

        logger.info("Source summary: %s", json.dumps(source_info, ensure_ascii=False))
        logger.info("Conn summary: %s", json.dumps(conn_info, ensure_ascii=False))

        # 3) Compose a tiny execution context from CF1 fields
        context_obj = {
            "cdm_process_id": msg.get("cdm_process_id") or msg.get("cdmProcessId"),
            "object_name": object_name,
            "cdm_landing_path": mapping.get("cdm_landing_path"),
            "cdm_target_path": mapping.get("cdm_target_path"),
            "cdm_target_type": mapping.get("cdm_target_type"),
        }

        # 4) Run mock DAG1/2/3
        dag1 = _dag1_ingest(context_obj, source_info)
        dag2 = _dag2_quality(context_obj, source_info)
        dag3 = _dag3_publish(context_obj)

        # 5) Write status artifacts back to the same SOR bucket so you can verify from UI/CLI
        run_root = f"{status_prefix}{context_obj['cdm_process_id']}/{object_name}/"
        _write_json_to_gcs(bucket_name, run_root + "verification.json", {
            "received_message": msg,
            "source_summary": source_info,
            "connection_summary": conn_info,
            "ts": _now_iso(),
        })
        _write_json_to_gcs(bucket_name, run_root + "dag1_ingest.json", dag1)
        _write_json_to_gcs(bucket_name, run_root + "dag2_quality.json", dag2)
        _write_json_to_gcs(bucket_name, run_root + "dag3_publish.json", dag3)

        # 6) Optionally forward a compact success signal to CF3
        if next_topic:
            publisher = _ensure_pub()
            payload = {
                "app_id": msg.get("app_id"),
                "cdm_process_id": context_obj["cdm_process_id"],
                "object_name": object_name,
                "status": "SUCCESS",
                "ran": ["dag1_ingest", "dag2_quality", "dag3_publish"],
                "sor_bucket": f"gs://{bucket_name}/{base_prefix}".rstrip("/"),
                "run_root": f"gs://{bucket_name}/{run_root}",
                "ts": _now_iso(),
            }
            data = json.dumps(payload).encode("utf-8")
            future = publisher.publish(next_topic, data=data)
            future.result()  # raise on failure
            logger.info("Forwarded summary to %s", next_topic)
