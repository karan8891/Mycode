# Cloud-to-Cloud Storage Transfer Service (STS) End-to-End Documentation

## Overview
This document consolidates architecture, design decisions, code patterns, infrastructure, IAM roles, message formats, DAG patterns, logging/audit setup, retries, performance test cases, and known issues for STS across four scenarios:

1. **APMF (Five9) → GDW (CDMP)**
2. **Event-Driven STS POC**
3. **Batch STS POC**
4. **STS Performance Test Cases**

---

## 1) APMF (Five9) → GDW (CDMP)

### Architecture
- **APMF CF (source finalize)**: Builds `.toc` JSON, writes to `toc/sts_*.toc`, publishes pointer message.
- **GDW CF (orchestrator)**: Reads pointer, optional buffer, builds STS job body (env-only), triggers Composer DAG with `dag_run.conf`.
- **Airflow DAG**: Creates/runs STS, waits for completion, audits to BigQuery, success → publishes TOC JSON to `cdmnxt-trigger-topic`; failure → checksum, retry with missing files, audit again.

### Infra
- Pub/Sub: `sts-completion-topic`, subs: `cf-trigger-dag`, `sts-completion-sub`, `cdmnxt-trigger-topic`.
- GCS: source + destination bucket.
- BigQuery: `sts_audit` (run-level), `sts_transfer_files` (optional per-file).
- Composer: public webserver, no IAP.

### IAM
- Composer SA: `storagetransfer.admin`, `pubsub.subscriber`, `bigquery.dataEditor`.
- STS service agent:
  - Source: `storage.objectViewer`
  - Destination: `storage.objectAdmin`
  - Pub/Sub: `pubsub.publisher` on completion topic

### Message Formats
**Pointer**
```json
{"bucket":"<src>","name":"<obj>","toc_bucket":"<bkt>","toc_name":"toc/sts_...toc"}
```
**TOC (minimal)**
```json
{"cdm_process_id":"<uuid>","cdm_object_mapping":[{"dataobject":[{"name":"path/file.parquet"}]}]}
```

### DAG Pattern
- Create & run → wait → audit → retry missing → publish TOC.

### Common Errors
- Sink `path` must not have leading slash.
- `transferSpec` must be dict.
- `notificationConfig.pubsubTopic` must be FQN.
- Job not starting: run() or schedule.
- AWS hook import error: dummy plugin shim.

---

## 2) Event-Driven STS POC

### Goal
Transfer only on new file arrivals using pre-created or deduplicated jobs.

### Options
**Deduplicated Create & Run**
- List jobs, reuse if exists, else create.
- Check for recent files before run.

**Pre-Created Job**
- Store job name, run when events arrive.

### Infra
- Source bucket → event publisher.
- Optional buffer to batch prefixes.

### Env
```
PROJECT_ID, SOURCE_BUCKET, DEST_BUCKET, DEST_PATH
STS_COMPLETION_TOPIC_FQN, TOC_SUBSCRIPTION_FQN
BUFFER_SECONDS, MAX_BATCH
```

### DAG Skeleton
- create_or_get_job → check_recent_files → run_job → wait_status_sensor → audit → publish.

### Gotchas
- Batch includePrefixes.
- Throttle concurrency.
- Ensure pubsub.publisher for STS agent.

---

## 3) Batch STS POC

### Goal
Predictable scheduled bulk transfers.

### Approach
- Pre-create STS job with source/dest buckets, includePrefixes, overwrite option, notificationConfig, schedule.
- Trigger via schedule or Airflow RunJobOperator.

### DAG Skeleton
- run_precreated_job → wait_status → audit → publish.

### Considerations
- Avoid overlapping scans.
- Consider versioning/checksum for safety.

---

## 4) STS Performance Test Cases

### Purpose
Validate throughput, reliability, concurrency.

### Test Matrix
| Case | Variables | Measure |
|------|-----------|---------|
| P1 Small files burst | 100k @ 10–100KB | ops duration, objects/sec |
| P2 Medium | 10k @ 5–50MB | bytes/sec |
| P3 Large | 1k @ 1–5GB | stability |
| P4 Deep prefixes | 5–10 levels | listing time |
| P5 Concurrency | 1,3,5,10 ops | throughput |
| P6 Cross-project | src/dst split | IAM delays |
| P7 Retry path | inject fails | retry success |
| P8 Cold start | idle run | startup latency |
| P9 Overwrite ON/OFF | toggle | dupes |
| P10 Event vs Batch | same data | latency tradeoff |

### Metrics
- transferCounters
- start/end time
- Airflow task durations
- Pub/Sub latency
- BQ audit counts

### Tooling
- Synthetic data gen.
- BQ dashboards.
- Alert on high failure.

### Acceptance
- MB/s throughput target
- ≥ 99% success within SLA
- ≤ target latency

---

## Appendices

### Unique Job Name
```python
from datetime import datetime as dt; import uuid
job_name = f"event_sts_job_{dt.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}"
```

### Notification Config
```json
{"pubsubTopic": "projects/<proj>/topics/sts-completion-topic","eventTypes": ["TRANSFER_OPERATION_SUCCESS","TRANSFER_OPERATION_FAILED","TRANSFER_OPERATION_ABORTED"],"payloadFormat": "JSON"}
```

### Pause/Resume/Inspect
```python
svc.transferOperations().get(name=op_name).execute()
svc.transferOperations().pause(name=op_name, body={"projectId": PROJECT_ID}).execute()
svc.transferOperations().resume(name=op_name, body={"projectId": PROJECT_ID}).execute()
```

### Per-File Logging Without TOC
Sink OBJECT_FINALIZE to BQ, query by op start/end to collect files.
```sql
SELECT * FROM gcs_object_events WHERE bucket=@dest AND event_ts BETWEEN @start AND @end
```

### Gotchas
- Remove leading slash from sink path.
- Topic must be FQN.
- Must run job after create unless scheduled.
- Cross-project Pub/Sub needs pubsub.publisher grant.
