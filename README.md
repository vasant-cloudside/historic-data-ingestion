# 🚀 IoT Event Data Pipeline — JSON → JSONL Conversion

> Batch pipeline that converts raw IoT event JSON files from Google Cloud Storage into schema-filtered JSONL files ready for BigQuery ingestion.

---

## 📋 Project Info

| Field | Details |
|---|---|
| **Project** | Datoms POC — Historic IoT Event Data |
| **Source Bucket** | `gs://datoms-poc-bkt-historic-data/iot-event-data/` |
| **Destination Bucket** | `gs://datoms-poc-bkt-historic-data-temp/jsonl_files/` |
| **Total Records** | ~890,000 across all assets (2025 & 2026) |
| **Script** | `event_data_json_conversion.py` |

---

## 📖 Overview

This pipeline converts raw IoT event JSON files stored in GCS into consolidated, schema-filtered JSONL files suitable for ingestion into BigQuery. It covers all asset records across the years **2025 and 2026**, combining them into per-asset output files appended in chronological order.

- Processes approximately **883,947 source blobs** containing ~890,000 IoT event records
- Outputs one **JSONL file per asset** where every line is a valid JSON object conforming to the defined BigQuery schema

---

## 🏗️ Pipeline Architecture

### High-Level Flow

```
GCS Source Buckets (2025/ + 2026/)
         │
         ▼
  Discover all asset folders
         │
         ▼
  For each asset:
    ├── List all blobs across year sub-folders
    ├── Split into batches of 2,000 blobs
    ├── Download blobs in parallel (20 workers)
    ├── Parse JSON → extract schema fields → cast types
    ├── Stream records to local temp JSONL file
    └── Upload temp file → GCS destination
```

### Source & Destination

| | Path |
|---|---|
| **Source bucket** | `datoms-poc-bkt-historic-data` |
| **Source root** | `iot-event-data/<ASSET_ID>/<YEAR>/` |
| **Destination bucket** | `datoms-poc-bkt-historic-data-temp` |
| **Destination path** | `jsonl_files/<ASSET_ID>.jsonl` |

### Supported Input Formats

The parser handles three JSON file layouts **without any configuration**:

| Format | Description |
|---|---|
| **JSON Array** | File contains a top-level array of event objects |
| **JSON Object** | File contains a single event object |
| **NDJSON / JSONL** | Each line in the file is an independent JSON object |

---

## 🗂️ Output Schema

Only the fields listed below are extracted and written to the output. All other fields in the source JSON are discarded.

| Field Name | BQ Type | Mode | Description |
|---|---|---|---|
| `generated_by` | STRING | Scalar | System or service that generated the event |
| `rule_id` | INTEGER | Scalar | Rule ID linked to asset and ART ID |
| `service_id` | INTEGER | Scalar | Application / Service ID |
| `client_id` | INTEGER | Scalar | Customer ID |
| `source_type` | STRING | Scalar | Source type: device / asset / activity / task / customer |
| `asset_id` | INTEGER | Scalar | Asset ID |
| `message` | STRING | Scalar | Event message |
| `message_template_id` | INTEGER | Scalar | Template used to generate message |
| `details` | JSON | Scalar | JSON object containing event details |
| `actions` | JSON | **REPEATED** | Array of actions to perform when event occurs |
| `rule_tags` | INTEGER | **REPEATED** | Array of tags: Activity / Violation / Fault |
| `violation_type` | STRING | Scalar | Violation type: violation or revocation |
| `should_event_be_visible` | BOOLEAN | Scalar | If false, event will not appear in dashboard |
| `ingestion_time` | DATETIME | Scalar | IST ingestion time (BigQuery insert time) |
| `event_id` | INTEGER | Scalar | Unique identity of the event |
| `generated_at` | INTEGER | Scalar | Unix timestamp when event was generated |

---

## 🔄 Type Casting Rules

Each field value is cast to its BigQuery type before being written to output:

| BQ Type | Casting Behaviour |
|---|---|
| **INTEGER** | `int(value)` — handles numeric strings e.g. `"123"` → `123`. Returns `null` on failure |
| **STRING** | `str(value)` — always succeeds for non-null values |
| **BOOLEAN** | Native Python bool kept as-is; strings `"true"`, `"1"`, `"yes"` → `true`; all others → `false` |
| **DATETIME** | Passed through as-is — BigQuery load job handles ISO 8601 strings and epoch strings |
| **JSON (scalar)** | dict/list kept as Python object; JSON strings parsed via `json.loads()` |
| **JSON (REPEATED)** | Always outputs a list. Missing field → `[]`. Single non-list value is wrapped in a list |

---

## ⚙️ Configuration Parameters

| Parameter | Default | Description |
|---|---|---|
| `MAX_WORKERS` | `20` | Parallel download threads per batch |
| `BATCH_SIZE` | `2,000` | Blobs processed per batch before RAM flush |
| `FUTURE_TIMEOUT_SEC` | `120` | Max seconds to wait for a single blob result |
| `MAX_RETRIES` | `3` | Download retry attempts before skipping a blob |
| `RETRY_BACKOFF_SEC` | `3` | Base wait between retries (multiplied by attempt number) |
| `UPLOAD_CHUNK_SIZE` | `16 MB` | Resumable upload chunk size to GCS |
| `SKIP_EXISTING` | `True` | Skip assets whose output JSONL already exists in destination |

---

## 🔑 Key Functions

### `discover_assets(client)`
Lists all unique asset folder names under `iot-event-data/` using GCS delimiter listing. Returns a sorted list of asset IDs without fetching all blob metadata.

### `discover_year_prefixes(client, asset_id)`
Dynamically finds all year sub-folders (`2025/`, `2026/`, etc.) under a given asset. No hardcoded years — new years are picked up automatically.

### `list_blobs(client, bucket_name, prefix)`
Lists all blob names under a GCS prefix. Returns **name strings only** (not blob objects) to avoid holding GCS resource handles across the full 883K blob set.

### `download_and_parse(blob_name, bucket_name)`
Downloads a single blob using a **per-thread `storage.Client`** (via `threading.local`). Applies retry logic with exponential backoff. Parses content as JSON Array, JSON Object, or NDJSON. Raw bytes and text are explicitly `del`-ed after parsing to release memory immediately.

### `process_batch(blob_names, bucket_name, fout, batch_num, asset_id)`
Submits a batch of blob names to a `ThreadPoolExecutor` with `MAX_WORKERS=20`. Uses `future.result(timeout=FUTURE_TIMEOUT_SEC)` so a hung download cannot stall the entire pipeline. Records are written line-by-line to the open file handle as they complete.

### `upload_file_to_gcs(client, local_path, dest_bucket, dest_blob_name)`
Uploads the completed local JSONL temp file to GCS using `upload_from_filename()` with a 16 MB resumable chunk size — suitable for output files exceeding 1 GB.

---

## 🔀 Execution Flow

```
1.  storage.Client() initialised once in main()
2.  Discover all asset IDs under iot-event-data/
3.  For each asset (sequential):
      a.  Skip if output already exists (SKIP_EXISTING=True)
      b.  Discover year sub-folders dynamically
      c.  Collect all blob names across all years
      d.  Slice into batches of BATCH_SIZE
      e.  Open temp file with 8 MB write buffer
      f.  For each batch (sequential):
            └── Download blobs in parallel (20 workers)
                Write records to temp file as they complete
      g.  Upload temp file to GCS destination
      h.  Delete temp file
4.  Print final summary table
```

---

## 🐛 Known Issues & Resolutions

| Issue Observed | Fix Applied |
|---|---|
| **Connection pool exhausted** | Reduced `MAX_WORKERS` from 100 → 20; per-thread `storage.Client` via `threading.local` |
| **OOM Kill (process Killed)** | Batch processing (2,000 blobs/batch); stream writes directly to disk |
| **Hung at 483,400 / 415,500 records** | Per-future timeout (120 s) + retry with exponential backoff |
| **Single-blob download stall** | `download_as_bytes(timeout=90)` with 3 retry attempts |
| **Memory spike from blob refs** | Store blob **names** only; create fresh blob handle per thread |

---

## 🚦 Running the Pipeline

### Prerequisites

- Python **3.10** or later
- Install the GCS library:
  ```bash
  pip install google-cloud-storage
  ```
- Configure Google Cloud credentials (ADC or service account):
  ```bash
  gcloud auth application-default login
  ```
- Write access to destination bucket: `datoms-poc-bkt-historic-data-temp`

### Execution

```bash
python event_data_json_conversion.py
```

The script logs progress every **500 files** within a batch and at the end of each batch, including cumulative record count and throughput (records/second).

### Tuning for Available Resources

| RAM Available | Recommended Settings |
|---|---|
| **> 8 GB** | `MAX_WORKERS=30–50`, `BATCH_SIZE=5000` for higher throughput |
| **4–8 GB** | Default settings (`MAX_WORKERS=20`, `BATCH_SIZE=2000`) |
| **< 4 GB** | `MAX_WORKERS=10`, `BATCH_SIZE=1000` to reduce memory pressure |

---

## 📦 Output Files

| Property | Value |
|---|---|
| **Location** | `gs://datoms-poc-bkt-historic-data-temp/jsonl_files/<ASSET_ID>.jsonl` |
| **Format** | JSONL (Newline-Delimited JSON) — one event record per line |
| **Encoding** | UTF-8 |
| **Content-Type** | `application/jsonlines` |

Records from 2025 appear before 2026 in each output file, matching the `SOURCE_PREFIXES` order in the script. New year folders are discovered automatically — no script changes needed.

---

## 📊 Final Run Summary (Example)

```
======================================================================
PIPELINE COMPLETE — SUMMARY
======================================================================
Asset                     Status       Blobs      Records   Failed   Time(s)
------------------------- ----------  ----------  ----------  --------  ---------
Asset-27471               done          883,947     890,123        0    1842.3
Asset-12345               done           45,231      46,000        2     210.1
Asset-99999               skipped             0           0        0       0.0
======================================================================
Total assets: 3  |  Total time: 2052.4s
Done: 2  |  Skipped: 1  |  Errors: 0
```
