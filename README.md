IoT Event Data Pipeline
JSON → JSONL Conversion  |

Project: Datoms POC — Historic IoT Event Data

Source Bucket
gs://datoms-poc-bkt-historic-data/iot-event-data/
Destination Bucket
gs://datoms-poc-bkt-historic-data-temp/jsonl_files/

Total Records
~890,000 across 2025 and 2026

Script `event_data_json_conversion.py`

1.  Overview
   
This document describes the batch pipeline that converts raw IoT event JSON files stored in Google Cloud Storage (GCS) into a single, schema-filtered JSONL file suitable for ingestion into BigQuery.
The pipeline covers all records for Assets across the years 2025 and 2026, combining them into one output file appended in chronological order.

The pipeline processes approximately 883,947 source blobs containing ~890,000 IoT event records. The output is one consolidated JSONL file where every line is a valid JSON object conforming to the defined
BigQuery schema.

2.  Pipeline Architecture
   
2.1  High-Level Flow

* List all blobs under the 2025/ and 2026/ GCS prefixes
* Divide blob names into batches of 2,000
* For each batch: download blobs in parallel (20 workers), parse JSON, extract schema fields, cast types, write records to a local temp JSONL file
* After all batches complete: upload the single temp JSONL file to the destination GCS bucket using a 16 MB resumable upload
* Delete the local temp file

2.2  Source & Destination

Source bucket:    datoms-poc-bkt-historic-data
Destination bucket: datoms-poc-bkt-historic-data-temp
Destination path: jsonl_files

2.3  Supported Input Formats

The parser handles three JSON file layouts without configuration:
JSON Array — the file contains a top-level array of event objects
JSON Object — the file contains a single event object
NDJSON / JSONL — each line in the file is an independent JSON object

3.  Type Casting Rules

Each field value is cast according to the following rules before being written to the output:
* INTEGER — converted using int(). Handles numeric strings (e.g. "123" → 123). Returns null on failure.
* STRING — converted using str(). Always succeeds for non-null values.
* BOOLEAN — native Python bool is kept; string values "true", "1", "yes" map to true; all others to false.
* DATETIME — kept as-is. BigQuery’s load job handles ISO 8601 strings and epoch strings.
* JSON (scalar) — dict/list values are kept as Python objects; JSON strings are parsed with json.loads().
* JSON (REPEATED) — always outputs a Python list. A missing field returns []. A single non-list value is wrapped in a list.

4.  Key Functions

* list_blobs(client, bucket_name, prefix)
* Lists all blob names under the given GCS prefix. Returns a list of name strings (not blob objects) to avoid holding GCS resource handles in memory across the full 883K blob set.
download_and_parse(blob_name, bucket_name)
* Downloads a single blob using a per-thread storage.Client (created via threading.local). Applies retry logic with exponential backoff. Parses the content as JSON Array, JSON Object, or NDJSON. Returns a list of schema-filtered, type-cast record dicts. Raw bytes and text are explicitly deleted after parsing to release memory immediately.
process_batch(blob_names, bucket_name, fout, batch_num)
* Submits a batch of blob names to a ThreadPoolExecutor with MAX_WORKERS=20. Uses future.result(timeout=FUTURE_TIMEOUT_SEC) so a hung download cannot stall the entire pipeline. Records are written line-by-line to the open file handle fout as they complete. Returns total records written in the batch.
upload_file_to_gcs(client, local_path, dest_bucket, dest_blob_name)
* Uploads the completed local JSONL temp file to the destination GCS bucket using blob.upload_from_filename() with a 16 MB resumable chunk size, suitable for files exceeding 1 GB.

4.1  Execution Flow

* `storage.Client()` is initialised once in main() for listing and uploading.
* Blob names are collected from both year prefixes into a single flat list.
* The list is sliced into batches of BATCH_SIZE (default 2,000).
* A single temp file is opened in write mode with an 8 MB write buffer.
* Each batch is processed sequentially; within each batch, downloads run in parallel.
* After all batches, the temp file is closed and uploaded to GCS.
* The temp file is deleted on success.

5.  Known Issues & Resolutions

The following issues were encountered during initial runs and resolved in the current version of the script:

* Connection pool exhausted - Reduced MAX_WORKERS from 100 → 20; per-thread storage.Client
* OOM Kill (Killed) - Batch processing (2,000 blobs/batch); stream writes to disk
* Hung at 483,400 / 415,500 - Per-future timeout (120 s) + retry with exponential backoff
* Single-blob download stall - download_as_bytes(timeout=90) with 3 retry attempts
* Memory spike from blob refs - Store blob names only; create fresh handle per thread

6.  Running the Pipeline
   
6.1  Prerequisites

* Python 3.10 or later
* google-cloud-storage library installed: pip install google-cloud-storage
* Google Cloud credentials configured (ADC or service account key): gcloud auth application-default login
* Write access to the destination bucket:  `datoms-poc-bkt-historic-data-temp`

6.2  Execution

python `event_data_json_conversion.py`

The script logs progress at every 500 files within a batch and at the end of each batch, including cumulative record count and throughput (records/second).

6.3  Tuning for Available Resources

If the job runs on a VM with more than 8 GB RAM, the following parameters may be increased for faster throughput:
* Increase MAX_WORKERS to 30–50 for more parallel downloads
* Increase BATCH_SIZE to 5,000 to reduce batch-overhead and improve write efficiency
* If RAM is constrained (< 4 GB), reduce MAX_WORKERS to 10 and BATCH_SIZE to 1,000

7.  Output File
Location:  gs://datoms-poc-bkt-historic-data-temp/jsonl_files/
Format:    JSONL (Newline-Delimited JSON) — one event record per line
Encoding:  UTF-8
Content type: application/jsonlines

The output file contains all records from 2025 and 2026 appended sequentially — 2025 records appear first followed by 2026 records, matching the order of SOURCE_PREFIXES in the script configuration.

The JSONL format is directly compatible with BigQuery’s bq load command using --source_format=NEWLINE_DELIMITED_JSON and the schema defined in Section 3.

