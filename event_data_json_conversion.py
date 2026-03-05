"""
GCS JSON to JSONL Converter — All Assets
==========================================
Dynamically discovers ALL asset folders under:
  gs://datoms-poc-bkt-historic-data/iot-event-data/

For each asset, processes all year sub-folders (2025/, 2026/, etc.)
and writes one JSONL file per asset to:
  gs://datoms-poc-bkt-historic-data-temp/jsonl_files/<ASSET_ID>.jsonl

- Batch processing (BATCH_SIZE blobs at a time) to cap RAM usage
- Per-thread storage.Client to avoid connection pool exhaustion
- Per-download timeout + retry with exponential backoff
- Streams directly to disk (no giant in-memory list)
- Skips assets whose output JSONL already exists (resumable runs)
"""

import json
import logging
import sys
import os
import time
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from google.cloud import storage
from google.api_core.exceptions import GoogleAPICallError

# ─── CONFIG ────────────────────────────────────────────────────────────────────
SOURCE_BUCKET    = "datoms-poc-bkt-historic-data"
SOURCE_ROOT      = "iot-event-data/"          # root prefix; assets are one level below
DEST_BUCKET      = "datoms-poc-bkt-historic-data-temp"
DEST_PREFIX      = "jsonl_files/"             # output: jsonl_files/<ASSET_ID>.jsonl

MAX_WORKERS        = 20
BATCH_SIZE         = 2_000
FUTURE_TIMEOUT_SEC = 120
MAX_RETRIES        = 3
RETRY_BACKOFF_SEC  = 3
UPLOAD_CHUNK_SIZE  = 16 * 1024 * 1024        # 16 MB resumable upload chunks
LOG_EVERY_FILES    = 500
SKIP_EXISTING      = True                    # set False to re-process already-done assets
# ───────────────────────────────────────────────────────────────────────────────

# ─── SCHEMA ────────────────────────────────────────────────────────────────────
SCHEMA = [
    {"name": "generated_by",            "type": "STRING",   "repeated": False},
    {"name": "rule_id",                 "type": "INTEGER",  "repeated": False},
    {"name": "service_id",              "type": "INTEGER",  "repeated": False},
    {"name": "client_id",               "type": "INTEGER",  "repeated": False},
    {"name": "source_type",             "type": "STRING",   "repeated": False},
    {"name": "asset_id",                "type": "INTEGER",  "repeated": False},
    {"name": "message",                 "type": "STRING",   "repeated": False},
    {"name": "message_template_id",     "type": "INTEGER",  "repeated": False},
    {"name": "details",                 "type": "JSON",     "repeated": False},
    {"name": "actions",                 "type": "JSON",     "repeated": True},
    {"name": "rule_tags",               "type": "INTEGER",  "repeated": True},
    {"name": "violation_type",          "type": "STRING",   "repeated": False},
    {"name": "should_event_be_visible", "type": "BOOLEAN",  "repeated": False},
    {"name": "ingestion_time",          "type": "DATETIME", "repeated": False},
    {"name": "event_id",                "type": "INTEGER",  "repeated": False},
    {"name": "generated_at",            "type": "INTEGER",  "repeated": False},
]
SCHEMA_MAP    = {f["name"]: f for f in SCHEMA}
SCHEMA_FIELDS = [f["name"] for f in SCHEMA]
# ───────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)


# ─── TYPE CASTING ──────────────────────────────────────────────────────────────
def cast_value(value, bq_type: str):
    if value is None:
        return None
    try:
        if bq_type == "INTEGER":
            return int(value)
        elif bq_type == "STRING":
            return str(value)
        elif bq_type == "BOOLEAN":
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.strip().lower() in ("true", "1", "yes")
            return bool(value)
        elif bq_type == "DATETIME":
            return str(value) if not isinstance(value, str) else value
        elif bq_type == "JSON":
            if isinstance(value, (dict, list)):
                return value
            return json.loads(value)
        else:
            return value
    except (ValueError, TypeError, json.JSONDecodeError):
        return None


def extract_record(raw: dict) -> dict:
    out = {}
    for field_name in SCHEMA_FIELDS:
        entry    = SCHEMA_MAP[field_name]
        value    = raw.get(field_name)
        bq_type  = entry["type"]
        repeated = entry["repeated"]
        if repeated:
            if value is None:
                out[field_name] = []
            elif isinstance(value, list):
                out[field_name] = [cast_value(v, bq_type) for v in value]
            else:
                out[field_name] = [cast_value(value, bq_type)]
        else:
            out[field_name] = cast_value(value, bq_type)
    return out


# ─── PER-THREAD CLIENT ────────────────────────────────────────────────────────
_thread_local = threading.local()

def get_client() -> storage.Client:
    if not hasattr(_thread_local, "client"):
        _thread_local.client = storage.Client()
    return _thread_local.client


# ─── ASSET DISCOVERY ─────────────────────────────────────────────────────────
def discover_assets(client: storage.Client) -> list[str]:
    """
    List all unique asset folder names directly under SOURCE_ROOT.
    e.g. iot-event-data/Asset-27471/ → "Asset-27471"
    Returns sorted list of asset IDs.
    """
    bucket = client.bucket(SOURCE_BUCKET)
    # Use delimiter to get "virtual directories" one level deep
    blobs = bucket.list_blobs(prefix=SOURCE_ROOT, delimiter="/")
    # Consume the iterator to populate blobs.prefixes
    _ = list(blobs)
    prefixes = blobs.prefixes  # e.g. ["iot-event-data/Asset-27471/", ...]

    assets = []
    for prefix in prefixes:
        # strip trailing slash and root, keep asset name
        asset_id = prefix.rstrip("/").split("/")[-1]
        if asset_id:
            assets.append(asset_id)

    assets.sort()
    log.info(f"Discovered {len(assets):,} assets under gs://{SOURCE_BUCKET}/{SOURCE_ROOT}")
    return assets


def discover_year_prefixes(client: storage.Client, asset_id: str) -> list[str]:
    """
    List all year sub-folders under an asset prefix.
    e.g. iot-event-data/Asset-27471/2025/ , iot-event-data/Asset-27471/2026/
    Returns list of full GCS prefix strings.
    """
    asset_prefix = f"{SOURCE_ROOT}{asset_id}/"
    bucket = client.bucket(SOURCE_BUCKET)
    blobs  = bucket.list_blobs(prefix=asset_prefix, delimiter="/")
    _      = list(blobs)
    year_prefixes = sorted(blobs.prefixes)
    return year_prefixes


def dest_blob_exists(client: storage.Client, dest_blob_name: str) -> bool:
    bucket = client.bucket(DEST_BUCKET)
    blob   = bucket.blob(dest_blob_name)
    return blob.exists()


# ─── DOWNLOAD ────────────────────────────────────────────────────────────────
def download_and_parse(blob_name: str, bucket_name: str) -> list[dict]:
    client = get_client()
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(blob_name)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            raw  = blob.download_as_bytes(timeout=90)
            text = raw.decode("utf-8").strip()
            del raw

            if not text:
                return []

            raw_records = []
            try:
                data = json.loads(text)
                raw_records = data if isinstance(data, list) else [data]
            except json.JSONDecodeError:
                for line in text.splitlines():
                    line = line.strip()
                    if line:
                        try:
                            raw_records.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass
            del text

            result = [extract_record(r) for r in raw_records if isinstance(r, dict)]
            del raw_records
            return result

        except (GoogleAPICallError, Exception) as exc:
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF_SEC * attempt
                log.warning(f"Attempt {attempt}/{MAX_RETRIES} failed [{blob_name}]: {exc}. Retry in {wait}s...")
                time.sleep(wait)
            else:
                log.error(f"All retries failed [{blob_name}]: {exc}")
    return []


def list_blobs_for_prefix(client: storage.Client, bucket_name: str, prefix: str) -> list[str]:
    bucket = client.bucket(bucket_name)
    blobs  = list(bucket.list_blobs(prefix=prefix))
    log.info(f"  Found {len(blobs):,} blobs under gs://{bucket_name}/{prefix}")
    return [b.name for b in blobs]


def process_batch(blob_names: list, bucket_name: str, fout, batch_num: int, asset_id: str) -> tuple[int, int]:
    """Returns (records_written, failed_count)."""
    record_count = 0
    done         = 0
    failed       = 0
    total        = len(blob_names)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(download_and_parse, name, bucket_name): name
            for name in blob_names
        }
        for future in as_completed(futures):
            blob_name = futures[future]
            try:
                records = future.result(timeout=FUTURE_TIMEOUT_SEC)
                for rec in records:
                    fout.write(json.dumps(rec, separators=(",", ":")) + "\n")
                    record_count += 1
                done += 1
            except TimeoutError:
                log.warning(f"TIMEOUT [{asset_id}] {blob_name} — skipping")
                future.cancel()
                failed += 1
                done   += 1
            except Exception as exc:
                log.error(f"Error [{asset_id}] {blob_name}: {exc}")
                failed += 1
                done   += 1

            if done % LOG_EVERY_FILES == 0 or done == total:
                log.info(
                    f"  [{asset_id}] Batch {batch_num}: {done:,}/{total:,} files  |  "
                    f"{record_count:,} records  |  {failed} failed"
                )

    return record_count, failed


def upload_file_to_gcs(client: storage.Client, local_path: str, dest_bucket: str, dest_blob_name: str):
    size_mb = os.path.getsize(local_path) / (1024 * 1024)
    log.info(f"  Uploading {size_mb:.1f} MB -> gs://{dest_bucket}/{dest_blob_name} ...")
    bucket = client.bucket(dest_bucket)
    blob   = bucket.blob(dest_blob_name, chunk_size=UPLOAD_CHUNK_SIZE)
    blob.upload_from_filename(local_path, content_type="application/jsonlines")
    log.info(f"  Upload complete.")


# ─── PER-ASSET PIPELINE ──────────────────────────────────────────────────────
def process_asset(client: storage.Client, asset_id: str) -> dict:
    """
    Full pipeline for one asset. Returns a summary dict.
    """
    dest_blob_name = f"{DEST_PREFIX}{asset_id}.jsonl"
    summary = {"asset": asset_id, "status": None, "records": 0, "blobs": 0, "failed": 0, "elapsed": 0}

    # Skip if already done
    if SKIP_EXISTING and dest_blob_exists(client, dest_blob_name):
        log.info(f"[{asset_id}] SKIPPED — output already exists at gs://{DEST_BUCKET}/{dest_blob_name}")
        summary["status"] = "skipped"
        return summary

    log.info(f"[{asset_id}] Starting ...")
    asset_start = time.time()

    # Discover year sub-folders dynamically
    year_prefixes = discover_year_prefixes(client, asset_id)
    if not year_prefixes:
        log.warning(f"[{asset_id}] No year sub-folders found — skipping")
        summary["status"] = "no_data"
        return summary

    log.info(f"[{asset_id}] Year prefixes: {[p.split('/')[-2] for p in year_prefixes]}")

    # Collect all blob names across all years
    all_blob_names: list[str] = []
    for prefix in year_prefixes:
        all_blob_names.extend(list_blobs_for_prefix(client, SOURCE_BUCKET, prefix))

    total_blobs = len(all_blob_names)
    log.info(f"[{asset_id}] Total blobs: {total_blobs:,}")

    if total_blobs == 0:
        log.warning(f"[{asset_id}] No blobs found — skipping")
        summary["status"] = "no_data"
        return summary

    # Stream to temp file, batch by batch
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".jsonl", prefix=f"{asset_id}_")
    os.close(tmp_fd)

    total_records = 0
    total_failed  = 0

    try:
        batches     = [all_blob_names[i : i + BATCH_SIZE] for i in range(0, total_blobs, BATCH_SIZE)]
        num_batches = len(batches)
        log.info(f"[{asset_id}] {num_batches} batch(es) of up to {BATCH_SIZE:,} blobs")

        with open(tmp_path, "w", encoding="utf-8", buffering=8 * 1024 * 1024) as fout:
            for batch_num, batch in enumerate(batches, start=1):
                log.info(f"[{asset_id}] --- Batch {batch_num}/{num_batches} ({len(batch):,} blobs) ---")
                recs, fails = process_batch(batch, SOURCE_BUCKET, fout, batch_num, asset_id)
                total_records += recs
                total_failed  += fails

                elapsed = time.time() - asset_start
                rate    = total_records / elapsed if elapsed > 0 else 0
                log.info(
                    f"[{asset_id}] Batch {batch_num} done — "
                    f"{total_records:,} records so far  |  {rate:.0f} rec/s"
                )

        # Upload
        upload_file_to_gcs(client, tmp_path, DEST_BUCKET, dest_blob_name)
        summary["status"]  = "done"

    except Exception as exc:
        log.error(f"[{asset_id}] Pipeline failed: {exc}")
        summary["status"] = "error"

    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    elapsed = time.time() - asset_start
    summary.update({"records": total_records, "blobs": total_blobs, "failed": total_failed, "elapsed": round(elapsed, 1)})
    log.info(
        f"[{asset_id}] DONE — {total_records:,} records | "
        f"{total_blobs:,} blobs | {elapsed:.1f}s | status={summary['status']}"
    )
    return summary


# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 70)
    log.info("GCS JSON -> JSONL Converter  |  ALL ASSETS")
    log.info(f"Source: gs://{SOURCE_BUCKET}/{SOURCE_ROOT}")
    log.info(f"Dest:   gs://{DEST_BUCKET}/{DEST_PREFIX}")
    log.info(f"Workers: {MAX_WORKERS}  |  Batch: {BATCH_SIZE:,}  |  Timeout: {FUTURE_TIMEOUT_SEC}s  |  Skip existing: {SKIP_EXISTING}")
    log.info("=" * 70)

    pipeline_start = time.time()
    client         = storage.Client()

    # 1. Discover all assets
    assets = discover_assets(client)
    if not assets:
        log.warning("No assets found. Exiting.")
        return

    log.info(f"Assets to process: {assets}")

    # 2. Process each asset sequentially
    #    (assets are processed one-at-a-time; parallelism is within each asset's blob downloads)
    summaries = []
    for i, asset_id in enumerate(assets, start=1):
        log.info(f"\n{'='*70}")
        log.info(f"ASSET {i}/{len(assets)}  :  {asset_id}")
        log.info(f"{'='*70}")
        summary = process_asset(client, asset_id)
        summaries.append(summary)

    # 3. Final report
    total_elapsed = time.time() - pipeline_start
    log.info(f"\n{'='*70}")
    log.info("PIPELINE COMPLETE — SUMMARY")
    log.info(f"{'='*70}")
    log.info(f"{'Asset':<25} {'Status':<10} {'Blobs':>10} {'Records':>12} {'Failed':>8} {'Time(s)':>9}")
    log.info(f"{'-'*25} {'-'*10} {'-'*10} {'-'*12} {'-'*8} {'-'*9}")
    for s in summaries:
        log.info(
            f"{s['asset']:<25} {s['status']:<10} {s['blobs']:>10,} "
            f"{s['records']:>12,} {s['failed']:>8} {s['elapsed']:>9.1f}"
        )
    log.info(f"{'='*70}")
    log.info(f"Total assets: {len(assets)}  |  Total time: {total_elapsed:.1f}s")

    done_count    = sum(1 for s in summaries if s["status"] == "done")
    skipped_count = sum(1 for s in summaries if s["status"] == "skipped")
    error_count   = sum(1 for s in summaries if s["status"] == "error")
    log.info(f"Done: {done_count}  |  Skipped: {skipped_count}  |  Errors: {error_count}")


if __name__ == "__main__":
    main()