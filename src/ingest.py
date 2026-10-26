"""INGEST stage: pull job postings from the NYC Open Data Socrata API.

Fetches every row of the "Jobs NYC Postings" dataset with offset pagination
and stores an immutable, timestamped raw snapshot (the "raw zone") plus a
small metadata file describing the snapshot. No cleaning happens here --
the raw zone is a faithful copy of what the source returned.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import requests

from . import config


def fetch_all_rows() -> list[dict]:
    """Page through the Socrata API until a short page signals the end."""
    rows: list[dict] = []
    offset = 0
    select = ",".join(config.SOURCE_COLUMNS)
    while True:
        params = {
            "$select": select,
            "$limit": config.PAGE_SIZE,
            "$offset": offset,
            "$order": "job_id",
        }
        resp = requests.get(config.SOURCE_API, params=params, timeout=60)
        resp.raise_for_status()
        page = resp.json()
        rows.extend(page)
        if len(page) < config.PAGE_SIZE:
            break
        offset += config.PAGE_SIZE
    return rows


def run_ingest() -> dict:
    """Fetch all rows and persist a timestamped raw snapshot.

    Returns a dict with snapshot path, row count and fetch timestamp.
    """
    fetched_at = datetime.now(timezone.utc)
    rows = fetch_all_rows()

    config.RAW_DIR.mkdir(parents=True, exist_ok=True)
    stamp = fetched_at.strftime("%Y%m%dT%H%M%SZ")
    snapshot_path = config.RAW_DIR / f"postings_{stamp}.json"
    with open(snapshot_path, "w", encoding="utf-8") as fh:
        json.dump(rows, fh, ensure_ascii=False)

    meta = {
        "source_name": config.SOURCE_NAME,
        "source_dataset_id": config.SOURCE_DATASET_ID,
        "source_api": config.SOURCE_API,
        "fetched_at_utc": fetched_at.isoformat(),
        "row_count": len(rows),
        "snapshot_file": snapshot_path.name,
    }
    meta_path = config.RAW_DIR / f"postings_{stamp}.meta.json"
    with open(meta_path, "w", encoding="utf-8") as fh:
        json.dump(meta, fh, indent=2)

    print(f"[ingest] fetched {len(rows):,} rows -> {snapshot_path.name}")
    return {"snapshot_path": snapshot_path, **meta}


if __name__ == "__main__":
    run_ingest()
