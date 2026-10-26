#!/usr/bin/env python3
"""Run the full Job Market Data Pipeline end to end:

    ingest  -> fetch a fresh raw snapshot from NYC Open Data
    transform -> clean/dedupe/annualise with pandas, extract skills
    load    -> incremental load into the SQLite star-schema warehouse
    analytics -> run queries/*.sql, regenerate reports/analytics_summary.md
    checks  -> run the pytest data-quality suite (pipeline fails if it fails)

Usage:
    .venv/bin/python run_pipeline.py              # full run
    .venv/bin/python run_pipeline.py --skip-ingest  # reuse latest raw snapshot

Designed to be scheduled, e.g. weekly with cron (see README):
    0 6 * * 1  cd /path/to/job-market-pipeline && .venv/bin/python run_pipeline.py
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from src import analytics, ingest, load, transform  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-ingest", action="store_true",
                        help="reuse the newest raw snapshot instead of fetching")
    parser.add_argument("--skip-checks", action="store_true",
                        help="do not run the pytest data-quality suite")
    args = parser.parse_args()

    if args.skip_ingest:
        print("[ingest] skipped -- reusing latest raw snapshot")
        t_stats = transform.run_transform()
    else:
        ingest.run_ingest()
        t_stats = transform.run_transform()

    load.run_load(t_stats)
    analytics.run_analytics()

    if not args.skip_checks:
        print("[checks] running data-quality test suite ...")
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", "tests/", "-q"],
            cwd=Path(__file__).resolve().parent,
        )
        if proc.returncode != 0:
            print("[checks] FAILED -- see pytest output above")
            return proc.returncode
        print("[checks] all data-quality tests passed")
    print("[done] pipeline run complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
