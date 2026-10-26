"""LOAD stage: load staged postings into a star-schema warehouse (SQLite).

Schema:
    dim_company   (company_key, agency)
    dim_location  (location_key, work_location)
    dim_date      (date_key, full_date, year, quarter, month, month_name,
                   day, day_of_week)
    fact_postings (posting_key UNIQUE, job_id, company_key FK, location_key FK,
                   posting_date_key FK, ... measures ...)
    posting_skills(posting_key FK, skill)          -- bridge for skill analytics
    pipeline_runs (run_id, run_at, raw_rows, staged_rows, facts_inserted,
                   facts_skipped_existing, fact_total_after)

Loads are INCREMENTAL and IDEMPOTENT: a fact row is inserted only when its
posting_key is not already present, so re-running the pipeline over the same
snapshot inserts 0 new facts. Dimensions are get-or-create on their natural
keys. SQLite is used because no PostgreSQL server is available on this host;
the DDL is standard SQL and ports to Postgres with serial/identity keys.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pandas as pd

from . import config

DDL = """
CREATE TABLE IF NOT EXISTS dim_company (
    company_key INTEGER PRIMARY KEY,
    agency      TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS dim_location (
    location_key  INTEGER PRIMARY KEY,
    work_location TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS dim_date (
    date_key    INTEGER PRIMARY KEY,          -- YYYYMMDD
    full_date   TEXT NOT NULL UNIQUE,         -- ISO date
    year        INTEGER NOT NULL,
    quarter     INTEGER NOT NULL,
    month       INTEGER NOT NULL,
    month_name  TEXT NOT NULL,
    day         INTEGER NOT NULL,
    day_of_week TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS fact_postings (
    posting_key                    TEXT PRIMARY KEY,
    job_id                         TEXT NOT NULL,
    company_key                    INTEGER NOT NULL REFERENCES dim_company(company_key),
    location_key                   INTEGER NOT NULL REFERENCES dim_location(location_key),
    posting_date_key               INTEGER NOT NULL REFERENCES dim_date(date_key),
    posting_type                   TEXT,
    business_title                 TEXT,
    civil_service_title            TEXT,
    job_category                   TEXT,
    full_time_part_time_indicator  TEXT,
    career_level                   TEXT,
    division_work_unit             TEXT,
    number_of_positions            INTEGER,
    salary_min                     REAL,
    salary_max                     REAL,
    salary_frequency               TEXT,
    salary_min_annual              REAL,
    salary_max_annual              REAL,
    posting_updated                TEXT,
    post_until                     TEXT,
    loaded_at                      TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS posting_skills (
    posting_key TEXT NOT NULL REFERENCES fact_postings(posting_key),
    skill       TEXT NOT NULL,
    PRIMARY KEY (posting_key, skill)
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id                 INTEGER PRIMARY KEY,
    run_at                 TEXT NOT NULL,
    snapshot               TEXT,
    raw_rows               INTEGER,
    staged_rows            INTEGER,
    facts_inserted         INTEGER,
    facts_skipped_existing INTEGER,
    fact_total_after       INTEGER
);
"""


def connect() -> sqlite3.Connection:
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(config.DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _get_or_create(cur: sqlite3.Cursor, table: str, key_col: str,
                   name_col: str, value: str) -> int:
    cur.execute(f"SELECT {key_col} FROM {table} WHERE {name_col} = ?", (value,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(f"INSERT INTO {table} ({name_col}) VALUES (?)", (value,))
    return cur.lastrowid


def _ensure_date(cur: sqlite3.Cursor, iso_date: str) -> int:
    ts = pd.Timestamp(iso_date)
    date_key = int(ts.strftime("%Y%m%d"))
    cur.execute(
        "INSERT OR IGNORE INTO dim_date "
        "(date_key, full_date, year, quarter, month, month_name, day, day_of_week) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (date_key, iso_date, ts.year, (ts.month - 1) // 3 + 1, ts.month,
         ts.strftime("%B"), ts.day, ts.strftime("%A")),
    )
    return date_key


def run_load(transform_stats: dict | None = None) -> dict:
    staged = pd.read_csv(config.STAGED_DIR / "postings_clean.csv", dtype=str,
                         keep_default_na=False)
    skills = pd.read_csv(config.STAGED_DIR / "posting_skills.csv", dtype=str,
                         keep_default_na=False)
    # Numeric columns come back as strings with keep_default_na=False; restore.
    for col in ["salary_min", "salary_max", "salary_min_annual", "salary_max_annual"]:
        staged[col] = pd.to_numeric(staged[col], errors="coerce")
    staged["number_of_positions"] = pd.to_numeric(
        staged["number_of_positions"], errors="coerce").astype("Int64")

    conn = connect()
    cur = conn.cursor()
    cur.executescript(DDL)

    cur.execute("SELECT posting_key FROM fact_postings")
    existing = {r[0] for r in cur.fetchall()}

    loaded_at = datetime.now(timezone.utc).isoformat()
    inserted = 0
    new_keys: set[str] = set()
    for row in staged.itertuples(index=False):
        if row.posting_key in existing:
            continue
        company_key = _get_or_create(cur, "dim_company", "company_key",
                                     "agency", row.agency or "Unknown")
        location_key = _get_or_create(cur, "dim_location", "location_key",
                                      "work_location", row.work_location or "Unknown")
        date_key = _ensure_date(cur, row.posting_date)
        cur.execute(
            "INSERT INTO fact_postings VALUES "
            "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                row.posting_key, row.job_id, company_key, location_key, date_key,
                row.posting_type, row.business_title, row.civil_service_title,
                row.job_category, row.full_time_part_time_indicator,
                row.career_level, row.division_work_unit,
                None if pd.isna(row.number_of_positions) else int(row.number_of_positions),
                None if pd.isna(row.salary_min) else float(row.salary_min),
                None if pd.isna(row.salary_max) else float(row.salary_max),
                row.salary_frequency,
                None if pd.isna(row.salary_min_annual) else float(row.salary_min_annual),
                None if pd.isna(row.salary_max_annual) else float(row.salary_max_annual),
                row.posting_updated, row.post_until, loaded_at,
            ),
        )
        inserted += 1
        new_keys.add(row.posting_key)

    # Skills bridge: insert only rows belonging to newly inserted facts.
    skill_rows = 0
    if new_keys and len(skills):
        new_skills = skills[skills["posting_key"].isin(new_keys)]
        cur.executemany(
            "INSERT OR IGNORE INTO posting_skills (posting_key, skill) VALUES (?, ?)",
            list(new_skills.itertuples(index=False, name=None)),
        )
        skill_rows = len(new_skills)

    cur.execute("SELECT COUNT(*) FROM fact_postings")
    total_after = cur.fetchone()[0]
    skipped = len(staged) - inserted

    stats = transform_stats or {}
    cur.execute(
        "INSERT INTO pipeline_runs (run_at, snapshot, raw_rows, staged_rows, "
        "facts_inserted, facts_skipped_existing, fact_total_after) "
        "VALUES (?,?,?,?,?,?,?)",
        (loaded_at, stats.get("snapshot", ""), stats.get("raw_rows"),
         len(staged), inserted, skipped, total_after),
    )
    conn.commit()
    conn.close()

    result = {
        "staged_rows": len(staged),
        "facts_inserted": inserted,
        "facts_skipped_existing": skipped,
        "skill_rows_inserted": skill_rows,
        "fact_total_after": total_after,
    }
    print(
        f"[load] staged={len(staged):,} inserted={inserted:,} "
        f"skipped_existing={skipped:,} total_facts={total_after:,}"
    )
    return result


if __name__ == "__main__":
    run_load()
