"""Data-quality tests for the job-market warehouse.

These run against the SQLite warehouse built by the pipeline
(data/warehouse.db) and the staged CSVs produced by the transform stage.
They assert the invariants the résumé claims rest on: unique posting keys,
no null keys, referential integrity between the fact table and its
dimensions, salary/date sanity, and staged-vs-loaded reconciliation.
"""

import sqlite3
from datetime import date

import pandas as pd
import pytest

from src import config


@pytest.fixture(scope="module")
def conn():
    assert config.DB_PATH.exists(), "warehouse.db missing -- run the pipeline first"
    c = sqlite3.connect(config.DB_PATH)
    yield c
    c.close()


@pytest.fixture(scope="module")
def staged():
    return pd.read_csv(config.STAGED_DIR / "postings_clean.csv", dtype=str,
                       keep_default_na=False)


def _scalar(conn, sql):
    return conn.execute(sql).fetchone()[0]


# --- Keys & completeness ------------------------------------------------------

def test_fact_table_not_empty(conn):
    assert _scalar(conn, "SELECT COUNT(*) FROM fact_postings") > 0


def test_no_null_posting_keys(conn):
    n = _scalar(conn, "SELECT COUNT(*) FROM fact_postings "
                      "WHERE posting_key IS NULL OR posting_key = ''")
    assert n == 0


def test_no_null_job_ids(conn):
    n = _scalar(conn, "SELECT COUNT(*) FROM fact_postings "
                      "WHERE job_id IS NULL OR job_id = ''")
    assert n == 0


def test_posting_keys_unique(conn):
    total = _scalar(conn, "SELECT COUNT(*) FROM fact_postings")
    distinct = _scalar(conn, "SELECT COUNT(DISTINCT posting_key) FROM fact_postings")
    assert total == distinct


def test_staged_posting_keys_unique(staged):
    assert staged["posting_key"].is_unique


# --- Referential integrity ----------------------------------------------------

def test_fact_company_keys_resolve(conn):
    n = _scalar(conn, "SELECT COUNT(*) FROM fact_postings f "
                      "LEFT JOIN dim_company c ON c.company_key = f.company_key "
                      "WHERE c.company_key IS NULL")
    assert n == 0


def test_fact_location_keys_resolve(conn):
    n = _scalar(conn, "SELECT COUNT(*) FROM fact_postings f "
                      "LEFT JOIN dim_location l ON l.location_key = f.location_key "
                      "WHERE l.location_key IS NULL")
    assert n == 0


def test_fact_date_keys_resolve(conn):
    n = _scalar(conn, "SELECT COUNT(*) FROM fact_postings f "
                      "LEFT JOIN dim_date d ON d.date_key = f.posting_date_key "
                      "WHERE d.date_key IS NULL")
    assert n == 0


def test_no_null_foreign_keys(conn):
    n = _scalar(conn, "SELECT COUNT(*) FROM fact_postings WHERE company_key IS NULL "
                      "OR location_key IS NULL OR posting_date_key IS NULL")
    assert n == 0


def test_skills_bridge_referential_integrity(conn):
    n = _scalar(conn, "SELECT COUNT(*) FROM posting_skills s "
                      "LEFT JOIN fact_postings f ON f.posting_key = s.posting_key "
                      "WHERE f.posting_key IS NULL")
    assert n == 0


def test_dimension_natural_keys_unique(conn):
    for table, col in [("dim_company", "agency"), ("dim_location", "work_location"),
                       ("dim_date", "full_date")]:
        total = _scalar(conn, f"SELECT COUNT(*) FROM {table}")
        distinct = _scalar(conn, f"SELECT COUNT(DISTINCT {col}) FROM {table}")
        assert total == distinct, f"{table}.{col} has duplicates"


# --- Value sanity ---------------------------------------------------------------

def test_salary_min_not_above_max(conn):
    n = _scalar(conn, "SELECT COUNT(*) FROM fact_postings "
                      "WHERE salary_min IS NOT NULL AND salary_max IS NOT NULL "
                      "AND salary_min > salary_max")
    assert n == 0


def test_annualised_salary_plausible(conn):
    n = _scalar(conn, "SELECT COUNT(*) FROM fact_postings "
                      "WHERE salary_min_annual IS NOT NULL "
                      "AND (salary_min_annual <= 0 OR salary_min_annual > 2000000)")
    assert n == 0


def test_posting_dates_in_range(conn):
    today = date.today().isoformat()
    n = _scalar(conn, "SELECT COUNT(*) FROM fact_postings f "
                      "JOIN dim_date d ON d.date_key = f.posting_date_key "
                      f"WHERE d.full_date < '2000-01-01' OR d.full_date > '{today}'")
    assert n == 0


def test_dim_date_attributes_consistent(conn):
    bad = _scalar(conn, "SELECT COUNT(*) FROM dim_date WHERE "
                        "date_key <> CAST(strftime('%Y%m%d', full_date) AS INTEGER) "
                        "OR month < 1 OR month > 12 OR day < 1 OR day > 31 "
                        "OR quarter <> (month - 1) / 3 + 1")
    assert bad == 0


def test_number_of_positions_positive_where_present(conn):
    n = _scalar(conn, "SELECT COUNT(*) FROM fact_postings "
                      "WHERE number_of_positions IS NOT NULL "
                      "AND number_of_positions <= 0")
    assert n == 0


# --- Reconciliation -------------------------------------------------------------

def test_fact_count_matches_staged(conn, staged):
    """Every staged posting is loaded exactly once (idempotent loads mean
    the fact count equals the staged unique-key count for the snapshot)."""
    fact_n = _scalar(conn, "SELECT COUNT(*) FROM fact_postings")
    assert fact_n == len(staged)


def test_latest_run_log_consistent(conn):
    row = conn.execute(
        "SELECT facts_inserted, facts_skipped_existing, fact_total_after "
        "FROM pipeline_runs ORDER BY run_id DESC LIMIT 1").fetchone()
    assert row is not None
    fact_n = _scalar(conn, "SELECT COUNT(*) FROM fact_postings")
    assert row[2] == fact_n
    assert row[0] + row[1] >= 0
