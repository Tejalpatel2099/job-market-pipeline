# Job Market Data Pipeline & Analytics

An end-to-end data pipeline that ingests live job postings from a public
open-data API, cleans and models them into a star-schema warehouse with
incremental (idempotent) loads, guards the warehouse with a data-quality
test suite, and publishes SQL analytics on the job market.

Built with **Python, pandas, SQLite and pytest** — deliberately boring,
honest technology: no Airflow/dbt/Spark, because a single-machine pipeline
of ~2.6k rows a week does not need them.

## Data source

**NYC Open Data — "Jobs NYC Postings"** (dataset `kpav-sd4t`), published by
the NYC Department of Citywide Administrative Services (DCAS) and updated
weekly. It lists current job postings for New York City government agencies
— title, agency, category, career level, salary range, work location,
posting date and free-text skills/description.

- Dataset page: https://data.cityofnewyork.us/City-Government/Jobs-NYC-Postings/kpav-sd4t
- API: Socrata, public, **no API key required**
  (`https://data.cityofnewyork.us/resource/kpav-sd4t.json`)

## Architecture

```
            INGEST                TRANSFORM              LOAD               ANALYSE
 Socrata API ──────► data/raw/ ──────────► data/staged/ ──────► data/warehouse.db ──────► reports/
 (paginated JSON)   timestamped    pandas: clean,        star schema,        queries/*.sql
                    snapshots      dedupe, annualise     incremental         + analytics.py
                    + metadata     salaries, extract     (idempotent)        (markdown summary)
                                   skill keywords

 Every run ends with the pytest data-quality suite; a failed suite fails the run.
 Run history is recorded in the warehouse table `pipeline_runs`.
```

### Star schema (SQLite)

```
 dim_company                     fact_postings                     dim_location
 ┌─────────────┐                ┌──────────────────────┐          ┌──────────────┐
 │ company_key │◄───────────────│ company_key   (FK)   │          │ location_key │
 │ agency      │                │ location_key  (FK)   │─────────►│ work_location│
 └─────────────┘                │ posting_date_key (FK)│          └──────────────┘
 dim_date                       │ posting_key   (PK)   │
 ┌─────────────┐                │ job_id, titles,      │          posting_skills
 │ date_key    │◄───────────────│ category, career lvl,│          ┌──────────────┐
 │ full_date   │                │ positions, salary    │◄─────────│ posting_key  │
 │ year/month/ │                │ (raw + annualised)   │          │ skill        │
 │ quarter/day │                └──────────────────────┘          └──────────────┘
 └─────────────┘
```

- **Grain:** one fact row per published posting. The source publishes the
  same opening separately for Internal and External audiences, so the
  stable key is `SHA-256(job_id | posting_date | posting_type | title)`.
  Deduping on `job_id` alone would wrongly collapse ~1,240 distinct
  postings; at the correct grain only exact duplicates are dropped
  (15 in the current snapshot).
- **Incremental loads:** a fact is inserted only if its `posting_key` is
  not already in the warehouse; dimensions are get-or-create. Re-running
  the pipeline over the same data inserts **0** rows (verified below).
- **SQLite** is used because no PostgreSQL server is available on this
  machine; the DDL is standard SQL and ports directly (swap the surrogate
  `INTEGER PRIMARY KEY`s for identity columns).

### Transform details worth knowing

- **Salary annualisation:** hourly × 2080, daily × 260, weekly × 52,
  monthly × 12, so pay is comparable across frequencies; raw values are
  kept alongside.
- **Skills:** a bridge table `posting_skills` is built by keyword-matching
  each posting's `preferred_skills` + description text against a fixed,
  documented list of ~30 skill patterns (`src/transform.py`,
  `SKILL_PATTERNS`). It is transparent keyword matching, not NLP.
- **Locations** are free-text street addresses in the source; they are
  whitespace-normalised only, **not** geocoded.
- A minority of postings carry two concatenated job categories in the
  source (e.g. "Health Policy, Research & Analysis"); analytics groups by
  the string exactly as published.

## How to run

```bash
cd job-market-pipeline
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

.venv/bin/python run_pipeline.py            # full run: ingest → transform → load → analytics → checks
.venv/bin/python run_pipeline.py --skip-ingest   # reuse the newest raw snapshot
.venv/bin/python -m pytest tests/ -q        # data-quality suite on its own
```

### Scheduling

`run_pipeline.py` is a single non-interactive command, designed to be
cron-scheduled (the source updates weekly):

```cron
# Mondays 06:00 — refresh the warehouse and regenerate analytics
0 6 * * 1  cd /path/to/job-market-pipeline && .venv/bin/python run_pipeline.py >> logs/pipeline.log 2>&1
```

## Data quality (pytest, 18 tests — all passing)

`tests/test_data_quality.py` asserts, against the built warehouse:

- fact table non-empty; no NULL posting keys or job IDs; posting keys unique
- referential integrity: every fact company/location/date key resolves to
  its dimension; no NULL foreign keys; skills bridge resolves to facts
- dimension natural keys unique (agency, location, date)
- salary sanity: min ≤ max; annualised salaries in a plausible range
- posting dates within 2000-01-01 … today; `dim_date` attributes consistent
  with the calendar; positions positive where present
- reconciliation: warehouse fact count equals the staged row count, and the
  latest `pipeline_runs` log row agrees with the warehouse

## Verified runs (2026-10-26, from a clean state)

| Run | Raw rows | Staged | Facts inserted | Skipped (already loaded) | Fact total |
|-----|----------|--------|----------------|--------------------------|------------|
| 1   | 2,594    | 2,579  | **2,579**      | 0                        | 2,579      |
| 2   | 2,594    | 2,579  | **0**          | 2,579                    | 2,579      |

Run 2 re-fetched the source and inserted nothing — the incremental load is
idempotent. Dimensions after loading: 59 agencies, 205 locations, 71
posting dates; 6,094 posting→skill rows. Test suite: **18 passed**.

## Sample findings (real output, `reports/analytics_summary.md`)

From the 2,579 live postings loaded on 2026-10-26 (posting dates
2026-03-17 → 2026-10-20):

- **Median annualised salary midpoint: $82,929.** Highest-paying category
  by average midpoint: *Technology, Data & Innovation* ($111,639 across
  135 postings), ahead of Legal Affairs ($103,071).
- **Top hiring agency:** Dept of Health/Mental Hygiene — 492 postings
  (644 positions), then Dept of Environment Protection (301) and the
  Department of Transportation (237).
- **Most demanded skill keyword:** Communication (1,068 postings, 41.4%),
  then Excel (716, 27.8%) and Budgeting (633, 24.5%). For tech roles the
  standouts are SQL (137 postings), Python (78), Power BI (61) and
  Data Analysis (220).
- **Only 255 postings (9.9%) are entry-level**; 77.0% ask for experienced
  non-managers — a quantified view of the new-grad bottleneck.
- **Posting volume is recent:** 1,205 postings dated September 2026 and
  1,037 dated October 2026 (the dataset carries live postings, so older
  months have mostly closed).
- **Most common workplace:** 42-09 28th Street, Queens (225 postings —
  the health department's HQ), then 30-30 Thomson Ave, Long Island City
  (200).

## Project layout

```
run_pipeline.py          one-command orchestrator (cron-friendly)
src/config.py            source + path configuration
src/ingest.py            Socrata fetch → timestamped raw snapshots
src/transform.py         pandas cleaning, dedupe, annualisation, skills
src/load.py              star-schema DDL + incremental idempotent load
src/analytics.py         runs queries/, writes reports/analytics_summary.md
queries/                 six documented SQL analytics queries
tests/test_data_quality.py   18 pytest data-quality tests
data/raw|staged/         raw snapshots / staged CSVs
data/warehouse.db        SQLite star-schema warehouse
reports/                 generated analytics summary
```
