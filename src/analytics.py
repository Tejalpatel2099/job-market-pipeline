"""ANALYTICS stage: run the SQL queries in queries/ and write a summary.

Produces reports/analytics_summary.md containing headline numbers computed
from the warehouse (totals, median annualised salary midpoint, top agency,
top skill) and one markdown table per .sql file -- every number in the
report comes out of the database built by this pipeline.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pandas as pd

from . import config

QUERIES_DIR = config.PROJECT_ROOT / "queries"


def _md_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_(no rows)_\n"
    header = "| " + " | ".join(df.columns) + " |"
    sep = "| " + " | ".join("---" for _ in df.columns) + " |"
    body = "\n".join(
        "| " + " | ".join("" if pd.isna(v) else str(v) for v in row) + " |"
        for row in df.itertuples(index=False, name=None)
    )
    return f"{header}\n{sep}\n{body}\n"


def run_analytics() -> dict:
    conn = sqlite3.connect(config.DB_PATH)

    headline = {}
    headline["total_postings"] = pd.read_sql(
        "SELECT COUNT(*) AS n FROM fact_postings", conn).iloc[0, 0]
    headline["agencies"] = pd.read_sql(
        "SELECT COUNT(*) AS n FROM dim_company", conn).iloc[0, 0]
    headline["locations"] = pd.read_sql(
        "SELECT COUNT(*) AS n FROM dim_location", conn).iloc[0, 0]
    date_range = pd.read_sql(
        "SELECT MIN(full_date) AS lo, MAX(full_date) AS hi FROM dim_date "
        "WHERE date_key IN (SELECT posting_date_key FROM fact_postings)", conn)
    headline["date_min"] = date_range.iloc[0]["lo"]
    headline["date_max"] = date_range.iloc[0]["hi"]

    mids = pd.read_sql(
        "SELECT (salary_min_annual + salary_max_annual) / 2.0 AS mid "
        "FROM fact_postings WHERE salary_min_annual IS NOT NULL "
        "AND salary_max_annual IS NOT NULL", conn)["mid"]
    headline["postings_with_salary"] = int(len(mids))
    headline["median_midpoint_annual"] = float(mids.median()) if len(mids) else None

    top_agency = pd.read_sql(
        "SELECT c.agency, COUNT(*) AS n FROM fact_postings f "
        "JOIN dim_company c ON c.company_key = f.company_key "
        "GROUP BY c.agency ORDER BY n DESC LIMIT 1", conn)
    headline["top_agency"] = top_agency.iloc[0]["agency"]
    headline["top_agency_postings"] = int(top_agency.iloc[0]["n"])

    top_skill = pd.read_sql(
        "SELECT skill, COUNT(DISTINCT posting_key) AS n FROM posting_skills "
        "GROUP BY skill ORDER BY n DESC LIMIT 1", conn)
    headline["top_skill"] = top_skill.iloc[0]["skill"]
    headline["top_skill_postings"] = int(top_skill.iloc[0]["n"])

    entry = pd.read_sql(
        "SELECT COUNT(*) AS n FROM fact_postings "
        "WHERE career_level LIKE 'Entry%'", conn).iloc[0, 0]
    headline["entry_level_postings"] = int(entry)

    lines = [
        "# Job Market Analytics Summary",
        "",
        f"_Generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} "
        f"from source: {config.SOURCE_NAME}._",
        "",
        "## Headline numbers",
        "",
        f"- **Live postings in warehouse:** {headline['total_postings']:,}",
        f"- **Distinct agencies (companies):** {headline['agencies']:,}",
        f"- **Distinct work locations:** {headline['locations']:,}",
        f"- **Posting dates span:** {headline['date_min']} to {headline['date_max']}",
        f"- **Postings with a parseable salary:** {headline['postings_with_salary']:,} "
        f"of {headline['total_postings']:,}",
    ]
    if headline["median_midpoint_annual"] is not None:
        lines.append(
            f"- **Median annualised salary midpoint:** "
            f"${headline['median_midpoint_annual']:,.0f}")
    lines += [
        f"- **Top agency:** {headline['top_agency']} "
        f"({headline['top_agency_postings']:,} postings)",
        f"- **Most demanded skill keyword:** {headline['top_skill']} "
        f"({headline['top_skill_postings']:,} postings)",
        f"- **Entry-level postings:** {headline['entry_level_postings']:,}",
        "",
    ]

    for sql_file in sorted(QUERIES_DIR.glob("*.sql")):
        sql = sql_file.read_text(encoding="utf-8")
        title = sql.strip().splitlines()[0].lstrip("- ").strip()
        df = pd.read_sql(sql, conn)
        lines += [f"## {title}", "", _md_table(df), ""]

    conn.close()
    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out = config.REPORTS_DIR / "analytics_summary.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"[analytics] summary written -> {out.relative_to(config.PROJECT_ROOT)}")
    return headline


if __name__ == "__main__":
    run_analytics()
