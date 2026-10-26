"""TRANSFORM stage: clean raw postings into analysis-ready staged tables.

Reads the newest raw snapshot, then with pandas:
  * trims/collapses whitespace in text fields and standardises casing of
    coded fields (salary frequency, full/part time),
  * parses salary ranges to numbers and annualises them by frequency,
  * standardises posting_date to an ISO date,
  * builds a stable posting_key and dedupes on it. The source grain is one
    row per (job_id, posting_date, posting_type): the same opening is
    published separately for Internal and External audiences, and those
    are distinct postings, not duplicates. The key is therefore
    SHA-256 of job_id | posting_date | posting_type | business_title,
    which drops only exact duplicate records (15 in the Oct 2026 snapshot),
  * extracts skill keywords from the free-text skills/description fields
    using the documented SKILL_PATTERNS list below,
  * writes staged CSVs: postings_clean.csv and posting_skills.csv.
"""

from __future__ import annotations

import hashlib
import json
import re

import pandas as pd

from . import config

# --- Skill keyword extraction -------------------------------------------------
# Each skill maps to a regex searched (case-insensitively) in the posting's
# preferred_skills + job_description text. This is transparent keyword
# matching against a fixed, documented list -- not NLP inference.
SKILL_PATTERNS: dict[str, str] = {
    "Python": r"\bpython\b",
    "SQL": r"\bsql\b",
    "Excel": r"\bexcel\b",
    "Power BI": r"\bpower\s?bi\b",
    "Tableau": r"\btableau\b",
    "Data Analysis": r"\bdata analysis\b|\banalysing data\b|\banalyzing data\b",
    "Data Visualization": r"\bdata visuali[sz]ation\b",
    "Machine Learning": r"\bmachine learning\b",
    "Statistical Analysis": r"\bstatistical analysis\b|\bstatistics\b",
    "Java": r"\bjava\b(?!script)",
    "JavaScript": r"\bjavascript\b|\bjava script\b",
    "GIS": r"\bgis\b|\bgeographic information\b",
    "ArcGIS": r"\barcgis\b|\besri\b",
    "Microsoft Office": r"\bmicrosoft office\b|\bms office\b|\bms suite\b",
    "SharePoint": r"\bsharepoint\b",
    "Salesforce": r"\bsalesforce\b",
    "SAP": r"\bsap\b",
    "MS Access": r"\bms access\b|\bmicrosoft access\b",
    "Project Management": r"\bproject management\b",
    "Budgeting": r"\bbudget(ing|s)?\b",
    "Auditing": r"\baudit(ing|s|or)?\b",
    "Procurement": r"\bprocurement\b",
    "Contract Management": r"\bcontract (management|administration)\b",
    "Customer Service": r"\bcustomer service\b",
    "Communication": r"\bcommunication skills\b|\bcommunicator\b",
    "Quality Assurance": r"\bquality assurance\b|\bqa testing\b",
    "Bilingual Spanish": r"\bspanish\b",
    "Driving License": r"\bdriver'?s? licen[cs]e\b",
}

# Multipliers to convert a salary figure to an annual figure by frequency.
ANNUALISATION = {
    "Annual": 1,
    "Hourly": 2080,   # 40 h/week x 52 weeks
    "Daily": 260,     # 5 days/week x 52 weeks
    "Weekly": 52,
    "Monthly": 12,
}


def _latest_snapshot() -> tuple[pd.DataFrame, dict]:
    snapshots = sorted(config.RAW_DIR.glob("postings_*.json"))
    snapshots = [p for p in snapshots if not p.name.endswith(".meta.json")]
    if not snapshots:
        raise FileNotFoundError("No raw snapshot found -- run ingest first.")
    path = snapshots[-1]
    with open(path, encoding="utf-8") as fh:
        records = json.load(fh)
    meta_path = path.with_suffix("").with_suffix(".meta.json")
    meta = {}
    if meta_path.exists():
        with open(meta_path, encoding="utf-8") as fh:
            meta = json.load(fh)
    return pd.DataFrame.from_records(records), meta


def _clean_text(series: pd.Series) -> pd.Series:
    """Trim, collapse internal whitespace; missing -> empty string."""
    return (
        series.fillna("")
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )


def _posting_key(row: pd.Series) -> str:
    """Stable posting key at the source grain:
    job_id | posting_date | posting_type | business_title."""
    raw = (
        f"{row['job_id']}|{row['posting_date']}|"
        f"{row['posting_type']}|{row['business_title']}"
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def extract_skills(df: pd.DataFrame) -> pd.DataFrame:
    """Return a long table (posting_key, skill) from free-text fields."""
    text = (
        df["preferred_skills"].fillna("") + " " + df["job_description"].fillna("")
    ).str.lower()
    rows: list[tuple[str, str]] = []
    compiled = {s: re.compile(p) for s, p in SKILL_PATTERNS.items()}
    for key, blob in zip(df["posting_key"], text):
        for skill, pattern in compiled.items():
            if pattern.search(blob):
                rows.append((key, skill))
    return pd.DataFrame(rows, columns=["posting_key", "skill"]).drop_duplicates()


def run_transform() -> dict:
    df, meta = _latest_snapshot()
    raw_rows = len(df)

    # Ensure every expected column exists (Socrata omits all-null columns).
    for col in config.SOURCE_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    text_cols = [
        "job_id", "agency", "posting_type", "business_title",
        "civil_service_title", "job_category", "career_level",
        "work_location", "division_work_unit", "post_until",
    ]
    for col in text_cols:
        df[col] = _clean_text(df[col])
    df["preferred_skills"] = _clean_text(df["preferred_skills"])
    df["job_description"] = _clean_text(df["job_description"])

    # Coded fields: normalise casing.
    df["salary_frequency"] = _clean_text(df["salary_frequency"]).str.title()
    df["full_time_part_time_indicator"] = (
        _clean_text(df["full_time_part_time_indicator"]).str.upper()
    )
    # Agency names arrive SHOUTED; title-case them, then restore the
    # well-known NYC agency acronyms that title-casing would mangle.
    df["agency"] = df["agency"].str.title()
    for acronym in ["NYPD", "FDNY", "DOHMH", "HRA", "DSS", "ACS", "DCAS",
                    "DOT", "DEP", "DHS", "HPD", "DOB", "DOE", "NYC", "NYCHA",
                    "TLC", "OEM", "OMB", "OPA"]:
        df["agency"] = df["agency"].str.replace(
            rf"\b{acronym.title()}\b", acronym, regex=True)

    # Dates: standardise to ISO date; rows with no valid key fields are rejected.
    df["posting_date"] = (
        pd.to_datetime(df["posting_date"], errors="coerce").dt.date.astype(str)
    )
    df["posting_updated"] = (
        pd.to_datetime(df["posting_updated"], errors="coerce").dt.date.astype(str)
    )
    valid = (df["job_id"] != "") & (df["posting_date"] != "NaT") & (
        df["posting_date"] != "nan"
    )
    rejected = int((~valid).sum())
    df = df[valid].copy()

    # Salary parsing + annualisation.
    df["salary_min"] = pd.to_numeric(df["salary_range_from"], errors="coerce")
    df["salary_max"] = pd.to_numeric(df["salary_range_to"], errors="coerce")
    factor = df["salary_frequency"].map(ANNUALISATION)
    df["salary_min_annual"] = df["salary_min"] * factor
    df["salary_max_annual"] = df["salary_max"] * factor

    # Positions: integer where parseable.
    df["number_of_positions"] = pd.to_numeric(
        df["number_of_positions"], errors="coerce"
    ).astype("Int64")

    # Stable key + dedupe (keep first occurrence).
    df["posting_key"] = df.apply(_posting_key, axis=1)
    before_dedupe = len(df)
    df = df.drop_duplicates(subset=["posting_key"], keep="first")
    dupes_dropped = before_dedupe - len(df)

    skills = extract_skills(df)

    keep = [
        "posting_key", "job_id", "agency", "posting_type", "business_title",
        "civil_service_title", "job_category", "full_time_part_time_indicator",
        "career_level", "work_location", "division_work_unit",
        "number_of_positions", "salary_min", "salary_max", "salary_frequency",
        "salary_min_annual", "salary_max_annual", "posting_date",
        "posting_updated", "post_until",
    ]
    staged = df[keep].sort_values(["posting_date", "job_id"]).reset_index(drop=True)

    config.STAGED_DIR.mkdir(parents=True, exist_ok=True)
    staged_path = config.STAGED_DIR / "postings_clean.csv"
    skills_path = config.STAGED_DIR / "posting_skills.csv"
    staged.to_csv(staged_path, index=False)
    skills.to_csv(skills_path, index=False)

    stats = {
        "raw_rows": raw_rows,
        "rejected_rows": rejected,
        "duplicates_dropped": dupes_dropped,
        "staged_rows": len(staged),
        "skill_rows": len(skills),
        "snapshot": meta.get("snapshot_file", ""),
    }
    print(
        f"[transform] raw={raw_rows:,} rejected={rejected} "
        f"dupes={dupes_dropped} staged={len(staged):,} skill_rows={len(skills):,}"
    )
    return stats


if __name__ == "__main__":
    run_transform()
