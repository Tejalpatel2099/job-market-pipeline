"""Central configuration for the Job Market Data Pipeline."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
STAGED_DIR = DATA_DIR / "staged"
DB_PATH = DATA_DIR / "warehouse.db"
REPORTS_DIR = PROJECT_ROOT / "reports"

# --- Source: NYC Open Data, "Jobs NYC Postings" (dataset id kpav-sd4t) -----
# Published by the NYC Department of Citywide Administrative Services (DCAS),
# updated weekly. The Socrata API is public and needs no API key.
SOURCE_NAME = "NYC Open Data - Jobs NYC Postings"
SOURCE_DATASET_ID = "kpav-sd4t"
SOURCE_API = f"https://data.cityofnewyork.us/resource/{SOURCE_DATASET_ID}.json"
SOURCE_PAGE = f"https://data.cityofnewyork.us/City-Government/Jobs-NYC-Postings/{SOURCE_DATASET_ID}"
PAGE_SIZE = 1000  # Socrata pagination page size

# Columns pulled from the source (subset of the published schema).
SOURCE_COLUMNS = [
    "job_id",
    "agency",
    "posting_type",
    "number_of_positions",
    "business_title",
    "civil_service_title",
    "job_category",
    "full_time_part_time_indicator",
    "career_level",
    "salary_range_from",
    "salary_range_to",
    "salary_frequency",
    "work_location",
    "division_work_unit",
    "job_description",
    "preferred_skills",
    "posting_date",
    "post_until",
    "posting_updated",
]
