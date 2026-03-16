import os
from dotenv import load_dotenv

load_dotenv()

# Companies House
CH_API_KEY = os.getenv("CH_API_KEY", "")
CH_BASE_URL = "https://api.company-information.service.gov.uk"
CH_DOCUMENT_URL = "https://document-api.company-information.service.gov.uk"

# Database
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost/fast_growth_index")

# Index criteria
MIN_TURNOVER_GBP = 1_000_000          # £1m minimum baseline turnover
MIN_GROWTH_PERCENT = 100.0             # 100% minimum YoY growth

# Pipeline settings
MAX_CANDIDATES = 50_000                # Safety cap on candidate pool
API_RATE_LIMIT_DELAY = 0.5            # Seconds between API calls (CH rate limit: 600/min)
REQUEST_TIMEOUT = 30

# Index year → account period mapping
# For a given index year, we look at accounts ending in these calendar years
# e.g. 2026 index uses FY23/24 (baseline) and FY24/25 (growth)
INDEX_YEAR_ACCOUNT_PERIODS = {
    2026: {"baseline_year": 2024, "growth_year": 2025},
    2027: {"baseline_year": 2025, "growth_year": 2026},
    2028: {"baseline_year": 2026, "growth_year": 2027},
}

# SIC codes to include — fast-growth relevant sectors
# Extend or customise this list as needed
INCLUDED_SIC_CODES = None  # None = all sectors; set to list of strings to filter

# Company statuses to include
VALID_COMPANY_STATUSES = ["active"]
VALID_COMPANY_TYPES = ["ltd", "plc"]  # plc included but will be filtered out if subsidiary check added
