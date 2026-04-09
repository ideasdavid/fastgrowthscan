import os
from dotenv import load_dotenv

load_dotenv()

# Companies House
CH_API_KEY = os.getenv("CH_API_KEY", "")
CH_BASE_URL = "https://api.company-information.service.gov.uk"
CH_DOCUMENT_URL = "https://document-api.company-information.service.gov.uk"

# Anthropic — for AI-powered PDF parsing fallback
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

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
# These are the DEFAULT periods — they can be overridden at runtime via CLI flags
# e.g. python run_pipeline.py --year 2026 --baseline-year 2023 --growth-year 2024
INDEX_YEAR_ACCOUNT_PERIODS = {
    2024: {"baseline_year": 2022, "growth_year": 2023},  # FY21/22 vs FY22/23
    2025: {"baseline_year": 2023, "growth_year": 2024},  # FY22/23 vs FY23/24
    2026: {"baseline_year": 2024, "growth_year": 2025},  # FY23/24 vs FY24/25
    2027: {"baseline_year": 2025, "growth_year": 2026},  # FY24/25 vs FY25/26
    2028: {"baseline_year": 2026, "growth_year": 2027},  # FY25/26 vs FY26/27
}

# ── Bulk data settings ────────────────────────────────────────────────────────
BULK_DATA_URL = "http://download.companieshouse.gov.uk"
BULK_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "bulk")

# Account categories to exclude — companies too small to have £1m+ turnover
EXCLUDED_ACCOUNT_CATEGORIES = [
    "MICRO ENTITY",
    "DORMANT",
    "NO ACCOUNTS FILED",
    "UNAUDITED ABRIDGED",
]

# Mapping from CH bulk CSV company type strings → our internal type codes
BULK_COMPANY_TYPE_MAP = {
    "Private Limited Company": "ltd",
    "PRI/LTD BY GUAR/NSC (Private, limited by guarantee, no share capital)": "ltd",
    "Private Unlimited Company": None,
    "Public Limited Company": "plc",
    "Old Public Company": None,
    "Private Limited Company by guarantee without share capital": None,
    "Scottish Qualifying Partnership": None,
}

# ── Candidate tiering ─────────────────────────────────────────────────────────
# Tiers control the order in which candidates are processed from bulk data.
# Higher-priority tiers are processed first so the best candidates are found early.
# Each tier defines account_categories to include and an age range (years since incorporation).
CANDIDATE_TIERS = [
    {
        "name": "Tier 1: Large companies, young (FULL accounts, 3-15 years)",
        "account_categories": ["FULL"],
        "min_age": 3,
        "max_age": 15,
    },
    {
        "name": "Tier 2: Large companies, established (FULL accounts, 16+ years)",
        "account_categories": ["FULL"],
        "min_age": 16,
        "max_age": None,
    },
    {
        "name": "Tier 3: Medium companies (MEDIUM, 3-15 years)",
        "account_categories": ["MEDIUM"],
        "min_age": 3,
        "max_age": 15,
    },
    {
        "name": "Tier 4: Audit-exempt full accounts (TOTAL EXEMPTION FULL, 3-15 years)",
        "account_categories": ["TOTAL EXEMPTION FULL"],
        "min_age": 3,
        "max_age": 15,
    },
    {
        "name": "Tier 5: Small companies (SMALL/GROUP, 3-10 years)",
        "account_categories": ["SMALL", "GROUP"],
        "min_age": 3,
        "max_age": 10,
    },
]

# SIC codes to include — fast-growth relevant sectors
# Extend or customise this list as needed
INCLUDED_SIC_CODES = None  # None = all sectors; set to list of strings to filter

# Company statuses to include
VALID_COMPANY_STATUSES = ["active"]
VALID_COMPANY_TYPES = ["ltd", "plc"]

# ── SIC code sector groups ────────────────────────────────────────────────────
# Used to split pipeline runs by sector, working around CH's 10k result limit.
# Each group can be run independently — results accumulate in the same DB.

SECTOR_GROUPS = {
    "technology": {
        "label": "Technology & Software",
        "sic_codes": [
            "62011", "62012", "62020", "62090",
            "63110", "63120", "63910", "63990",
        ],
    },
    "professional_services": {
        "label": "Professional Services",
        "sic_codes": [
            "69101", "69102", "69201", "69202", "69203",
            "70100", "70221", "70229",
            "71111", "71112", "71120",
            "73110", "73120", "73200",
            "74100", "74201", "74202", "74203", "74209", "74300",
        ],
    },
    "manufacturing": {
        "label": "Manufacturing",
        "sic_codes": [
            "10110", "10120", "10130", "10200", "10310", "10320",
            "13100", "13200", "13300", "13910", "13920", "13930",
            "20110", "20120", "20130", "20140", "20150", "20160",
            "25110", "25120", "25210", "25290", "25300", "25400",
            "26110", "26120", "26200", "26300", "26400", "26511",
            "27110", "27120", "27200", "27310", "27320", "27330",
            "28110", "28120", "28130", "28140", "28150", "28160",
        ],
    },
    "wholesale_retail": {
        "label": "Wholesale & Retail",
        "sic_codes": [
            "45111", "45112", "45190", "45200", "45310", "45320",
            "46110", "46120", "46130", "46140", "46150", "46160",
            "47110", "47190", "47210", "47220", "47230", "47240",
            "47250", "47260", "47290", "47300", "47410", "47420",
            "47430", "47510", "47520", "47530", "47540", "47591",
            "47599", "47610", "47620", "47630", "47640", "47650",
            "47710", "47720", "47730", "47740", "47750", "47760",
            "47770", "47780", "47790", "47810", "47820", "47890",
            "47910", "47990",
        ],
    },
    "health_life_sciences": {
        "label": "Health & Life Sciences",
        "sic_codes": [
            "72110", "72190", "72200",
            "86101", "86102", "86210", "86220", "86230", "86900",
            "87100", "87200", "87300", "87900",
            "21100", "21200",
        ],
    },
    "construction_property": {
        "label": "Construction & Property",
        "sic_codes": [
            "41100", "41201", "41202",
            "42110", "42120", "42130", "42210", "42220", "42910", "42990",
            "43110", "43120", "43130", "43210", "43220", "43290",
            "43310", "43320", "43330", "43341", "43342", "43390",
            "43910", "43990",
            "68100", "68201", "68202", "68209", "68310", "68320",
        ],
    },
    "logistics_transport": {
        "label": "Logistics & Transport",
        "sic_codes": [
            "49100", "49200", "49310", "49320", "49390", "49410", "49420",
            "50100", "50200", "50300", "50400",
            "51101", "51102", "51210", "51220",
            "52101", "52102", "52103", "52211", "52212", "52213",
            "52219", "52220", "52230", "52240", "52290",
        ],
    },
    "financial_services": {
        "label": "Financial Services",
        "sic_codes": [
            "64110", "64190", "64201", "64202", "64203", "64204",
            "64205", "64209", "64301", "64302", "64303", "64304",
            "64305", "64306", "64910", "64920", "64991", "64992",
            "64999", "65110", "65120", "65201", "65202", "65300",
            "66110", "66120", "66190", "66210", "66220", "66290",
            "66300",
        ],
    },
    "media_entertainment": {
        "label": "Media & Entertainment",
        "sic_codes": [
            "58110", "58120", "58130", "58141", "58142", "58190",
            "58210", "58290",
            "59111", "59112", "59113", "59114", "59120", "59131",
            "59132", "59133", "59140", "59200",
            "60100", "60200",
            "90010", "90020", "90030", "90040",
        ],
    },
    "education_training": {
        "label": "Education & Training",
        "sic_codes": [
            "85100", "85200", "85310", "85320", "85410", "85421",
            "85422", "85510", "85520", "85530", "85590", "85600",
            "78100", "78200", "78300",
        ],
    },
}
