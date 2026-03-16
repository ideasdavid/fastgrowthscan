# Fast Growth Index — Platform

A pipeline that identifies the fastest growing privately held companies in the UK,
using Companies House public data.

## Criteria
- Turnover FY23/24 ≥ £1,000,000
- Turnover growth FY24/25 vs FY23/24 ≥ 100%
- Privately held (active UK Ltd)
- Both years of accounts parseable

## Project Structure
```
fast-growth-index/
├── app/
│   ├── api/           # Companies House API client
│   ├── parser/        # iXBRL account document parser
│   ├── pipeline/      # Orchestration: search → fetch → score
│   ├── models/        # SQLAlchemy database models
│   └── config.py      # Settings and constants
├── migrations/        # Alembic DB migrations
├── main.py            # FastAPI entrypoint
├── run_pipeline.py    # CLI: run the index for a given year
└── .env               # API keys (not committed)
```

## Setup
1. Copy `.env.example` to `.env` and add your Companies House API key
2. Set your DATABASE_URL in `.env`
3. Run `alembic upgrade head` to create tables
4. Run `python run_pipeline.py --year 2026` to run the index

## Index Years
Each run is stored by index year. Results are cumulative — you can
query and compare across years from the dashboard.
