#!/usr/bin/env python3
"""
CLI: Run the Fast Growth Index pipeline for a given year.

Usage:
    python run_pipeline.py --year 2026
    python run_pipeline.py --year 2026 --dry-run
"""
import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

from app.models.session import SessionLocal, init_db
from app.pipeline.pipeline import FastGrowthPipeline
from app.config import INDEX_YEAR_ACCOUNT_PERIODS


def main():
    parser = argparse.ArgumentParser(description="Run the Fast Growth Index pipeline")
    parser.add_argument("--year", type=int, required=True, help="Index year e.g. 2026")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but don't write to DB")
    args = parser.parse_args()

    if args.year not in INDEX_YEAR_ACCOUNT_PERIODS:
        print(f"Error: Index year {args.year} not configured in app/config.py")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  Fast Growth Index {args.year}")
    periods = INDEX_YEAR_ACCOUNT_PERIODS[args.year]
    print(f"  Baseline accounts year: FY{periods['baseline_year']-1}/{str(periods['baseline_year'])[-2:]}")
    print(f"  Growth accounts year:   FY{periods['growth_year']-1}/{str(periods['growth_year'])[-2:]}")
    print(f"{'='*60}\n")

    init_db()
    db = SessionLocal()

    try:
        pipeline = FastGrowthPipeline(db=db, index_year=args.year, dry_run=args.dry_run)
        stats = pipeline.run()

        print(f"\n{'='*60}")
        print(f"  Pipeline complete for {args.year}")
        print(f"  Candidates processed: {stats['candidates']:,}")
        print(f"  ✓ Qualifies:          {stats['qualifies']:,}")
        print(f"  ⚑ Manual review:      {stats['manual_review']:,}")
        print(f"  ✗ Does not qualify:   {stats['does_not_qualify']:,}")
        print(f"  ✗ Errors:             {stats['error']:,}")
        print(f"{'='*60}\n")

    finally:
        db.close()


if __name__ == "__main__":
    main()
