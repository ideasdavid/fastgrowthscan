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
from app.pipeline.bulk_data import BulkDataManager
from app.config import INDEX_YEAR_ACCOUNT_PERIODS


def main():
    parser = argparse.ArgumentParser(description="Run the Fast Growth Index pipeline")
    parser.add_argument("--year", type=int, required=False, help="Index year e.g. 2026")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but don't write to DB")
    parser.add_argument(
        "--candidate-source", choices=["api", "bulk", "auto"], default="auto",
        help="Candidate source: 'bulk' (pre-filtered snapshot), 'api' (CH search), 'auto' (bulk if available)"
    )
    parser.add_argument(
        "--refresh-bulk", action="store_true",
        help="Download and ingest latest CH bulk data before running"
    )
    parser.add_argument(
        "--bulk-only", action="store_true",
        help="Only download/ingest bulk data, don't run pipeline"
    )
    args = parser.parse_args()

    if not args.bulk_only and not args.year:
        parser.error("--year is required unless using --bulk-only")

    if args.year and args.year not in INDEX_YEAR_ACCOUNT_PERIODS:
        print(f"Error: Index year {args.year} not configured in app/config.py")
        sys.exit(1)

    init_db()
    db = SessionLocal()

    try:
        # ── Bulk data refresh ─────────────────────────────────────────────
        if args.refresh_bulk or args.bulk_only:
            print(f"\n{'='*60}")
            print("  Companies House Bulk Data Refresh")
            print(f"{'='*60}\n")

            manager = BulkDataManager(db)
            stats = manager.refresh()

            print(f"\n  Source file:    {stats['source_file']}")
            print(f"  Snapshot date:  {stats.get('snapshot_date', 'unknown')}")
            print(f"  Total rows:     {stats['total_rows']:,}")

            if args.year:
                periods = INDEX_YEAR_ACCOUNT_PERIODS[args.year]
                count = manager.count_pre_filtered(growth_year=periods["growth_year"])
                print(f"  Pre-filtered candidates (index {args.year}): {count:,}")

            print(f"{'='*60}\n")

            if args.bulk_only:
                return

        # ── Pipeline run ──────────────────────────────────────────────────
        periods = INDEX_YEAR_ACCOUNT_PERIODS[args.year]

        print(f"\n{'='*60}")
        print(f"  Fast Growth Index {args.year}")
        print(f"  Baseline accounts year: FY{periods['baseline_year']-1}/{str(periods['baseline_year'])[-2:]}")
        print(f"  Growth accounts year:   FY{periods['growth_year']-1}/{str(periods['growth_year'])[-2:]}")
        print(f"  Candidate source:       {args.candidate_source}")
        print(f"{'='*60}\n")

        pipeline = FastGrowthPipeline(
            db=db, index_year=args.year, dry_run=args.dry_run,
            candidate_source=args.candidate_source,
        )
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
