#!/usr/bin/env python3
"""
Re-process MANUAL_REVIEW companies using the AI PDF parser.
Run this after the main pipeline to convert PDF-only filings
into scored results where possible.

Usage:
    python3 reprocess_manual_review.py --year 2026
    python3 reprocess_manual_review.py --year 2026 --limit 100  # test batch
"""
import argparse
import logging
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

from app.models.session import SessionLocal, init_db
from app.models.db import IndexResult, Company, ResultStatus
from app.api.companies_house import CompaniesHouseClient
from app.parser.pdf_ai import parse_pdf_with_ai
from app.config import (
    MIN_TURNOVER_GBP, MIN_GROWTH_PERCENT,
    INDEX_YEAR_ACCOUNT_PERIODS, ANTHROPIC_API_KEY
)

logger = logging.getLogger(__name__)


def calc_growth(baseline, growth):
    if not baseline:
        return 0.0
    return ((growth - baseline) / abs(baseline)) * 100


def reprocess(index_year: int, limit: int = None):
    if not ANTHROPIC_API_KEY:
        print("ERROR: ANTHROPIC_API_KEY not set")
        sys.exit(1)

    periods = INDEX_YEAR_ACCOUNT_PERIODS.get(index_year)
    if not periods:
        print(f"ERROR: No period config for {index_year}")
        sys.exit(1)

    baseline_year = periods["baseline_year"]
    growth_year = periods["growth_year"]

    db = SessionLocal()
    client = CompaniesHouseClient()

    # Fetch all manual review results for this year
    q = (
        db.query(IndexResult)
        .filter_by(index_year=index_year, status=ResultStatus.MANUAL_REVIEW)
        .order_by(IndexResult.id)
    )
    if limit:
        q = q.limit(limit)

    results = q.all()
    total = len(results)

    print(f"\n{'='*60}")
    print(f"  Re-processing {total} MANUAL_REVIEW companies")
    print(f"  Index year: {index_year} | AI PDF parser")
    print(f"{'='*60}\n")

    stats = {"converted_qualifies": 0, "converted_dnq": 0, "still_manual": 0, "error": 0}

    for i, result in enumerate(results, 1):
        company_number = result.company_number.zfill(8)

        try:
            # Get filing history
            filings = client.get_accounts_filings(company_number)
            if not filings:
                stats["still_manual"] += 1
                continue

            # Find the growth year filing
            growth_filing = None
            baseline_filing = None
            for f in filings:
                period = f.get("description_values", {})
                made_up_date = period.get("made_up_date", "")
                if not made_up_date:
                    import re
                    match = re.search(r"(\d{4})-\d{2}-\d{2}", f.get("description", ""))
                    if match:
                        made_up_date = match.group(0)
                if made_up_date.startswith(str(growth_year)):
                    growth_filing = f
                elif made_up_date.startswith(str(baseline_year)):
                    baseline_filing = f

            if not growth_filing or not baseline_filing:
                stats["still_manual"] += 1
                continue

            # Download the growth year filing
            doc_url = growth_filing.get("links", {}).get("document_metadata")
            baseline_doc_url = baseline_filing.get("links", {}).get("document_metadata")

            if not doc_url:
                stats["still_manual"] += 1
                continue

            content = client.get_pdf_content(doc_url)
            if not content:
                stats["still_manual"] += 1
                continue

            # Try AI parse on growth year filing
            ai = parse_pdf_with_ai(content)

            # If only one period found, try baseline filing too and combine
            if ai.success and len(ai.periods) < 2 and baseline_doc_url:
                baseline_content = client.get_document_content(baseline_doc_url)
                if baseline_content:
                    ai_base = parse_pdf_with_ai(baseline_content)
                    if ai_base.success:
                        ai.periods = ai.periods + ai_base.periods
                        ai.success = True

            if not ai.success or len(ai.periods) < 2:
                stats["still_manual"] += 1
                continue

            # Sort periods by end date
            valid = [p for p in ai.periods if p.period_end and p.turnover is not None]
            if len(valid) < 2:
                stats["still_manual"] += 1
                continue

            valid.sort(key=lambda p: p.period_end, reverse=True)
            growth_p = valid[0]
            baseline_p = valid[1]

            # Validate years
            if (int(growth_p.period_end[:4]) != growth_year or
                    int(baseline_p.period_end[:4]) != baseline_year):
                stats["still_manual"] += 1
                continue

            # Score
            growth_pct = calc_growth(baseline_p.turnover, growth_p.turnover)

            result.baseline_period_start = baseline_p.period_start
            result.baseline_period_end = baseline_p.period_end
            result.baseline_turnover = baseline_p.turnover
            result.growth_period_start = growth_p.period_start
            result.growth_period_end = growth_p.period_end
            result.growth_turnover = growth_p.turnover
            result.growth_percent = growth_pct
            result.baseline_document_url = baseline_doc_url
            result.growth_document_url = doc_url
            result.manual_review_reason = None

            if baseline_p.turnover >= MIN_TURNOVER_GBP and growth_pct >= MIN_GROWTH_PERCENT:
                result.status = ResultStatus.QUALIFIES
                stats["converted_qualifies"] += 1
                logger.info(
                    f"✓ QUALIFIES: {company_number} — "
                    f"baseline £{baseline_p.turnover:,.0f} → "
                    f"growth £{growth_p.turnover:,.0f} ({growth_pct:.1f}%)"
                )
            else:
                result.status = ResultStatus.DOES_NOT_QUALIFY
                stats["converted_dnq"] += 1

            db.commit()

        except Exception as e:
            logger.error(f"Error processing {company_number}: {e}")
            stats["error"] += 1

        # Progress every 50
        if i % 50 == 0:
            logger.info(f"Progress: {i}/{total} — {stats}")

        time.sleep(0.5)  # Respect rate limits

    # Re-rank qualifiers
    logger.info("Re-ranking qualifying companies...")
    qualifiers = (
        db.query(IndexResult)
        .filter_by(index_year=index_year, status=ResultStatus.QUALIFIES)
        .order_by(IndexResult.growth_percent.desc())
        .all()
    )
    for rank, r in enumerate(qualifiers, 1):
        r.rank = rank
    db.commit()

    db.close()

    print(f"\n{'='*60}")
    print(f"  Re-processing complete")
    print(f"  ✓ Newly qualifies:      {stats['converted_qualifies']}")
    print(f"  ✗ Newly DNQ:            {stats['converted_dnq']}")
    print(f"  ⚑ Still manual review: {stats['still_manual']}")
    print(f"  Total qualifiers now:   {len(qualifiers)}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--limit", type=int, default=None, help="Cap number to process (for testing)")
    args = parser.parse_args()
    reprocess(args.year, args.limit)
