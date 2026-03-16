#!/usr/bin/env python3
"""
Backfill normalised regions for companies already in the database.
Uses postcode-to-region mapping rather than the raw CH country/region fields.

Usage:
    python3 backfill_regions.py
"""
import json
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

from dotenv import load_dotenv
load_dotenv()

from app.models.session import SessionLocal
from app.models.db import Company
from app.pipeline.regions import extract_region_from_address

logger = logging.getLogger(__name__)


def backfill():
    db = SessionLocal()

    companies = db.query(Company).all()
    total = len(companies)
    updated = 0
    no_postcode = 0

    logger.info(f"Backfilling regions for {total} companies...")

    for i, company in enumerate(companies):
        try:
            address = {}
            if company.registered_office_address:
                address = json.loads(company.registered_office_address)

            new_region = extract_region_from_address(address)

            if new_region != company.region:
                company.region = new_region
                updated += 1

            if not address.get("postal_code") and not address.get("postcode"):
                no_postcode += 1

        except Exception as e:
            logger.error(f"Error processing {company.company_number}: {e}")

        if i % 1000 == 0 and i > 0:
            db.commit()
            logger.info(f"Progress: {i}/{total} — {updated} updated so far")

    db.commit()
    db.close()

    print(f"\n{'='*50}")
    print(f"  Backfill complete")
    print(f"  Companies processed: {total:,}")
    print(f"  Regions updated:     {updated:,}")
    print(f"  No postcode found:   {no_postcode:,}")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    backfill()
