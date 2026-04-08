"""
Companies House bulk data snapshot ingestion.

Downloads the free monthly CSV snapshot (~5M companies, ~2GB),
loads it into a staging table, and provides pre-filtered candidate
queries that replace the slow API-based candidate search.

Bulk data product: http://download.companieshouse.gov.uk/en_output.html
"""
import csv
import io
import logging
import os
import re
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from sqlalchemy import delete, insert, select, func
from sqlalchemy.orm import Session

from app.config import (
    BULK_DATA_URL, BULK_DATA_DIR,
    EXCLUDED_ACCOUNT_CATEGORIES, BULK_COMPANY_TYPE_MAP,
    VALID_COMPANY_STATUSES,
)
from app.models.db import BulkCompanySnapshot, BulkDataMetadata

logger = logging.getLogger(__name__)


def extract_sic_code(sic_text: str) -> Optional[str]:
    """Extract numeric SIC code from bulk CSV text like '62012 - Business and domestic software development'."""
    if not sic_text or not sic_text.strip():
        return None
    parts = sic_text.strip().split(" - ", 1)
    code = parts[0].strip()
    if code.isdigit():
        return code
    return None


def parse_bulk_date(date_str: str) -> Optional[str]:
    """Convert CH bulk CSV date format DD/MM/YYYY to ISO YYYY-MM-DD."""
    if not date_str or not date_str.strip():
        return None
    try:
        return datetime.strptime(date_str.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


class BulkDataManager:
    """Manages download, ingestion, and querying of Companies House bulk data."""

    def __init__(self, db: Session, data_dir: str = None):
        self.db = db
        self.data_dir = Path(data_dir or BULK_DATA_DIR)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    # ─────────────────────────────────────────────────────────────────────────
    # Download
    # ─────────────────────────────────────────────────────────────────────────

    def download_snapshot(self, url: str = None) -> Path:
        """
        Download the latest Companies House bulk data ZIP.
        If url is not provided, scrapes the listing page for the latest file.
        Returns the path to the extracted CSV.
        """
        if not url:
            url = self._find_latest_zip_url()

        filename = url.rsplit("/", 1)[-1]
        zip_path = self.data_dir / filename

        logger.info(f"Downloading bulk data from {url} ...")
        resp = requests.get(url, stream=True, timeout=600)
        resp.raise_for_status()

        total = int(resp.headers.get("content-length", 0))
        downloaded = 0

        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = (downloaded / total) * 100
                    if downloaded % (50 * 1024 * 1024) < 1024 * 1024:  # Log every ~50MB
                        logger.info(f"  Downloaded {downloaded / 1024 / 1024:.0f}MB / {total / 1024 / 1024:.0f}MB ({pct:.0f}%)")

        logger.info(f"Download complete: {zip_path} ({downloaded / 1024 / 1024:.0f}MB)")

        # Extract CSV from ZIP
        csv_path = self._extract_zip(zip_path)
        return csv_path

    def _find_latest_zip_url(self) -> str:
        """Scrape the CH download page to find the latest BasicCompanyDataAsOneFile ZIP."""
        listing_url = f"{BULK_DATA_URL}/en_output.html"
        resp = requests.get(listing_url, timeout=30)
        resp.raise_for_status()

        # Look for links matching BasicCompanyDataAsOneFile-YYYY-MM-DD.zip
        pattern = r'href="(BasicCompanyDataAsOneFile-\d{4}-\d{2}-\d{2}\.zip)"'
        matches = re.findall(pattern, resp.text)

        if not matches:
            # Try alternate pattern without date
            pattern = r'href="(BasicCompanyDataAsOneFile[^"]*\.zip)"'
            matches = re.findall(pattern, resp.text)

        if not matches:
            raise RuntimeError(
                f"Could not find BasicCompanyDataAsOneFile ZIP on {listing_url}. "
                "You can provide a direct URL with download_snapshot(url=...)"
            )

        # Take the last match (most recent)
        latest = sorted(matches)[-1]
        return f"{BULK_DATA_URL}/{latest}"

    def _extract_zip(self, zip_path: Path) -> Path:
        """Extract the CSV from a Companies House bulk data ZIP."""
        logger.info(f"Extracting {zip_path} ...")
        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_names:
                raise RuntimeError(f"No CSV file found in {zip_path}")
            csv_name = csv_names[0]
            zf.extract(csv_name, self.data_dir)
            csv_path = self.data_dir / csv_name

        logger.info(f"Extracted: {csv_path}")
        return csv_path

    # ─────────────────────────────────────────────────────────────────────────
    # Ingest
    # ─────────────────────────────────────────────────────────────────────────

    def ingest_csv(self, csv_path: Path, chunk_size: int = 50_000) -> int:
        """
        Stream the bulk CSV into the bulk_company_snapshot table.
        Truncates the table first, then bulk-inserts in chunks.
        Returns total rows ingested.
        """
        logger.info(f"Ingesting {csv_path} into bulk_company_snapshot (chunk_size={chunk_size:,}) ...")

        # Truncate staging table
        self.db.execute(delete(BulkCompanySnapshot))
        self.db.commit()

        total_rows = 0
        batch = []

        with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)

            for row in reader:
                record = self._map_csv_row(row)
                if record:
                    batch.append(record)

                if len(batch) >= chunk_size:
                    self._flush_batch(batch)
                    total_rows += len(batch)
                    batch = []
                    if total_rows % 500_000 == 0:
                        logger.info(f"  Ingested {total_rows:,} rows ...")

            # Flush remaining
            if batch:
                self._flush_batch(batch)
                total_rows += len(batch)

        logger.info(f"Ingestion complete: {total_rows:,} rows")
        return total_rows

    def _map_csv_row(self, row: dict) -> Optional[dict]:
        """Map a CSV DictReader row to a bulk_company_snapshot record."""
        company_number = (row.get("CompanyNumber") or row.get(" CompanyNumber") or "").strip()
        if not company_number:
            return None

        return {
            "company_number": company_number.zfill(8),
            "company_name": (row.get("CompanyName") or "").strip()[:500],
            "company_status": (row.get("CompanyStatus") or "").strip(),
            "company_type": (row.get("CompanyCategory") or "").strip(),
            "incorporation_date": parse_bulk_date(row.get("IncorporationDate", "")),
            "account_category": (row.get("Accounts.AccountCategory") or "").strip().upper(),
            "sic_code_1": extract_sic_code(row.get("SICCode.SicText_1", "")),
            "sic_code_2": extract_sic_code(row.get("SICCode.SicText_2", "")),
            "sic_code_3": extract_sic_code(row.get("SICCode.SicText_3", "")),
            "sic_code_4": extract_sic_code(row.get("SICCode.SicText_4", "")),
            "postcode": (row.get("RegAddress.PostCode") or "").strip()[:15],
        }

    def _flush_batch(self, batch: list[dict]):
        """Bulk-insert a batch of records using SQLAlchemy Core for speed."""
        self.db.execute(insert(BulkCompanySnapshot), batch)
        self.db.commit()

    # ─────────────────────────────────────────────────────────────────────────
    # Pre-filter
    # ─────────────────────────────────────────────────────────────────────────

    def apply_pre_filters(
        self,
        growth_year: int,
        sic_codes: list[str] = None,
    ) -> list[dict]:
        """
        Query the snapshot table with pre-filter criteria.
        Returns a list of candidate dicts matching the shape _get_candidates() yields.
        """
        t = BulkCompanySnapshot

        # Valid company types from the bulk CSV
        valid_csv_types = [
            csv_type for csv_type, internal in BULK_COMPANY_TYPE_MAP.items()
            if internal is not None
        ]

        # Incorporation cutoff: must be 3+ years old
        cutoff_date = f"{growth_year - 3}-12-31"

        query = (
            select(t)
            .where(t.company_status == "Active")
            .where(t.company_type.in_(valid_csv_types))
            .where(t.incorporation_date <= cutoff_date)
            .where(t.incorporation_date.isnot(None))
            .where(~t.account_category.in_(EXCLUDED_ACCOUNT_CATEGORIES))
        )

        # Optional SIC code filter for sector-specific runs
        if sic_codes:
            from sqlalchemy import or_
            query = query.where(
                or_(
                    t.sic_code_1.in_(sic_codes),
                    t.sic_code_2.in_(sic_codes),
                    t.sic_code_3.in_(sic_codes),
                    t.sic_code_4.in_(sic_codes),
                )
            )

        logger.info("Running pre-filter query against bulk snapshot ...")
        rows = self.db.execute(query).scalars().all()
        logger.info(f"Pre-filter returned {len(rows):,} candidates")

        # Convert to the dict shape the pipeline expects
        candidates = []
        for row in rows:
            sic_list = [c for c in [row.sic_code_1, row.sic_code_2, row.sic_code_3, row.sic_code_4] if c]
            internal_type = BULK_COMPANY_TYPE_MAP.get(row.company_type, "ltd")

            candidates.append({
                "company_number": row.company_number,
                "company_name": row.company_name,
                "company_status": "active",
                "company_type": internal_type,
                "date_of_creation": row.incorporation_date,
                "sic_codes": sic_list,
                "registered_office_address": {"postal_code": row.postcode},
            })

        return candidates

    def count_pre_filtered(self, growth_year: int, sic_codes: list[str] = None) -> int:
        """Return the count of candidates that would pass pre-filtering (without loading them all)."""
        t = BulkCompanySnapshot

        valid_csv_types = [
            csv_type for csv_type, internal in BULK_COMPANY_TYPE_MAP.items()
            if internal is not None
        ]

        cutoff_date = f"{growth_year - 3}-12-31"

        query = (
            select(func.count())
            .select_from(t)
            .where(t.company_status == "Active")
            .where(t.company_type.in_(valid_csv_types))
            .where(t.incorporation_date <= cutoff_date)
            .where(t.incorporation_date.isnot(None))
            .where(~t.account_category.in_(EXCLUDED_ACCOUNT_CATEGORIES))
        )

        if sic_codes:
            from sqlalchemy import or_
            query = query.where(
                or_(
                    t.sic_code_1.in_(sic_codes),
                    t.sic_code_2.in_(sic_codes),
                    t.sic_code_3.in_(sic_codes),
                    t.sic_code_4.in_(sic_codes),
                )
            )

        return self.db.execute(query).scalar()

    # ─────────────────────────────────────────────────────────────────────────
    # Refresh (full orchestration)
    # ─────────────────────────────────────────────────────────────────────────

    def refresh(self, url: str = None) -> dict:
        """
        Full refresh: download → extract → ingest → update metadata.
        Returns stats dict.
        """
        # Create metadata record
        meta = BulkDataMetadata(downloaded_at=datetime.utcnow())
        self.db.add(meta)
        self.db.commit()

        try:
            csv_path = self.download_snapshot(url=url)
            meta.source_file = csv_path.name

            # Extract snapshot date from filename if possible
            date_match = re.search(r"(\d{4}-\d{2}-\d{2})", csv_path.name)
            if date_match:
                meta.snapshot_date = date_match.group(1)

            total_rows = self.ingest_csv(csv_path)
            meta.total_rows = total_rows
            meta.ingested_at = datetime.utcnow()
            self.db.commit()

            stats = {
                "source_file": csv_path.name,
                "snapshot_date": meta.snapshot_date,
                "total_rows": total_rows,
            }

            logger.info(f"Bulk data refresh complete: {stats}")
            return stats

        except Exception as e:
            meta.notes = f"Refresh failed: {e}"
            self.db.commit()
            raise

    # ─────────────────────────────────────────────────────────────────────────
    # Status
    # ─────────────────────────────────────────────────────────────────────────

    def get_snapshot_info(self) -> Optional[dict]:
        """Return metadata about the most recent successful bulk data ingestion."""
        meta = (
            self.db.query(BulkDataMetadata)
            .filter(BulkDataMetadata.ingested_at.isnot(None))
            .order_by(BulkDataMetadata.ingested_at.desc())
            .first()
        )
        if not meta:
            return None

        total_in_table = self.db.query(func.count()).select_from(BulkCompanySnapshot).scalar()

        return {
            "id": meta.id,
            "source_file": meta.source_file,
            "snapshot_date": meta.snapshot_date,
            "downloaded_at": meta.downloaded_at.isoformat() if meta.downloaded_at else None,
            "ingested_at": meta.ingested_at.isoformat() if meta.ingested_at else None,
            "total_rows": meta.total_rows,
            "rows_in_table": total_in_table,
        }

    def is_available(self) -> bool:
        """Check if bulk data has been ingested and is available for use."""
        meta = (
            self.db.query(BulkDataMetadata)
            .filter(BulkDataMetadata.ingested_at.isnot(None))
            .first()
        )
        return meta is not None
