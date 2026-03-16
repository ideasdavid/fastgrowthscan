"""
Pipeline orchestrator.
Runs the full Fast Growth Index pipeline for a given index year.
"""
import json
import logging
import time
from datetime import datetime, date
from typing import Optional
from sqlalchemy.orm import Session

from app.api.companies_house import CompaniesHouseClient
from app.parser.ixbrl import parse_accounts, ParseResult
from app.models.db import Company, IndexResult, PipelineRun, ResultStatus
from app.config import (
    MIN_TURNOVER_GBP, MIN_GROWTH_PERCENT,
    INDEX_YEAR_ACCOUNT_PERIODS, INCLUDED_SIC_CODES, MAX_CANDIDATES
)

logger = logging.getLogger(__name__)


class FastGrowthPipeline:

    def __init__(self, db: Session, index_year: int, dry_run: bool = False):
        self.db = db
        self.index_year = index_year
        self.dry_run = dry_run
        self.client = CompaniesHouseClient()

        if index_year not in INDEX_YEAR_ACCOUNT_PERIODS:
            raise ValueError(f"No account period config for index year {index_year}")

        self.periods = INDEX_YEAR_ACCOUNT_PERIODS[index_year]
        self.baseline_year = self.periods["baseline_year"]  # e.g. 2024
        self.growth_year = self.periods["growth_year"]      # e.g. 2025

        self.stats = {
            "candidates": 0,
            "qualifies": 0,
            "manual_review": 0,
            "does_not_qualify": 0,
            "error": 0,
        }

    def run(self):
        logger.info(f"Starting Fast Growth Index {self.index_year}")
        logger.info(f"Looking for accounts ending in {self.baseline_year} and {self.growth_year}")

        run = PipelineRun(index_year=self.index_year)
        self.db.add(run)
        self.db.commit()

        try:
            for company_data in self._get_candidates():
                self.stats["candidates"] += 1
                try:
                    self._process_company(company_data)
                except Exception as e:
                    logger.error(f"Error processing {company_data.get('company_number')}: {e}")
                    self.stats["error"] += 1

                if self.stats["candidates"] % 100 == 0:
                    logger.info(f"Progress: {self.stats}")

            # Rank the qualifiers
            self._assign_ranks()

            # Update run record
            run.completed_at = datetime.utcnow()
            run.candidates_found = self.stats["candidates"]
            run.qualifies_count = self.stats["qualifies"]
            run.manual_review_count = self.stats["manual_review"]
            run.does_not_qualify_count = self.stats["does_not_qualify"]
            run.error_count = self.stats["error"]
            self.db.commit()

            logger.info(f"Pipeline complete: {self.stats}")
            return self.stats

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            run.notes = f"Pipeline failed: {e}"
            self.db.commit()
            raise

    # ─────────────────────────────────────────────────────────────────────────
    # Stage 1: Build candidate pool
    # ─────────────────────────────────────────────────────────────────────────

    def _get_candidates(self):
        """
        Yield candidate companies from Companies House search.
        Filters: active, ltd, incorporated >= 3 years ago.
        """
        cutoff_date = date(self.growth_year - 3, 1, 1).isoformat()
        seen = set()

        for company in self.client.iter_all_companies(
            company_type="ltd",
            company_status="active",
            sic_codes=INCLUDED_SIC_CODES,
            max_results=MAX_CANDIDATES,
        ):
            number = company.get("company_number")
            if not number or number in seen:
                continue
            seen.add(number)

            # Filter out recently incorporated companies
            inc_date = company.get("date_of_creation", "")
            if inc_date and inc_date > cutoff_date:
                continue

            yield company

    # ─────────────────────────────────────────────────────────────────────────
    # Stage 2 & 3: Fetch financials and score
    # ─────────────────────────────────────────────────────────────────────────

    def _process_company(self, company_data: dict):
        """Fetch accounts, parse, score, and save result for one company."""
        number = company_data["company_number"]
        name = company_data.get("company_name", "")

        # Upsert company record
        company = self.db.get(Company, number)
        if not company:
            company = Company(company_number=number)
            self.db.add(company)

        company.company_name = name
        company.company_status = company_data.get("company_status")
        company.company_type = company_data.get("company_type")
        company.incorporated_date = company_data.get("date_of_creation")
        company.sic_codes = json.dumps(company_data.get("sic_codes", []))
        company.registered_office_address = json.dumps(
            company_data.get("registered_office_address", {})
        )
        company.region = self._extract_region(
            company_data.get("registered_office_address", {})
        )

        # Check for existing result for this year (allow re-runs)
        existing = (
            self.db.query(IndexResult)
            .filter_by(index_year=self.index_year, company_number=number)
            .first()
        )

        result = existing or IndexResult(
            index_year=self.index_year, company_number=number
        )

        # Fetch and evaluate accounts
        status, data = self._evaluate_financials(number)
        result.status = status

        if data:
            result.baseline_period_start = data.get("baseline_start")
            result.baseline_period_end = data.get("baseline_end")
            result.baseline_turnover = data.get("baseline_turnover")
            result.growth_period_start = data.get("growth_start")
            result.growth_period_end = data.get("growth_end")
            result.growth_turnover = data.get("growth_turnover")
            result.growth_percent = data.get("growth_percent")
            result.baseline_filing_id = data.get("baseline_filing_id")
            result.growth_filing_id = data.get("growth_filing_id")
            result.baseline_document_url = data.get("baseline_doc_url")
            result.growth_document_url = data.get("growth_doc_url")

        if status == ResultStatus.MANUAL_REVIEW:
            result.manual_review_reason = data.get("reason") if data else "Unknown"

        self.db.add(result)
        self.db.commit()

        self.stats[status.value.lower().replace(" ", "_")] = (
            self.stats.get(status.value.lower().replace(" ", "_"), 0) + 1
        )
        # Map to our stats keys
        key_map = {
            ResultStatus.QUALIFIES: "qualifies",
            ResultStatus.DOES_NOT_QUALIFY: "does_not_qualify",
            ResultStatus.MANUAL_REVIEW: "manual_review",
            ResultStatus.ERROR: "error",
        }
        self.stats[key_map[status]] += 1

    def _evaluate_financials(self, company_number: str) -> tuple[ResultStatus, Optional[dict]]:
        """
        Fetch filing history, download accounts, parse, and apply criteria.
        Returns (status, data_dict).
        """
        try:
            filings = self.client.get_accounts_filings(company_number)
        except Exception as e:
            return ResultStatus.ERROR, {"reason": str(e)}

        if not filings:
            return ResultStatus.DOES_NOT_QUALIFY, None

        # Find filings whose period_of_accounts end year matches baseline and growth years
        baseline_filing = self._find_filing_for_year(filings, self.baseline_year)
        growth_filing = self._find_filing_for_year(filings, self.growth_year)

        if not growth_filing:
            # Growth year accounts not yet filed — can't evaluate
            return ResultStatus.DOES_NOT_QUALIFY, None

        if not baseline_filing:
            return ResultStatus.DOES_NOT_QUALIFY, None

        # Try to get financial data from the growth year filing (often contains prior year comparatives)
        doc_url = growth_filing.get("links", {}).get("document_metadata")
        baseline_doc_url = baseline_filing.get("links", {}).get("document_metadata")

        if not doc_url:
            return ResultStatus.MANUAL_REVIEW, {
                "reason": "No document metadata link found on filing",
                "growth_filing_id": growth_filing.get("transaction_id"),
            }

        content = self.client.get_document_content(doc_url)

        if content is None:
            return ResultStatus.MANUAL_REVIEW, {
                "reason": "Document not downloadable — may be PDF only",
                "growth_filing_id": growth_filing.get("transaction_id"),
                "growth_doc_url": doc_url,
            }

        parsed = parse_accounts(content)

        if not parsed.success:
            # Try the baseline filing separately if growth filing parse failed
            if baseline_doc_url and "No turnover" in (parsed.reason or ""):
                return ResultStatus.MANUAL_REVIEW, {
                    "reason": parsed.reason,
                    "growth_filing_id": growth_filing.get("transaction_id"),
                    "baseline_filing_id": baseline_filing.get("transaction_id"),
                    "growth_doc_url": doc_url,
                    "baseline_doc_url": baseline_doc_url,
                }
            return ResultStatus.MANUAL_REVIEW, {
                "reason": parsed.reason,
                "growth_filing_id": growth_filing.get("transaction_id"),
                "growth_doc_url": doc_url,
            }

        # Validate the periods match what we expect
        baseline = parsed.baseline
        growth = parsed.growth

        if not baseline or not growth:
            return ResultStatus.MANUAL_REVIEW, {
                "reason": "Could not identify two distinct accounting periods",
                "growth_doc_url": doc_url,
            }

        # Check the growth period end year matches
        growth_end_year = int(growth.end_date[:4]) if growth.end_date else 0
        baseline_end_year = int(baseline.end_date[:4]) if baseline.end_date else 0

        if growth_end_year != self.growth_year or baseline_end_year != self.baseline_year:
            return ResultStatus.MANUAL_REVIEW, {
                "reason": (
                    f"Period mismatch: found {baseline_end_year}/{growth_end_year}, "
                    f"expected {self.baseline_year}/{self.growth_year}"
                ),
                "growth_doc_url": doc_url,
            }

        # Apply criteria
        data = {
            "baseline_start": baseline.start_date,
            "baseline_end": baseline.end_date,
            "baseline_turnover": baseline.turnover,
            "growth_start": growth.start_date,
            "growth_end": growth.end_date,
            "growth_turnover": growth.turnover,
            "baseline_filing_id": baseline_filing.get("transaction_id"),
            "growth_filing_id": growth_filing.get("transaction_id"),
            "baseline_doc_url": baseline_doc_url,
            "growth_doc_url": doc_url,
        }

        if baseline.turnover is None or growth.turnover is None:
            return ResultStatus.MANUAL_REVIEW, {**data, "reason": "Turnover value missing"}

        if baseline.turnover < MIN_TURNOVER_GBP:
            data["growth_percent"] = self._calc_growth(baseline.turnover, growth.turnover)
            return ResultStatus.DOES_NOT_QUALIFY, data

        growth_pct = self._calc_growth(baseline.turnover, growth.turnover)
        data["growth_percent"] = growth_pct

        if growth_pct >= MIN_GROWTH_PERCENT:
            return ResultStatus.QUALIFIES, data
        else:
            return ResultStatus.DOES_NOT_QUALIFY, data

    # ─────────────────────────────────────────────────────────────────────────
    # Stage 4: Ranking
    # ─────────────────────────────────────────────────────────────────────────

    def _assign_ranks(self):
        """Rank all qualifying companies by growth % descending."""
        qualifiers = (
            self.db.query(IndexResult)
            .filter_by(index_year=self.index_year, status=ResultStatus.QUALIFIES)
            .order_by(IndexResult.growth_percent.desc())
            .all()
        )
        for i, result in enumerate(qualifiers, start=1):
            result.rank = i
        self.db.commit()
        logger.info(f"Ranked {len(qualifiers)} qualifying companies")

    # ─────────────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _find_filing_for_year(filings: list[dict], year: int) -> Optional[dict]:
        """
        Find the most recent accounts filing whose period_of_accounts end date
        falls within the target year.
        """
        for filing in filings:
            period = filing.get("description_values", {})
            made_up_date = (
                period.get("made_up_date")
                or filing.get("description_values", {}).get("period_end")
            )
            if not made_up_date:
                # Try parsing from description
                desc = filing.get("description", "")
                import re
                match = re.search(r"(\d{4})-\d{2}-\d{2}", desc)
                if match:
                    made_up_date = match.group(0)

            if made_up_date and made_up_date.startswith(str(year)):
                return filing

        return None

    @staticmethod
    def _calc_growth(baseline: float, growth: float) -> float:
        if baseline == 0:
            return 0.0
        return ((growth - baseline) / abs(baseline)) * 100

    @staticmethod
    def _extract_region(address: dict) -> Optional[str]:
        """
        Best-effort region extraction from a Companies House address.
        Returns the region, county, or country.
        """
        return (
            address.get("region")
            or address.get("county")
            or address.get("country")
        )
