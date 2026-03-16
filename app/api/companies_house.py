"""
Companies House API client.
Handles authentication, rate limiting, and all endpoint calls.
"""
import time
import logging
import requests
from typing import Optional, Generator
from app.config import (
    CH_API_KEY, CH_BASE_URL, CH_DOCUMENT_URL,
    API_RATE_LIMIT_DELAY, REQUEST_TIMEOUT
)

logger = logging.getLogger(__name__)


class CompaniesHouseClient:

    def __init__(self, api_key: str = CH_API_KEY):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.auth = (api_key, "")  # CH uses API key as HTTP Basic username
        self._last_call = 0.0

    def _get(self, url: str, params: dict = None, base: str = CH_BASE_URL) -> dict:
        """Rate-limited GET with error handling."""
        elapsed = time.time() - self._last_call
        if elapsed < API_RATE_LIMIT_DELAY:
            time.sleep(API_RATE_LIMIT_DELAY - elapsed)

        full_url = f"{base}{url}" if not url.startswith("http") else url
        try:
            resp = self.session.get(full_url, params=params, timeout=REQUEST_TIMEOUT)
            self._last_call = time.time()
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return {}
            if e.response.status_code == 429:
                logger.warning("Rate limited — sleeping 60s")
                time.sleep(60)
                return self._get(url, params, base)
            logger.error(f"HTTP error {e.response.status_code} for {full_url}")
            raise
        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise

    # -------------------------------------------------------------------------
    # Company search
    # -------------------------------------------------------------------------

    def search_companies(
        self,
        query: str = "",
        company_type: str = "ltd",
        company_status: str = "active",
        size: int = 100,
        start_index: int = 0
    ) -> dict:
        """
        Advanced company search. Returns raw API response dict.
        """
        params = {
            "company_type": company_type,
            "company_status": company_status,
            "size": size,
            "start_index": start_index,
        }
        if query:
            params["q"] = query
        return self._get("/advanced-search/companies", params=params)

    def iter_all_companies(
        self,
        company_type: str = "ltd",
        company_status: str = "active",
        sic_codes: list[str] = None,
        batch_size: int = 100,
        max_results: int = 50_000,
    ) -> Generator[dict, None, None]:
        """
        Paginate through company search results, yielding individual company dicts.
        Optionally filter by SIC codes (Companies House supports one SIC per search call).
        """
        if sic_codes:
            # CH advanced search supports sic_codes as a filter
            for sic in sic_codes:
                yield from self._iter_search(
                    company_type=company_type,
                    company_status=company_status,
                    sic_codes=[sic],
                    batch_size=batch_size,
                    max_results=max_results,
                )
        else:
            yield from self._iter_search(
                company_type=company_type,
                company_status=company_status,
                batch_size=batch_size,
                max_results=max_results,
            )

    def _iter_search(
        self,
        company_type, company_status, sic_codes=None,
        batch_size=100, max_results=50_000
    ) -> Generator[dict, None, None]:
        start = 0
        seen = 0
        while seen < max_results:
            params = {
                "company_type": company_type,
                "company_status": company_status,
                "size": min(batch_size, max_results - seen),
                "start_index": start,
            }
            if sic_codes:
                params["sic_codes"] = ",".join(sic_codes)

            data = self._get("/advanced-search/companies", params=params)
            items = data.get("items", [])
            if not items:
                break

            for item in items:
                yield item
                seen += 1

            total = data.get("hits", 0)
            start += len(items)
            if start >= total:
                break

        logger.info(f"Search complete — {seen} companies retrieved")

    # -------------------------------------------------------------------------
    # Company profile
    # -------------------------------------------------------------------------

    def get_company(self, company_number: str) -> dict:
        return self._get(f"/company/{company_number}")

    # -------------------------------------------------------------------------
    # Filing history
    # -------------------------------------------------------------------------

    def get_filing_history(
        self,
        company_number: str,
        category: str = "accounts",
        items_per_page: int = 25
    ) -> dict:
        return self._get(
            f"/company/{company_number}/filing-history",
            params={"category": category, "items_per_page": items_per_page}
        )

    def get_accounts_filings(self, company_number: str) -> list[dict]:
        """Return list of accounts filings, newest first."""
        data = self.get_filing_history(company_number, category="accounts")
        return data.get("items", [])

    # -------------------------------------------------------------------------
    # Document retrieval
    # -------------------------------------------------------------------------

    def get_document_metadata(self, document_url: str) -> dict:
        """
        Fetch document metadata from the document API.
        document_url is the 'links.document_metadata' value from a filing item.
        """
        return self._get(document_url, base="")

    def get_pdf_content(self, document_url: str) -> Optional[bytes]:
        """Download the PDF version of a filing document specifically."""
        try:
            meta = self.get_document_metadata(document_url)
            resources = meta.get("resources", {})
            if "application/pdf" not in resources:
                logger.warning(f"No PDF found for {document_url}")
                return None
            # The download URL is the document metadata URL with /content appended
            # and Accept header set to application/pdf
            base_url = document_url if not document_url.endswith("/") else document_url[:-1]
            content_url = f"{base_url}/content"
            elapsed = time.time() - self._last_call
            if elapsed < API_RATE_LIMIT_DELAY:
                time.sleep(API_RATE_LIMIT_DELAY - elapsed)
            resp = self.session.get(
                content_url,
                headers={"Accept": "application/pdf"},
                timeout=REQUEST_TIMEOUT
            )
            self._last_call = time.time()
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            logger.error(f"Failed to download PDF {document_url}: {e}")
            return None

    def get_document_content(self, document_url: str) -> Optional[bytes]:
        """
        Download the actual iXBRL/XML document content.
        Returns raw bytes, or None if unavailable.
        """
        try:
            meta = self.get_document_metadata(document_url)
            resources = meta.get("resources", {})

            # Prefer iXBRL, fall back to XBRL, then XML
            content_url = None
            for fmt in ["application/xhtml+xml", "application/xml", "text/html"]:
                if fmt in resources:
                    content_url = resources[fmt].get("links", {}).get("self")
                    if content_url:
                        break

            if not content_url:
                logger.warning(f"No parseable format found for {document_url}")
                return None

            elapsed = time.time() - self._last_call
            if elapsed < API_RATE_LIMIT_DELAY:
                time.sleep(API_RATE_LIMIT_DELAY - elapsed)

            resp = self.session.get(content_url, timeout=REQUEST_TIMEOUT)
            self._last_call = time.time()
            resp.raise_for_status()
            return resp.content

        except Exception as e:
            logger.error(f"Failed to download document {document_url}: {e}")
            return None
