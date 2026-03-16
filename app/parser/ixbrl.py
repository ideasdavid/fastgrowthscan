"""
iXBRL / XBRL parser for Companies House annual accounts.

Extracts turnover/revenue figures and accounting period dates.
Handles the variety of tag names used across different filing software.
"""
import logging
import re
from dataclasses import dataclass, field
from typing import Optional
from lxml import etree

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Turnover tag names used across different iXBRL filing packages
# Order matters: more specific / more common first
# ─────────────────────────────────────────────────────────────────────────────
TURNOVER_TAGS = [
    # UK GAAP / FRS 102
    "uk-gaap:Turnover",
    "uk-gaap:TurnoverGrossOperatingRevenue",
    "uk-gaap:TurnoverRevenue",
    "uk-core:TurnoverRevenue",
    "uk-core:Turnover",
    # IFRS
    "ifrs-full:Revenue",
    "ifrs-full:RevenueFromContractsWithCustomers",
    # Common namespace variations (we also do a namespace-agnostic fallback)
    "Turnover",
    "TurnoverRevenue",
    "Revenue",
]

# Local name fallback (matches regardless of namespace prefix)
TURNOVER_LOCAL_NAMES = {
    "turnover", "turnoverrevenue", "turnovergrosspoperatingrevenue",
    "revenue", "revenueFromContractsWithCustomers",
}

# Period context tags
PERIOD_TAGS = [
    "xbrli:startDate", "xbrli:endDate",
    "startDate", "endDate",
]


@dataclass
class AccountPeriod:
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    turnover: Optional[float] = None
    context_ref: Optional[str] = None


@dataclass
class ParseResult:
    success: bool
    baseline: Optional[AccountPeriod] = None   # The earlier of the two periods
    growth: Optional[AccountPeriod] = None     # The later of the two periods
    all_periods: list[AccountPeriod] = field(default_factory=list)
    reason: Optional[str] = None               # Why parsing failed / flagged


def parse_accounts(content: bytes) -> ParseResult:
    """
    Main entry point. Parse iXBRL bytes and return up to two accounting periods
    with turnover figures.
    """
    if not content:
        return ParseResult(success=False, reason="No document content")

    try:
        root = _parse_xml(content)
    except Exception as e:
        return ParseResult(success=False, reason=f"XML parse error: {e}")

    # Build a map of context_ref → period dates
    contexts = _extract_contexts(root)

    if not contexts:
        return ParseResult(success=False, reason="No XBRL contexts found — likely PDF or HTML only")

    # Extract all turnover values with their context refs
    turnover_values = _extract_turnover_values(root, contexts)

    if not turnover_values:
        return ParseResult(
            success=False,
            reason="No turnover tags found — company may file abbreviated accounts (no P&L)"
        )

    # Sort periods by end date descending (most recent first)
    periods = sorted(turnover_values, key=lambda p: p.end_date or "", reverse=True)

    if len(periods) >= 2:
        growth = periods[0]    # Most recent = growth year
        baseline = periods[1]  # Prior year = baseline
        return ParseResult(
            success=True,
            baseline=baseline,
            growth=growth,
            all_periods=periods,
        )
    elif len(periods) == 1:
        # Only one year of data in this filing — common in first-year iXBRL filings
        return ParseResult(
            success=False,
            reason="Only one accounting period found in document — need prior year for growth calc",
            all_periods=periods,
        )
    else:
        return ParseResult(success=False, reason="No usable accounting periods extracted")


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_xml(content: bytes) -> etree._Element:
    """Parse bytes as XML/HTML, stripping namespaces for easier querying."""
    try:
        parser = etree.XMLParser(recover=True, remove_comments=True)
        return etree.fromstring(content, parser=parser)
    except Exception:
        # Try as HTML if XML parse fails
        parser = etree.HTMLParser(recover=True)
        return etree.fromstring(content, parser=parser)


def _extract_contexts(root: etree._Element) -> dict:
    """
    Build a dict of context_id → {start_date, end_date} from xbrli:context elements.
    """
    contexts = {}
    ns_map = _get_ns_map(root)

    # Search with and without namespace
    for ctx in root.iter():
        local = etree.QName(ctx.tag).localname if ctx.tag and "{" in ctx.tag else ctx.tag
        if local != "context":
            continue

        ctx_id = ctx.get("id")
        if not ctx_id:
            continue

        period = {}
        for child in ctx.iter():
            child_local = etree.QName(child.tag).localname if child.tag and "{" in child.tag else child.tag
            if child_local == "startDate" and child.text:
                period["start_date"] = child.text.strip()
            elif child_local == "endDate" and child.text:
                period["end_date"] = child.text.strip()

        if "end_date" in period:
            contexts[ctx_id] = period

    return contexts


def _extract_turnover_values(root: etree._Element, contexts: dict) -> list[AccountPeriod]:
    """
    Find all elements whose local name matches a known turnover tag.
    Return one AccountPeriod per unique accounting period.
    """
    found: dict[str, AccountPeriod] = {}  # end_date → AccountPeriod

    for element in root.iter():
        tag = element.tag
        if not tag or not isinstance(tag, str):
            continue

        local = etree.QName(tag).localname if "{" in tag else tag
        if local.lower() not in TURNOVER_LOCAL_NAMES:
            continue

        # Skip if this is a label or reference, not a value
        if element.get("xlink:type") or element.get("{http://www.w3.org/1999/xlink}type"):
            continue

        context_ref = element.get("contextRef")
        if not context_ref or context_ref not in contexts:
            continue

        ctx = contexts[context_ref]
        end_date = ctx.get("end_date")
        if not end_date:
            continue

        # Parse the numeric value
        value = _parse_numeric(element)
        if value is None:
            continue

        # Apply scale factor if present (iXBRL uses decimals attribute)
        decimals = element.get("decimals")
        scale = element.get("scale")
        if scale:
            try:
                value = value * (10 ** int(scale))
            except ValueError:
                pass

        # Keep the largest turnover value per period (handles duplicates)
        if end_date not in found or value > (found[end_date].turnover or 0):
            found[end_date] = AccountPeriod(
                start_date=ctx.get("start_date"),
                end_date=end_date,
                turnover=value,
                context_ref=context_ref,
            )

    return list(found.values())


def _parse_numeric(element: etree._Element) -> Optional[float]:
    """Extract a numeric value from an element, handling formatting."""
    text = element.text
    if not text:
        return None
    text = text.strip().replace(",", "").replace(" ", "")
    # Handle negative values in brackets: (1234) → -1234
    if text.startswith("(") and text.endswith(")"):
        text = "-" + text[1:-1]
    try:
        return float(text)
    except ValueError:
        return None


def _get_ns_map(root: etree._Element) -> dict:
    """Collect all namespace declarations from the document."""
    ns = {}
    for element in root.iter():
        ns.update(element.nsmap or {})
    return ns
