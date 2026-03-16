"""
AI-powered PDF accounts parser.
When iXBRL parsing fails (PDF-only filings), this downloads the PDF
and uses Claude to extract turnover figures.
"""
import base64
import json
import logging
import re
import requests
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# Import Anthropic key from config
from app.config import ANTHROPIC_API_KEY

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"

EXTRACTION_PROMPT = """You are an expert at reading UK company annual accounts.

Extract the turnover (also called revenue or sales) figures from these accounts.
Look for:
- Turnover
- Revenue  
- Total revenue
- Net revenue
- Sales

Return ONLY a JSON array with this exact format, no other text:
[
  {"period_end": "YYYY-MM-DD", "period_start": "YYYY-MM-DD", "turnover": 1234567},
  {"period_end": "YYYY-MM-DD", "period_start": "YYYY-MM-DD", "turnover": 7654321}
]

Include all accounting periods present in the document (usually current year and prior year comparative).
Use null if a value cannot be determined.
If no turnover figure exists at all (e.g. balance sheet only), return an empty array: []
"""


@dataclass
class AIParsedPeriod:
    period_end: Optional[str]
    period_start: Optional[str]
    turnover: Optional[float]


@dataclass 
class AIParseResult:
    success: bool
    periods: list[AIParsedPeriod]
    reason: Optional[str] = None


def parse_pdf_with_ai(pdf_content: bytes) -> AIParseResult:
    """
    Send a PDF to Claude and extract turnover figures.
    Returns AIParseResult with periods found.
    """
    if not pdf_content:
        return AIParseResult(success=False, periods=[], reason="No PDF content")

    if not ANTHROPIC_API_KEY:
        return AIParseResult(success=False, periods=[], reason="No Anthropic API key configured")

    # Encode PDF as base64
    pdf_b64 = base64.standard_b64encode(pdf_content).decode("utf-8")

    payload = {
        "model": "claude-opus-4-5",
        "max_tokens": 1024,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "document",
                        "source": {
                            "type": "base64",
                            "media_type": "application/pdf",
                            "data": pdf_b64,
                        },
                    },
                    {
                        "type": "text",
                        "text": EXTRACTION_PROMPT,
                    },
                ],
            }
        ],
    }

    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    import time
    time.sleep(2)  # Anthropic rate limit buffer

    try:
        resp = requests.post(
            ANTHROPIC_API_URL,
            json=payload,
            headers=headers,
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()

        raw_text = data["content"][0]["text"].strip()
        logger.info(f"AI extraction response: {raw_text[:200]}")

        # Parse JSON response
        periods = _parse_ai_response(raw_text)

        if periods is None:
            return AIParseResult(
                success=False,
                periods=[],
                reason="AI response could not be parsed as JSON"
            )

        if len(periods) == 0:
            return AIParseResult(
                success=False,
                periods=[],
                reason="AI found no turnover figures in document (likely balance sheet only)"
            )

        return AIParseResult(success=True, periods=periods)

    except requests.HTTPError as e:
        logger.error(f"Anthropic API HTTP error: {e}")
        return AIParseResult(success=False, periods=[], reason=f"API error: {e}")
    except Exception as e:
        logger.error(f"AI PDF parse failed: {e}")
        return AIParseResult(success=False, periods=[], reason=str(e))


def _parse_ai_response(text: str) -> Optional[list[AIParsedPeriod]]:
    """Extract JSON array from AI response text."""
    # Strip markdown code blocks if present
    text = re.sub(r"```json\s*", "", text)
    text = re.sub(r"```\s*", "", text)
    text = text.strip()

    try:
        raw = json.loads(text)
        if not isinstance(raw, list):
            return None

        periods = []
        for item in raw:
            turnover = item.get("turnover")
            if turnover is not None:
                try:
                    turnover = float(turnover)
                except (ValueError, TypeError):
                    turnover = None

            periods.append(AIParsedPeriod(
                period_end=item.get("period_end"),
                period_start=item.get("period_start"),
                turnover=turnover,
            ))

        return periods

    except json.JSONDecodeError:
        # Try to find a JSON array within the text
        match = re.search(r"\[.*?\]", text, re.DOTALL)
        if match:
            try:
                return _parse_ai_response(match.group(0))
            except Exception:
                pass
        return None
