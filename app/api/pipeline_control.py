"""
Pipeline control API.
Allows triggering and monitoring pipeline runs via HTTP from Lovable.
"""
import threading
import logging
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Header
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.models.session import SessionLocal
from app.models.db import PipelineRun, IndexResult, ResultStatus
from app.config import INDEX_YEAR_ACCOUNT_PERIODS, SECTOR_GROUPS
import os

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])

_active_run: dict = {}

PIPELINE_SECRET = os.getenv("PIPELINE_SECRET", "")


def verify_secret(x_pipeline_secret: str = Header(default="")):
    if PIPELINE_SECRET and x_pipeline_secret != PIPELINE_SECRET:
        raise HTTPException(status_code=401, detail="Invalid pipeline secret")


class PipelineStartRequest(BaseModel):
    index_year: int
    baseline_year: int
    growth_year: int
    sector: Optional[str] = None  # None = all sectors
    candidate_source: Optional[str] = "auto"  # "api", "bulk", or "auto"


class PipelineStatusResponse(BaseModel):
    running: bool
    index_year: Optional[int] = None
    baseline_year: Optional[int] = None
    growth_year: Optional[int] = None
    sector: Optional[str] = None
    sector_label: Optional[str] = None
    started_at: Optional[str] = None
    candidates_processed: int = 0
    qualifies: int = 0
    manual_review: int = 0
    does_not_qualify: int = 0
    error: Optional[str] = None
    current_tier: Optional[str] = None
    tier_candidates: int = 0


@router.post("/run")
def start_pipeline(
    req: PipelineStartRequest,
    _: None = Depends(verify_secret),
):
    global _active_run

    if _active_run.get("running"):
        raise HTTPException(
            status_code=409,
            detail=f"Pipeline already running for year {_active_run.get('index_year')} — wait for it to complete"
        )

    # Validate sector if provided
    sector_label = None
    sic_codes = None
    if req.sector:
        if req.sector not in SECTOR_GROUPS:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown sector '{req.sector}'. Valid sectors: {list(SECTOR_GROUPS.keys())}"
            )
        sector_label = SECTOR_GROUPS[req.sector]["label"]
        sic_codes = SECTOR_GROUPS[req.sector]["sic_codes"]

    INDEX_YEAR_ACCOUNT_PERIODS[req.index_year] = {
        "baseline_year": req.baseline_year,
        "growth_year": req.growth_year,
    }

    _active_run = {
        "running": True,
        "index_year": req.index_year,
        "baseline_year": req.baseline_year,
        "growth_year": req.growth_year,
        "sector": req.sector,
        "sector_label": sector_label,
        "started_at": datetime.utcnow().isoformat(),
        "candidates_processed": 0,
        "qualifies": 0,
        "manual_review": 0,
        "does_not_qualify": 0,
        "error": None,
    }

    candidate_source = req.candidate_source or "auto"

    thread = threading.Thread(
        target=_run_pipeline_thread,
        args=(req.index_year, req.baseline_year, req.growth_year, sic_codes, req.sector, sector_label, candidate_source),
        daemon=True,
    )
    thread.start()

    return {
        "status": "started",
        "index_year": req.index_year,
        "baseline_year": req.baseline_year,
        "growth_year": req.growth_year,
        "sector": req.sector,
        "sector_label": sector_label,
        "candidate_source": candidate_source,
    }


@router.get("/status", response_model=PipelineStatusResponse)
def get_status(_: None = Depends(verify_secret)):
    return PipelineStatusResponse(**_active_run) if _active_run else PipelineStatusResponse(running=False)


@router.get("/history")
def get_history(limit: int = 20):
    db = SessionLocal()
    try:
        runs = (
            db.query(PipelineRun)
            .order_by(PipelineRun.started_at.desc())
            .limit(limit)
            .all()
        )
        return [
            {
                "id": r.id,
                "index_year": r.index_year,
                "baseline_year": r.baseline_year,
                "growth_year": r.growth_year,
                "sector": r.sector,
                "sector_label": r.sector_label,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "completed_at": r.completed_at.isoformat() if r.completed_at else None,
                "candidates_found": r.candidates_found,
                "qualifies_count": r.qualifies_count,
                "manual_review_count": r.manual_review_count,
                "does_not_qualify_count": r.does_not_qualify_count,
                "notes": r.notes,
            }
            for r in runs
        ]
    finally:
        db.close()


@router.get("/sectors")
def get_sectors():
    """Return all available sector groups with completion status."""
    db = SessionLocal()
    try:
        # Find which sectors have been run for each index year
        completed = (
            db.query(PipelineRun.sector, PipelineRun.index_year, PipelineRun.completed_at)
            .filter(PipelineRun.completed_at.isnot(None))
            .filter(PipelineRun.sector.isnot(None))
            .all()
        )
        completed_set = {(r.sector, r.index_year) for r in completed}

        return {
            "sectors": {
                key: {
                    "label": val["label"],
                    "sic_code_count": len(val["sic_codes"]),
                }
                for key, val in SECTOR_GROUPS.items()
            },
            "completed_runs": [
                {"sector": r.sector, "index_year": r.index_year}
                for r in completed
            ]
        }
    finally:
        db.close()


@router.post("/reprocess-pdf")
def start_pdf_reprocess(
    index_year: int,
    limit: Optional[int] = None,
    _: None = Depends(verify_secret),
):
    """Re-process MANUAL_REVIEW companies using the AI PDF parser."""
    global _active_run

    if _active_run.get("running"):
        raise HTTPException(
            status_code=409,
            detail="Pipeline already running — wait for it to complete"
        )

    _active_run = {
        "running": True,
        "index_year": index_year,
        "baseline_year": None,
        "growth_year": None,
        "sector": "pdf_reprocess",
        "sector_label": "AI PDF Reprocessing",
        "started_at": datetime.utcnow().isoformat(),
        "candidates_processed": 0,
        "qualifies": 0,
        "manual_review": 0,
        "does_not_qualify": 0,
        "error": None,
    }

    thread = threading.Thread(
        target=_run_pdf_reprocess_thread,
        args=(index_year, limit),
        daemon=True,
    )
    thread.start()

    return {"status": "started", "index_year": index_year, "mode": "pdf_reprocess"}


@router.get("/config")
def get_config():
    return {
        "default_periods": {
            str(year): {
                "baseline_year": periods["baseline_year"],
                "growth_year": periods["growth_year"],
                "label": (
                    f"FY{periods['baseline_year']-1}/{str(periods['baseline_year'])[-2:]} "
                    f"vs FY{periods['growth_year']-1}/{str(periods['growth_year'])[-2:]}"
                ),
            }
            for year, periods in INDEX_YEAR_ACCOUNT_PERIODS.items()
        },
        "sectors": {
            key: val["label"]
            for key, val in SECTOR_GROUPS.items()
        }
    }


def _run_pdf_reprocess_thread(index_year: int, limit: int = None):
    """Run the AI PDF reprocessing in a background thread."""
    global _active_run
    db = SessionLocal()
    try:
        from app.models.db import IndexResult, ResultStatus
        from app.api.companies_house import CompaniesHouseClient
        from app.parser.pdf_ai import parse_pdf_with_ai
        from app.config import MIN_TURNOVER_GBP, MIN_GROWTH_PERCENT, INDEX_YEAR_ACCOUNT_PERIODS
        import re, time

        periods = INDEX_YEAR_ACCOUNT_PERIODS.get(index_year, {})
        baseline_year = periods.get("baseline_year")
        growth_year = periods.get("growth_year")

        q = db.query(IndexResult).filter_by(
            index_year=index_year, status=ResultStatus.MANUAL_REVIEW
        ).order_by(IndexResult.id)
        if limit:
            q = q.limit(limit)

        results = q.all()
        total = len(results)
        client = CompaniesHouseClient()
        converted_q = 0
        converted_dnq = 0
        still_manual = 0

        for i, result in enumerate(results):
            try:
                company_number = result.company_number.zfill(8)
                filings = client.get_accounts_filings(company_number)
                if not filings:
                    still_manual += 1
                    continue

                growth_filing = baseline_filing = None
                for f in filings:
                    made_up_date = f.get("description_values", {}).get("made_up_date", "")
                    if not made_up_date:
                        match = re.search(r"(\d{4})-\d{2}-\d{2}", f.get("description", ""))
                        if match:
                            made_up_date = match.group(0)
                    if made_up_date.startswith(str(growth_year)):
                        growth_filing = f
                    elif made_up_date.startswith(str(baseline_year)):
                        baseline_filing = f

                if not growth_filing or not baseline_filing:
                    still_manual += 1
                    continue

                doc_url = growth_filing.get("links", {}).get("document_metadata")
                baseline_doc_url = baseline_filing.get("links", {}).get("document_metadata")

                if not doc_url:
                    still_manual += 1
                    continue

                content = client.get_pdf_content(doc_url)
                if not content:
                    still_manual += 1
                    continue

                ai = parse_pdf_with_ai(content)
                if ai.success and len(ai.periods) < 2 and baseline_doc_url:
                    bc = client.get_pdf_content(baseline_doc_url)
                    if bc:
                        ai_base = parse_pdf_with_ai(bc)
                        if ai_base.success:
                            ai.periods = ai.periods + ai_base.periods
                            ai.success = True

                if not ai.success or len(ai.periods) < 2:
                    still_manual += 1
                    continue

                valid = [p for p in ai.periods if p.period_end and p.turnover is not None]
                if len(valid) < 2:
                    still_manual += 1
                    continue

                valid.sort(key=lambda p: p.period_end, reverse=True)
                gp, bp = valid[0], valid[1]

                if int(gp.period_end[:4]) != growth_year or int(bp.period_end[:4]) != baseline_year:
                    still_manual += 1
                    continue

                growth_pct = ((gp.turnover - bp.turnover) / abs(bp.turnover)) * 100 if bp.turnover else 0

                result.baseline_period_start = bp.period_start
                result.baseline_period_end = bp.period_end
                result.baseline_turnover = bp.turnover
                result.growth_period_start = gp.period_start
                result.growth_period_end = gp.period_end
                result.growth_turnover = gp.turnover
                result.growth_percent = growth_pct
                result.manual_review_reason = None

                if bp.turnover >= MIN_TURNOVER_GBP and growth_pct >= MIN_GROWTH_PERCENT:
                    result.status = ResultStatus.QUALIFIES
                    converted_q += 1
                else:
                    result.status = ResultStatus.DOES_NOT_QUALIFY
                    converted_dnq += 1

                db.commit()

            except Exception as e:
                logger.error(f"PDF reprocess error for {result.company_number}: {e}")
                db.rollback()
                still_manual += 1

            _active_run["candidates_processed"] = i + 1
            _active_run["qualifies"] = converted_q
            _active_run["does_not_qualify"] = converted_dnq
            _active_run["manual_review"] = still_manual
            time.sleep(0.5)

        # Re-rank qualifiers
        qualifiers = db.query(IndexResult).filter_by(
            index_year=index_year, status=ResultStatus.QUALIFIES
        ).order_by(IndexResult.growth_percent.desc()).all()
        for rank, r in enumerate(qualifiers, 1):
            r.rank = rank
        db.commit()

        _active_run["running"] = False
        logger.info(f"PDF reprocess complete: {converted_q} qualifies, {converted_dnq} DNQ, {still_manual} still manual")

    except Exception as e:
        logger.error(f"PDF reprocess thread failed: {e}")
        _active_run["running"] = False
        _active_run["error"] = str(e)
    finally:
        db.close()


def _run_pipeline_thread(
    index_year: int, baseline_year: int, growth_year: int,
    sic_codes: list, sector: str, sector_label: str,
    candidate_source: str = "auto",
):
    global _active_run
    db = SessionLocal()

    try:
        from app.pipeline.pipeline import FastGrowthPipeline
        from app.config import INCLUDED_SIC_CODES

        # Temporarily override SIC codes for this run
        import app.config as cfg
        original_sic = cfg.INCLUDED_SIC_CODES
        if sic_codes:
            cfg.INCLUDED_SIC_CODES = sic_codes

        pipeline = FastGrowthPipeline(
            db=db, index_year=index_year, candidate_source=candidate_source
        )

        # Store sector on the pipeline run record
        original_process = pipeline._process_company

        def tracked_process(company_data):
            original_process(company_data)
            _active_run["candidates_processed"] = pipeline.stats.get("candidates", 0)
            _active_run["qualifies"] = pipeline.stats.get("qualifies", 0)
            _active_run["manual_review"] = pipeline.stats.get("manual_review", 0)
            _active_run["does_not_qualify"] = pipeline.stats.get("does_not_qualify", 0)
            _active_run["current_tier"] = pipeline.stats.get("current_tier")
            _active_run["tier_candidates"] = pipeline.stats.get("tier_candidates", 0)

        pipeline._process_company = tracked_process
        pipeline.run()

        # Update pipeline run with sector info
        run = (
            db.query(PipelineRun)
            .filter_by(index_year=index_year)
            .order_by(PipelineRun.started_at.desc())
            .first()
        )
        if run:
            run.sector = sector
            run.sector_label = sector_label
            db.commit()

        cfg.INCLUDED_SIC_CODES = original_sic

        _active_run["running"] = False
        _active_run["candidates_processed"] = pipeline.stats.get("candidates", 0)
        _active_run["qualifies"] = pipeline.stats.get("qualifies", 0)
        _active_run["manual_review"] = pipeline.stats.get("manual_review", 0)
        _active_run["does_not_qualify"] = pipeline.stats.get("does_not_qualify", 0)

    except Exception as e:
        logger.error(f"Pipeline thread failed: {e}")
        _active_run["running"] = False
        _active_run["error"] = str(e)
    finally:
        db.close()


# ── AI PDF Reprocessing ────────────────────────────────────────────────────────

# Separate tracker for the AI reprocess job
_active_reprocess: dict = {}


class ReprocessRequest(BaseModel):
    index_year: int
    limit: Optional[int] = None  # None = all manual reviews


class ReprocessStatusResponse(BaseModel):
    running: bool
    index_year: Optional[int] = None
    total: int = 0
    processed: int = 0
    converted_qualifies: int = 0
    converted_dnq: int = 0
    still_manual: int = 0
    error: Optional[str] = None


@router.post("/reprocess")
def start_reprocess(
    req: ReprocessRequest,
    _: None = Depends(verify_secret),
):
    """Start the AI PDF reprocessing job for MANUAL_REVIEW companies."""
    global _active_reprocess

    if _active_reprocess.get("running"):
        raise HTTPException(
            status_code=409,
            detail="AI reprocessing already running — wait for it to complete"
        )

    if _active_run.get("running"):
        raise HTTPException(
            status_code=409,
            detail="Main pipeline is running — wait for it to complete first"
        )

    from app.config import ANTHROPIC_API_KEY
    if not ANTHROPIC_API_KEY:
        raise HTTPException(
            status_code=400,
            detail="ANTHROPIC_API_KEY not configured on server"
        )

    # Count how many manual reviews exist
    db = SessionLocal()
    try:
        q = db.query(IndexResult).filter_by(
            index_year=req.index_year,
            status=ResultStatus.MANUAL_REVIEW
        )
        total = q.count()
    finally:
        db.close()

    if total == 0:
        return {"status": "nothing_to_do", "message": f"No MANUAL_REVIEW companies for {req.index_year}"}

    _active_reprocess = {
        "running": True,
        "index_year": req.index_year,
        "total": min(total, req.limit) if req.limit else total,
        "processed": 0,
        "converted_qualifies": 0,
        "converted_dnq": 0,
        "still_manual": 0,
        "error": None,
    }

    thread = threading.Thread(
        target=_run_reprocess_thread,
        args=(req.index_year, req.limit),
        daemon=True,
    )
    thread.start()

    return {
        "status": "started",
        "index_year": req.index_year,
        "total_to_process": _active_reprocess["total"],
    }


@router.get("/reprocess/status", response_model=ReprocessStatusResponse)
def get_reprocess_status(_: None = Depends(verify_secret)):
    """Get AI reprocessing job status."""
    return ReprocessStatusResponse(**_active_reprocess) if _active_reprocess else ReprocessStatusResponse(running=False)


@router.get("/reprocess/count")
def get_reprocess_count(index_year: int):
    """How many MANUAL_REVIEW companies are waiting for AI processing."""
    db = SessionLocal()
    try:
        count = db.query(IndexResult).filter_by(
            index_year=index_year,
            status=ResultStatus.MANUAL_REVIEW
        ).count()
        return {"index_year": index_year, "manual_review_count": count}
    finally:
        db.close()


def _run_reprocess_thread(index_year: int, limit: Optional[int]):
    """Run AI PDF reprocessing in background thread."""
    global _active_reprocess
    import time
    import re

    db = SessionLocal()
    try:
        from app.api.companies_house import CompaniesHouseClient
        from app.parser.pdf_ai import parse_pdf_with_ai
        from app.config import MIN_TURNOVER_GBP, MIN_GROWTH_PERCENT, INDEX_YEAR_ACCOUNT_PERIODS

        periods = INDEX_YEAR_ACCOUNT_PERIODS.get(index_year, {})
        baseline_year = periods.get("baseline_year")
        growth_year = periods.get("growth_year")

        if not baseline_year or not growth_year:
            _active_reprocess["running"] = False
            _active_reprocess["error"] = f"No period config for {index_year}"
            return

        client = CompaniesHouseClient()

        q = db.query(IndexResult).filter_by(
            index_year=index_year,
            status=ResultStatus.MANUAL_REVIEW
        ).order_by(IndexResult.id)

        if limit:
            q = q.limit(limit)

        results = q.all()
        stats = {"converted_qualifies": 0, "converted_dnq": 0, "still_manual": 0}

        for i, result in enumerate(results):
            company_number = result.company_number.zfill(8)

            try:
                filings = client.get_accounts_filings(company_number)
                if not filings:
                    stats["still_manual"] += 1
                    _active_reprocess["processed"] = i + 1
                    continue

                growth_filing = None
                baseline_filing = None
                for f in filings:
                    period = f.get("description_values", {})
                    made_up_date = period.get("made_up_date", "")
                    if not made_up_date:
                        match = re.search(r"(\d{4})-\d{2}-\d{2}", f.get("description", ""))
                        if match:
                            made_up_date = match.group(0)
                    if made_up_date.startswith(str(growth_year)):
                        growth_filing = f
                    elif made_up_date.startswith(str(baseline_year)):
                        baseline_filing = f

                if not growth_filing or not baseline_filing:
                    stats["still_manual"] += 1
                    _active_reprocess["processed"] = i + 1
                    continue

                doc_url = growth_filing.get("links", {}).get("document_metadata")
                baseline_doc_url = baseline_filing.get("links", {}).get("document_metadata")

                if not doc_url:
                    stats["still_manual"] += 1
                    _active_reprocess["processed"] = i + 1
                    continue

                content = client.get_pdf_content(doc_url)
                if not content:
                    stats["still_manual"] += 1
                    _active_reprocess["processed"] = i + 1
                    continue

                ai = parse_pdf_with_ai(content)

                if ai.success and len(ai.periods) < 2 and baseline_doc_url:
                    baseline_content = client.get_pdf_content(baseline_doc_url)
                    if baseline_content:
                        ai_base = parse_pdf_with_ai(baseline_content)
                        if ai_base.success:
                            ai.periods = ai.periods + ai_base.periods
                            ai.success = True

                if not ai.success or len(ai.periods) < 2:
                    stats["still_manual"] += 1
                    _active_reprocess["processed"] = i + 1
                    continue

                valid = [p for p in ai.periods if p.period_end and p.turnover is not None]
                if len(valid) < 2:
                    stats["still_manual"] += 1
                    _active_reprocess["processed"] = i + 1
                    continue

                valid.sort(key=lambda p: p.period_end, reverse=True)
                growth_p = valid[0]
                baseline_p = valid[1]

                if (int(growth_p.period_end[:4]) != growth_year or
                        int(baseline_p.period_end[:4]) != baseline_year):
                    stats["still_manual"] += 1
                    _active_reprocess["processed"] = i + 1
                    continue

                growth_pct = ((growth_p.turnover - baseline_p.turnover) / abs(baseline_p.turnover)) * 100

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
                    logger.info(f"✓ QUALIFIES via AI: {company_number} ({growth_pct:.1f}%)")
                else:
                    result.status = ResultStatus.DOES_NOT_QUALIFY
                    stats["converted_dnq"] += 1

                db.commit()

            except Exception as e:
                logger.error(f"Reprocess error for {company_number}: {e}")
                db.rollback()
                stats["still_manual"] += 1

            _active_reprocess["processed"] = i + 1
            _active_reprocess["converted_qualifies"] = stats["converted_qualifies"]
            _active_reprocess["converted_dnq"] = stats["converted_dnq"]
            _active_reprocess["still_manual"] = stats["still_manual"]

            time.sleep(0.5)

        # Re-rank qualifiers
        qualifiers = (
            db.query(IndexResult)
            .filter_by(index_year=index_year, status=ResultStatus.QUALIFIES)
            .order_by(IndexResult.growth_percent.desc())
            .all()
        )
        for rank, r in enumerate(qualifiers, 1):
            r.rank = rank
        db.commit()

        logger.info(f"Reprocess complete: {stats}")

    except Exception as e:
        logger.error(f"Reprocess thread failed: {e}")
        _active_reprocess["error"] = str(e)
    finally:
        _active_reprocess["running"] = False
        db.close()


# ── Bulk Data Explorer ────────────────────────────────────────────────────────


@router.get("/bulk/explore")
def explore_bulk_data(
    search: Optional[str] = None,
    company_status: Optional[str] = None,
    company_type: Optional[str] = None,
    account_category: Optional[str] = None,
    sic_code: Optional[str] = None,
    postcode: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
):
    """Query the bulk company snapshot with filters and pagination."""
    from app.models.db import BulkCompanySnapshot
    from sqlalchemy import or_

    db = SessionLocal()
    try:
        query = db.query(BulkCompanySnapshot)

        if search:
            query = query.filter(BulkCompanySnapshot.company_name.ilike(f"%{search}%"))
        if company_status:
            query = query.filter(BulkCompanySnapshot.company_status == company_status)
        if company_type:
            query = query.filter(BulkCompanySnapshot.company_type == company_type)
        if account_category:
            query = query.filter(BulkCompanySnapshot.account_category == account_category)
        if sic_code:
            query = query.filter(
                or_(
                    BulkCompanySnapshot.sic_code_1.like(f"{sic_code}%"),
                    BulkCompanySnapshot.sic_code_2.like(f"{sic_code}%"),
                    BulkCompanySnapshot.sic_code_3.like(f"{sic_code}%"),
                    BulkCompanySnapshot.sic_code_4.like(f"{sic_code}%"),
                )
            )
        if postcode:
            query = query.filter(BulkCompanySnapshot.postcode.ilike(f"{postcode}%"))

        total = query.count()
        rows = query.order_by(BulkCompanySnapshot.company_name).offset(offset).limit(limit).all()

        return {
            "total": total,
            "offset": offset,
            "limit": limit,
            "results": [
                {
                    "company_number": r.company_number,
                    "company_name": r.company_name,
                    "company_status": r.company_status,
                    "company_type": r.company_type,
                    "incorporation_date": r.incorporation_date,
                    "account_category": r.account_category,
                    "sic_codes": [c for c in [r.sic_code_1, r.sic_code_2, r.sic_code_3, r.sic_code_4] if c],
                    "postcode": r.postcode,
                }
                for r in rows
            ],
        }
    finally:
        db.close()


@router.get("/bulk/filters")
def get_bulk_filters():
    """Return distinct values for bulk data filter dropdowns."""
    from app.models.db import BulkCompanySnapshot

    db = SessionLocal()
    try:
        statuses = [r[0] for r in db.query(BulkCompanySnapshot.company_status).distinct().all() if r[0]]
        types = [r[0] for r in db.query(BulkCompanySnapshot.company_type).distinct().all() if r[0]]
        categories = [r[0] for r in db.query(BulkCompanySnapshot.account_category).distinct().all() if r[0]]

        return {
            "statuses": sorted(statuses),
            "company_types": sorted(types),
            "account_categories": sorted(categories),
        }
    finally:
        db.close()


# ── Bulk Data Refresh ─────────────────────────────────────────────────────────

_active_bulk_refresh: dict = {}


class BulkRefreshRequest(BaseModel):
    url: Optional[str] = None  # Optional direct URL override


class BulkStatusResponse(BaseModel):
    refreshing: bool = False
    started_at: Optional[str] = None
    total_rows: Optional[int] = None
    error: Optional[str] = None
    # Latest snapshot info
    snapshot_date: Optional[str] = None
    ingested_at: Optional[str] = None
    rows_in_table: Optional[int] = None


@router.post("/bulk/refresh")
def start_bulk_refresh(
    req: BulkRefreshRequest = BulkRefreshRequest(),
    _: None = Depends(verify_secret),
):
    """Download and ingest the latest Companies House bulk data snapshot."""
    global _active_bulk_refresh

    if _active_bulk_refresh.get("refreshing"):
        raise HTTPException(
            status_code=409,
            detail="Bulk data refresh already in progress"
        )

    if _active_run.get("running"):
        raise HTTPException(
            status_code=409,
            detail="Pipeline is running — wait for it to complete before refreshing bulk data"
        )

    _active_bulk_refresh = {
        "refreshing": True,
        "started_at": datetime.utcnow().isoformat(),
        "total_rows": None,
        "error": None,
    }

    thread = threading.Thread(
        target=_run_bulk_refresh_thread,
        args=(req.url,),
        daemon=True,
    )
    thread.start()

    return {"status": "started"}


@router.get("/bulk/status", response_model=BulkStatusResponse)
def get_bulk_status(_: None = Depends(verify_secret)):
    """Get bulk data refresh status and latest snapshot info."""
    db = SessionLocal()
    try:
        from app.pipeline.bulk_data import BulkDataManager
        manager = BulkDataManager(db)
        try:
            info = manager.get_snapshot_info()
        except Exception as e:
            logger.warning(f"Failed to get snapshot info: {e}")
            db.rollback()
            info = None

        resp = {
            "refreshing": _active_bulk_refresh.get("refreshing", False),
            "started_at": _active_bulk_refresh.get("started_at"),
            "total_rows": _active_bulk_refresh.get("total_rows"),
            "error": _active_bulk_refresh.get("error"),
        }
        if info:
            resp["snapshot_date"] = info.get("snapshot_date")
            resp["ingested_at"] = info.get("ingested_at")
            resp["rows_in_table"] = info.get("rows_in_table")

        return BulkStatusResponse(**resp)
    finally:
        db.close()


def _run_bulk_refresh_thread(url: str = None):
    """Run bulk data download and ingestion in a background thread."""
    global _active_bulk_refresh
    db = SessionLocal()

    try:
        from app.pipeline.bulk_data import BulkDataManager
        manager = BulkDataManager(db)
        stats = manager.refresh(url=url)

        _active_bulk_refresh["total_rows"] = stats.get("total_rows")
        _active_bulk_refresh["refreshing"] = False
        logger.info(f"Bulk refresh thread complete: {stats}")

    except Exception as e:
        logger.error(f"Bulk refresh thread failed: {e}")
        _active_bulk_refresh["error"] = str(e)
        _active_bulk_refresh["refreshing"] = False
    finally:
        db.close()
