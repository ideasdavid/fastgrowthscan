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

    thread = threading.Thread(
        target=_run_pipeline_thread,
        args=(req.index_year, req.baseline_year, req.growth_year, sic_codes, req.sector, sector_label),
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


def _run_pipeline_thread(
    index_year: int, baseline_year: int, growth_year: int,
    sic_codes: list, sector: str, sector_label: str
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

        pipeline = FastGrowthPipeline(db=db, index_year=index_year)

        # Store sector on the pipeline run record
        original_process = pipeline._process_company

        def tracked_process(company_data):
            original_process(company_data)
            _active_run["candidates_processed"] = pipeline.stats.get("candidates", 0)
            _active_run["qualifies"] = pipeline.stats.get("qualifies", 0)
            _active_run["manual_review"] = pipeline.stats.get("manual_review", 0)
            _active_run["does_not_qualify"] = pipeline.stats.get("does_not_qualify", 0)

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
