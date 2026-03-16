"""
Pipeline control API.
Allows triggering and monitoring pipeline runs via HTTP.
This is what Lovable calls to start a pipeline run remotely.
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
from app.config import INDEX_YEAR_ACCOUNT_PERIODS
import os

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])

# Simple in-memory run tracker
# (single instance — Railway runs one container)
_active_run: dict = {}


# ── Auth ──────────────────────────────────────────────────────────────────────

PIPELINE_SECRET = os.getenv("PIPELINE_SECRET", "")


def verify_secret(x_pipeline_secret: str = Header(default="")):
    """
    Basic secret key auth for pipeline endpoints.
    Set PIPELINE_SECRET env var in Railway.
    If not set, all requests are allowed (dev mode).
    """
    if PIPELINE_SECRET and x_pipeline_secret != PIPELINE_SECRET:
        raise HTTPException(status_code=401, detail="Invalid pipeline secret")


# ── Request/Response models ────────────────────────────────────────────────────

class PipelineStartRequest(BaseModel):
    index_year: int
    baseline_year: int
    growth_year: int


class PipelineStatusResponse(BaseModel):
    running: bool
    index_year: Optional[int] = None
    baseline_year: Optional[int] = None
    growth_year: Optional[int] = None
    started_at: Optional[str] = None
    candidates_processed: int = 0
    qualifies: int = 0
    manual_review: int = 0
    does_not_qualify: int = 0
    error: Optional[str] = None


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/run")
def start_pipeline(
    req: PipelineStartRequest,
    _: None = Depends(verify_secret),
):
    """Start a pipeline run in the background."""
    global _active_run

    if _active_run.get("running"):
        raise HTTPException(
            status_code=409,
            detail=f"Pipeline already running for {_active_run.get('index_year')} — wait for it to complete"
        )

    # Register the custom period
    INDEX_YEAR_ACCOUNT_PERIODS[req.index_year] = {
        "baseline_year": req.baseline_year,
        "growth_year": req.growth_year,
    }

    _active_run = {
        "running": True,
        "index_year": req.index_year,
        "baseline_year": req.baseline_year,
        "growth_year": req.growth_year,
        "started_at": datetime.utcnow().isoformat(),
        "candidates_processed": 0,
        "qualifies": 0,
        "manual_review": 0,
        "does_not_qualify": 0,
        "error": None,
    }

    thread = threading.Thread(
        target=_run_pipeline_thread,
        args=(req.index_year, req.baseline_year, req.growth_year),
        daemon=True,
    )
    thread.start()

    return {
        "status": "started",
        "index_year": req.index_year,
        "baseline_year": req.baseline_year,
        "growth_year": req.growth_year,
    }


@router.get("/status", response_model=PipelineStatusResponse)
def get_status(_: None = Depends(verify_secret)):
    """Get the current pipeline run status."""
    return PipelineStatusResponse(**_active_run) if _active_run else PipelineStatusResponse(running=False)


@router.get("/history")
def get_history(limit: int = 10, db: Session = Depends(lambda: next(_get_db()))):
    """Get recent pipeline run history."""
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


@router.get("/config")
def get_config():
    """Return available default year configurations."""
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
        }
    }


# ── Background runner ──────────────────────────────────────────────────────────

def _run_pipeline_thread(index_year: int, baseline_year: int, growth_year: int):
    """Run the pipeline in a background thread, updating _active_run as it goes."""
    global _active_run
    db = SessionLocal()

    try:
        from app.pipeline.pipeline import FastGrowthPipeline

        # Monkey-patch the stats update so we can track progress live
        pipeline = FastGrowthPipeline(db=db, index_year=index_year)

        original_process = pipeline._process_company

        def tracked_process(company_data):
            original_process(company_data)
            _active_run["candidates_processed"] = pipeline.stats.get("candidates", 0)
            _active_run["qualifies"] = pipeline.stats.get("qualifies", 0)
            _active_run["manual_review"] = pipeline.stats.get("manual_review", 0)
            _active_run["does_not_qualify"] = pipeline.stats.get("does_not_qualify", 0)

        pipeline._process_company = tracked_process

        pipeline.run()

        _active_run["running"] = False
        _active_run["candidates_processed"] = pipeline.stats.get("candidates", 0)
        _active_run["qualifies"] = pipeline.stats.get("qualifies", 0)
        _active_run["manual_review"] = pipeline.stats.get("manual_review", 0)
        _active_run["does_not_qualify"] = pipeline.stats.get("does_not_qualify", 0)

        logger.info(f"Pipeline complete: {pipeline.stats}")

    except Exception as e:
        logger.error(f"Pipeline thread failed: {e}")
        _active_run["running"] = False
        _active_run["error"] = str(e)
    finally:
        db.close()


def _get_db():
    from app.models.session import SessionLocal
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
