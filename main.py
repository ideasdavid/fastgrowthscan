"""
FastAPI backend — serves results to the dashboard and handles CSV export.
"""
import csv
import io
import json
from typing import Optional
from fastapi import FastAPI, Depends, Query
from fastapi.responses import StreamingResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func
import os

from app.models.db import Company, IndexResult, ResultStatus, PipelineRun
from app.models.session import get_db, init_db

app = FastAPI(title="Fast Growth Index", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def on_startup():
    init_db()


# Serve the React dashboard
FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "frontend", "public")
if os.path.exists(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


@app.get("/", include_in_schema=False)
def serve_dashboard():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))


# ─────────────────────────────────────────────────────────────────────────────
# Results endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/results")
def get_results(
    index_year: int = Query(...),
    status: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    sic_code: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    sort_by: str = Query("rank"),
    sort_dir: str = Query("asc"),
    limit: int = Query(100),
    offset: int = Query(0),
    db: Session = Depends(get_db),
):
    q = (
        db.query(IndexResult, Company)
        .join(Company, IndexResult.company_number == Company.company_number)
        .filter(IndexResult.index_year == index_year)
    )

    if status:
        q = q.filter(IndexResult.status == status)
    if region:
        q = q.filter(Company.region == region)
    if sic_code:
        q = q.filter(Company.sic_codes.contains(sic_code))
    if search:
        q = q.filter(Company.company_name.ilike(f"%{search}%"))

    # Sorting
    sort_col = {
        "rank": IndexResult.rank,
        "growth_percent": IndexResult.growth_percent,
        "growth_turnover": IndexResult.growth_turnover,
        "baseline_turnover": IndexResult.baseline_turnover,
        "company_name": Company.company_name,
        "region": Company.region,
    }.get(sort_by, IndexResult.rank)

    if sort_dir == "desc":
        q = q.order_by(sort_col.desc().nullslast())
    else:
        q = q.order_by(sort_col.asc().nullsfirst())

    total = q.count()
    rows = q.offset(offset).limit(limit).all()

    return {
        "total": total,
        "results": [_format_result(r, c) for r, c in rows],
    }


@app.get("/api/results/summary")
def get_summary(index_year: int = Query(...), db: Session = Depends(get_db)):
    counts = (
        db.query(IndexResult.status, func.count(IndexResult.id))
        .filter(IndexResult.index_year == index_year)
        .group_by(IndexResult.status)
        .all()
    )
    summary = {s.value: c for s, c in counts}

    top = (
        db.query(IndexResult, Company)
        .join(Company)
        .filter(
            IndexResult.index_year == index_year,
            IndexResult.status == ResultStatus.QUALIFIES,
        )
        .order_by(IndexResult.rank)
        .limit(10)
        .all()
    )

    return {
        "index_year": index_year,
        "counts": summary,
        "top_10": [_format_result(r, c) for r, c in top],
    }


@app.get("/api/years")
def get_available_years(db: Session = Depends(get_db)):
    years = (
        db.query(IndexResult.index_year)
        .distinct()
        .order_by(IndexResult.index_year.desc())
        .all()
    )
    return [y[0] for y in years]


@app.get("/api/regions")
def get_regions(index_year: int = Query(...), db: Session = Depends(get_db)):
    regions = (
        db.query(Company.region, func.count(IndexResult.id))
        .join(IndexResult, Company.company_number == IndexResult.company_number)
        .filter(IndexResult.index_year == index_year)
        .filter(Company.region.isnot(None))
        .group_by(Company.region)
        .order_by(func.count(IndexResult.id).desc())
        .all()
    )
    return [{"region": r, "count": c} for r, c in regions]


@app.get("/api/pipeline-runs")
def get_pipeline_runs(db: Session = Depends(get_db)):
    runs = db.query(PipelineRun).order_by(PipelineRun.started_at.desc()).limit(20).all()
    return [
        {
            "id": r.id,
            "index_year": r.index_year,
            "started_at": r.started_at.isoformat() if r.started_at else None,
            "completed_at": r.completed_at.isoformat() if r.completed_at else None,
            "candidates_found": r.candidates_found,
            "qualifies_count": r.qualifies_count,
            "manual_review_count": r.manual_review_count,
            "notes": r.notes,
        }
        for r in runs
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Export
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/export/csv")
def export_csv(
    index_year: int = Query(...),
    status: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    q = (
        db.query(IndexResult, Company)
        .join(Company)
        .filter(IndexResult.index_year == index_year)
    )
    if status:
        q = q.filter(IndexResult.status == status)
    q = q.order_by(IndexResult.rank.asc().nullslast(), IndexResult.growth_percent.desc().nullslast())

    rows = q.all()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Rank", "Company Name", "Company Number", "Status",
        "Region", "SIC Codes",
        "Baseline Period", "Baseline Turnover (£)",
        "Growth Period", "Growth Turnover (£)",
        "Growth %",
        "Manual Review Reason",
        "Companies House URL",
    ])

    for result, company in rows:
        sic = ", ".join(json.loads(company.sic_codes or "[]"))
        writer.writerow([
            result.rank or "",
            company.company_name,
            company.company_number,
            result.status.value,
            company.region or "",
            sic,
            f"{result.baseline_period_start} to {result.baseline_period_end}",
            f"{result.baseline_turnover:,.0f}" if result.baseline_turnover else "",
            f"{result.growth_period_start} to {result.growth_period_end}",
            f"{result.growth_turnover:,.0f}" if result.growth_turnover else "",
            f"{result.growth_percent:.1f}%" if result.growth_percent else "",
            result.manual_review_reason or "",
            f"https://find-and-update.company-information.service.gov.uk/company/{company.company_number}",
        ])

    output.seek(0)
    filename = f"fast-growth-index-{index_year}-{status or 'all'}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _format_result(result: IndexResult, company: Company) -> dict:
    sic = []
    try:
        sic = json.loads(company.sic_codes or "[]")
    except Exception:
        pass

    return {
        "rank": result.rank,
        "company_number": company.company_number,
        "company_name": company.company_name,
        "status": result.status.value,
        "region": company.region,
        "sic_codes": sic,
        "incorporated_date": company.incorporated_date,
        "baseline_period": {
            "start": result.baseline_period_start,
            "end": result.baseline_period_end,
            "turnover": result.baseline_turnover,
        },
        "growth_period": {
            "start": result.growth_period_start,
            "end": result.growth_period_end,
            "turnover": result.growth_turnover,
        },
        "growth_percent": result.growth_percent,
        "manual_review_reason": result.manual_review_reason,
        "companies_house_url": (
            f"https://find-and-update.company-information.service.gov.uk/company/{company.company_number}"
        ),
        "baseline_filing_url": result.baseline_document_url,
        "growth_filing_url": result.growth_document_url,
    }
