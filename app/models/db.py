from sqlalchemy import (
    Column, String, Integer, Float, Boolean, DateTime, Text,
    Enum, ForeignKey, UniqueConstraint, Index, Numeric
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func
import enum

Base = declarative_base()


class ResultStatus(str, enum.Enum):
    QUALIFIES = "QUALIFIES"
    DOES_NOT_QUALIFY = "DOES_NOT_QUALIFY"
    MANUAL_REVIEW = "MANUAL_REVIEW"
    ERROR = "ERROR"


class ReviewStatus(str, enum.Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    CONTACTED = "CONTACTED"
    CONFIRMED = "CONFIRMED"
    DECLINED = "DECLINED"
    DISQUALIFIED = "DISQUALIFIED"


class Company(Base):
    __tablename__ = "companies"

    company_number = Column(String(8), primary_key=True)
    company_name = Column(String(500), nullable=False)
    company_status = Column(String(50))
    company_type = Column(String(50))
    incorporated_date = Column(String(20))
    sic_codes = Column(Text)
    registered_office_address = Column(Text)
    region = Column(String(100))
    last_fetched_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    results = relationship("IndexResult", back_populates="company")


class IndexResult(Base):
    """
    Pipeline fields:  written by run_pipeline.py, never touched by dashboard
    Editorial fields: written by team via Lovable dashboard
    """
    __tablename__ = "index_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    index_year = Column(Integer, nullable=False)
    company_number = Column(String(8), ForeignKey("companies.company_number"), nullable=False)

    # Pipeline-generated
    baseline_period_start = Column(String(20))
    baseline_period_end = Column(String(20))
    baseline_turnover = Column(Numeric(15, 2))
    growth_period_start = Column(String(20))
    growth_period_end = Column(String(20))
    growth_turnover = Column(Numeric(15, 2))
    growth_percent = Column(Numeric(8, 2))
    baseline_filing_id = Column(String(100))
    growth_filing_id = Column(String(100))
    baseline_document_url = Column(Text)
    growth_document_url = Column(Text)
    status = Column(Enum(ResultStatus, name="result_status"), nullable=False)
    manual_review_reason = Column(Text)
    rank = Column(Integer)
    processed_at = Column(DateTime, server_default=func.now())

    # Editorial — written by team
    contact_email = Column(String(255))
    contact_name = Column(String(255))
    contact_phone = Column(String(50))
    notes = Column(Text)
    review_status = Column(Enum(ReviewStatus, name="review_status"), server_default="PENDING")
    reviewed_by = Column(String(255))
    reviewed_at = Column(DateTime)
    confirmed_for_index = Column(Boolean, default=False, server_default="false")
    featured = Column(Boolean, default=False, server_default="false")
    linkedin_url = Column(Text)
    website_url = Column(Text)

    company = relationship("Company", back_populates="results")

    __table_args__ = (
        UniqueConstraint("index_year", "company_number", name="uq_year_company"),
        Index("ix_results_year_status", "index_year", "status"),
        Index("ix_results_year_rank", "index_year", "rank"),
        Index("ix_results_review_status", "review_status"),
    )


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    index_year = Column(Integer, nullable=False)
    started_at = Column(DateTime, server_default=func.now())
    completed_at = Column(DateTime)
    candidates_found = Column(Integer)
    qualifies_count = Column(Integer)
    manual_review_count = Column(Integer)
    does_not_qualify_count = Column(Integer)
    error_count = Column(Integer)
    baseline_year = Column(Integer)   # e.g. 2023 = FY22/23
    growth_year = Column(Integer)     # e.g. 2024 = FY23/24
    sector = Column(String(100))       # e.g. technology
    sector_label = Column(String(200)) # e.g. Technology & Software
    notes = Column(Text)
