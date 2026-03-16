from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from app.config import DATABASE_URL
from app.models.db import Base
import logging

logger = logging.getLogger(__name__)

_db_url = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(_db_url, echo=False)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def init_db():
    """Create tables if they don't exist. Non-fatal on connection error."""
    try:
        Base.metadata.create_all(bind=engine)
    except Exception as e:
        logger.warning(f"init_db skipped — could not connect: {e}")


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
