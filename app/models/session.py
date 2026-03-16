from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from app.config import DATABASE_URL
from app.models.db import Base

_db_url = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(_db_url, echo=False)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def init_db():
    """Create all tables if they don't exist."""
    Base.metadata.create_all(bind=engine)


def get_db() -> Session:
    """FastAPI dependency for DB sessions."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
