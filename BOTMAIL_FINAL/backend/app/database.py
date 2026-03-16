from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError

from config import DATABASE_URL, settings

# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=10,           # concurrent connections kept alive
    max_overflow=20,        # extra connections allowed under load
    pool_pre_ping=True,     # drop stale connections before use
    pool_recycle=1800,      # recycle connections every 30 min
    echo=settings.DEBUG,    # log SQL only in debug mode
)

# Enforce foreign keys if using SQLite (dev/test only)
@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_conn, _):
    if "sqlite" in DATABASE_URL:
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,  # avoids lazy-load errors after commit
)

Base = declarative_base()

# ---------------------------------------------------------------------------
# Dependency — FastAPI route injection
# ---------------------------------------------------------------------------
def get_db() -> Generator[Session, None, None]:
    """Yield a DB session and guarantee cleanup."""
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        raise
    finally:
        db.close()

# ---------------------------------------------------------------------------
# Context manager — Celery workers / scripts
# ---------------------------------------------------------------------------
@contextmanager
def get_db_context() -> Generator[Session, None, None]:
    """Use inside Celery tasks or CLI scripts where DI isn't available."""
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        raise
    finally:
        db.close()

# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------
def create_tables() -> None:
    Base.metadata.create_all(bind=engine)

def drop_tables() -> None:
    Base.metadata.drop_all(bind=engine)