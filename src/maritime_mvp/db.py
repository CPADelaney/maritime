# src/maritime_mvp/db.py
from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from .settings import settings

# Update the connection string for psycopg3
# psycopg3 uses 'postgresql+psycopg' instead of 'postgresql+psycopg2'
def get_sqlalchemy_url():
    url = settings.sqlalchemy_url
    # Convert psycopg2 URL to psycopg3 format
    if 'postgresql+psycopg2' in url:
        url = url.replace('postgresql+psycopg2', 'postgresql+psycopg')
    elif url.startswith('postgresql://'):
        # Default postgresql:// uses psycopg2, explicitly use psycopg
        url = url.replace('postgresql://', 'postgresql+psycopg://')
    return url

engine = create_engine(
    get_sqlalchemy_url(),
    pool_pre_ping=True,
    pool_recycle=300,
    # psycopg3 specific options
    connect_args={
        "options": "-c statement_timeout=30000",  # 30 second timeout
        # Prevent duplicate prepared statement errors across pooled connections
        # by disabling psycopg's automatic server-side prepared statements.
        "prepare_threshold": 0,
    }
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

logger = logging.getLogger(__name__)

_SEED_SCRIPTS = [
    ("port_documents", "port_documents.sql"),
]


def _script_statements(script: str) -> list[str]:
    statements: list[str] = []
    for raw in script.split(";"):
        stmt = raw.strip()
        if not stmt:
            continue
        upper = stmt.upper()
        if upper in {"BEGIN", "COMMIT"}:
            continue
        statements.append(stmt)
    return statements


def _run_sql_script(script_path: Path) -> None:
    if not script_path.exists():
        return
    script = script_path.read_text()
    statements = _script_statements(script)
    if not statements:
        return
    with engine.begin() as conn:
        for statement in statements:
            conn.exec_driver_sql(statement)


def _load_seed_data() -> None:
    base_dir = Path(__file__).resolve().parent.parent.parent
    seeds_root = base_dir / "db" / "seeds"
    if not seeds_root.exists():
        return

    with engine.connect() as conn:
        for table_name, script_name in _SEED_SCRIPTS:
            script_path = seeds_root / script_name
            if not script_path.exists():
                continue
            try:
                has_rows = conn.execute(text(f"SELECT 1 FROM {table_name} LIMIT 1")).first() is not None
            except Exception:
                logger.debug("Skipping seed load for %s; table unavailable.", table_name, exc_info=True)
                continue
            if has_rows:
                continue
            _run_sql_script(script_path)
            logger.info("Loaded seed data for %s from %s", table_name, script_path.name)


def init_db() -> None:
    # Safe if tables already exist
    from .models import Base

    Base.metadata.create_all(bind=engine)
    _load_seed_data()
