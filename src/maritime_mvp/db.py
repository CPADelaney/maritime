from __future__ import annotations
import logging
from typing import Any, Dict
from urllib.parse import quote_plus

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .settings import settings
from .models import Base  # ← unify Base here

logger = logging.getLogger("maritime-api")

# ---- Engine (OK for Supabase poolers) ----
engine = create_engine(
    settings.sqlalchemy_url,
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_size=5,
    max_overflow=5,
    future=True,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)


def init_db(safe: bool = True) -> bool:
    """
    Create tables if possible. If safe=True, swallow errors so the app can boot.
    Returns True if create_all ran without error.
    """
    try:
        # Import models ensures mappers are registered; Base comes from models.py
        from . import models  # noqa: F401
        Base.metadata.create_all(bind=engine)
        logger.info("DB init: metadata.create_all completed.")
        return True
    except Exception:
        logger.exception("DB init failed (create_all).")
        if not safe:
            raise
        return False


def _libpq_dsn_from_sqlalchemy_url() -> tuple[str, Dict[str, Any]]:
    """
    Build a psycopg2 / libpq-compatible DSN from the SQLAlchemy URL,
    stripping any '+psycopg2' driver token and ensuring sslmode.
    """
    sa_url = engine.url  # sqlalchemy.engine.URL
    user = sa_url.username or ""
    password = sa_url.password or settings.pg_password or ""
    host = sa_url.host or "localhost"
    port = sa_url.port or 5432
    db = sa_url.database or "postgres"
    sslmode = (sa_url.query or {}).get("sslmode") or "require"

    # psycopg2/libpq DSN must be postgresql:// (no "+psycopg2")
    dsn = (
        "postgresql://"
        f"{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{db}?sslmode={sslmode}"
    )

    info = {
        "dsn_user": user,
        "dsn_host": host,
        "dsn_port": port,
        "dsn_database": db,
        "dsn_sslmode": sslmode,
    }
    return dsn, info


def ping_db() -> Dict[str, Any]:
    """
    Attempt a direct connection using psycopg2 to show exactly what succeeds/fails.
    Password is never returned; we only return length + trailing-space flag.
    """
    source = "DATABASE_URL"
    if settings.pg_host or settings.pg_user or settings.pg_password or settings.pg_db:
        source = "PGVARS"

    info: Dict[str, Any] = {"source": source}
    dsn, dsn_info = _libpq_dsn_from_sqlalchemy_url()
    info.update(dsn_info)
    pw = settings.pg_password or (engine.url.password or "")
    info["password_len"] = len(pw)
    info["password_has_trailing_space"] = pw.rstrip() != pw

    conn = None
    try:
        conn = psycopg2.connect(dsn)
        with conn.cursor() as cur:
            cur.execute("select current_user, inet_server_addr()::text, inet_server_port(), version()")
            row = cur.fetchone()
            info["connect_ok"] = True
            info["server_current_user"] = row[0]
            info["server_addr"] = row[1]
            info["server_port"] = row[2]
            info["server_version"] = row[3]
    except Exception as e:
        info["connect_ok"] = False
        info["error"] = str(e)
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

    return info
