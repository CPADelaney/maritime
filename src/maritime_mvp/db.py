from __future__ import annotations
import logging
from typing import Any, Dict

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

from .settings import settings

logger = logging.getLogger("maritime-api")

# Engine
engine = create_engine(
    settings.sqlalchemy_url,
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_size=5,
    max_overflow=5,
    future=True,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)
Base = declarative_base()


def init_db(safe: bool = True) -> bool:
    """
    Create tables if possible. If safe=True, swallow errors so the app can boot.
    Returns True if create_all ran without error.
    """
    try:
        # Import models to register metadata
        from . import models  # noqa: F401
        Base.metadata.create_all(bind=engine)
        logger.info("DB init: metadata.create_all completed.")
        return True
    except Exception:
        logger.exception("DB init failed (create_all).")
        if not safe:
            raise
        return False


def ping_db() -> Dict[str, Any]:
    """
    Attempt a direct connection using psycopg2 to show exactly what succeeds/fails.
    Password is never returned; we only return length + trailing-space flag.
    """
    url = engine.url
    info: Dict[str, Any] = {
        "source": "DATABASE_URL" if settings.database_url else "PGVARS",
        "dsn_user": url.username,
        "dsn_host": url.host,
        "dsn_port": url.port,
        "dsn_database": url.database,
        "dsn_sslmode": url.query.get("sslmode"),
        "password_len": len((settings.pg_password or "")),
        "password_has_trailing_space": (settings.pg_password or "").rstrip() != (settings.pg_password or ""),
    }

    conn = None
    try:
        conn = psycopg2.connect(str(url))
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
