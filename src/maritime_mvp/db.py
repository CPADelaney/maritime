from __future__ import annotations
import logging
from typing import Any, Dict

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker

from .settings import settings
from .models import Base  # ← use the same Base as models.py

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

def init_db(safe: bool = True) -> bool:
    """
    Create tables if possible. If safe=True, swallow errors so the app can boot.
    Returns True if create_all ran without error.
    """
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("DB init: metadata.create_all completed.")
        return True
    except Exception:
        logger.exception("DB init failed (create_all).")
        if not safe:
            raise
        return False


def _psycopg2_kwargs(u: URL) -> Dict[str, Any]:
    """
    Translate SQLAlchemy URL -> psycopg2.connect kwargs.
    Avoid passing a DSN string so we don't lose the password to '***'
    and so '+psycopg2' driver suffix never leaks into libpq.
    """
    kw: Dict[str, Any] = {
        "user": u.username,
        "password": u.password,
        "host": u.host,
        "port": u.port,
        "dbname": u.database,
    }
    sslmode = u.query.get("sslmode")
    if sslmode:
        kw["sslmode"] = sslmode
    # If password missing on the URL but present in env PG* variables, use it
    if not kw.get("password") and settings.pg_password:
        kw["password"] = settings.pg_password
    return kw


def ping_db() -> Dict[str, Any]:
    """
    Attempt a direct connection using psycopg2 to show exactly what succeeds/fails.
    """
    url = engine.url
    info: Dict[str, Any] = {
        "source": "DATABASE_URL" if settings.database_url else "PGVARS",
        "dsn_user": url.username,
        "dsn_host": url.host,
        "dsn_port": url.port,
        "dsn_database": url.database,
        "dsn_sslmode": url.query.get("sslmode"),
    }

    conn = None
    try:
        conn = psycopg2.connect(**_psycopg2_kwargs(url))
        with conn.cursor() as cur:
            cur.execute("select current_user, inet_server_addr()::text, inet_server_port(), version()")
            row = cur.fetchone()
            info.update({
                "connect_ok": True,
                "server_current_user": row[0],
                "server_addr": row[1],
                "server_port": row[2],
                "server_version": row[3],
            })
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
