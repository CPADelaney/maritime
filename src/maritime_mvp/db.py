# db.py
from __future__ import annotations
import logging
from typing import Any, Dict
import hashlib  # ← add this

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker

from .settings import settings
from .models import Base

logger = logging.getLogger("maritime-api")

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
    if not kw.get("password") and settings.pg_password:
        kw["password"] = settings.pg_password
    return kw

def ping_db() -> Dict[str, Any]:
    url = engine.url
    info: Dict[str, Any] = {
        "source": "DATABASE_URL" if settings.database_url else "PGVARS",
        "dsn_user": url.username,
        "dsn_host": url.host,
        "dsn_port": url.port,
        "dsn_database": url.database,
        "dsn_sslmode": url.query.get("sslmode"),
    }

    # Build connect kwargs up front so we can inspect them even if connect() fails
    kw = _psycopg2_kwargs(url)

    # DEBUG (guarded): reveal *exact* password strings + repr + sha256
    if settings.debug_show_db_password:
        pw_url = url.password or ""
        pw_env = settings.pg_password or ""
        pw_sent = kw.get("password") or ""

        info.update({
            # What’s parsed off the URL:
            "debug_password_from_url": pw_url,
            "debug_password_from_url_repr": repr(pw_url),
            "debug_password_from_url_len": len(pw_url),
            "debug_password_from_url_sha256": hashlib.sha256(pw_url.encode()).hexdigest() if pw_url else None,

            # What’s in PG* env (if any):
            "debug_password_from_env": pw_env or None,
            "debug_password_from_env_repr": repr(pw_env) if pw_env else None,
            "debug_password_from_env_len": len(pw_env) if pw_env else 0,
            "debug_password_from_env_sha256": hashlib.sha256(pw_env.encode()).hexdigest() if pw_env else None,

            # What will actually be sent to psycopg2:
            "debug_password_psycopg2_sent": pw_sent,
            "debug_password_psycopg2_sent_repr": repr(pw_sent),
            "debug_password_psycopg2_sent_len": len(pw_sent),
            "debug_password_psycopg2_sent_sha256": hashlib.sha256(pw_sent.encode()).hexdigest() if pw_sent else None,

            # Quick equality checks:
            "debug_url_equals_env": (pw_url == pw_env) if pw_env else None,
            "debug_url_equals_sent": (pw_url == pw_sent),
            "debug_env_equals_sent": (pw_env == pw_sent) if pw_env else None,
        })

    conn = None
    try:
        conn = psycopg2.connect(**kw)
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
