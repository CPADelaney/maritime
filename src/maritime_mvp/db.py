# src/maritime_mvp/db.py
from __future__ import annotations
from sqlalchemy import create_engine
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
        "options": "-c statement_timeout=30000"  # 30 second timeout
    }
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

def init_db() -> None:
    # Safe if tables already exist
    from .models import Base
    Base.metadata.create_all(bind=engine)
