from __future__ import annotations
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .settings import settings

# Supabase can drop idle conns; pre_ping + recycle helps.
engine = create_engine(
    settings.sqlalchemy_url,
    pool_pre_ping=True,
    pool_recycle=300,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

def init_db() -> None:
    # Safe if tables already exist
    from .models import Base
    Base.metadata.create_all(bind=engine)
