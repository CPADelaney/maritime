from __future__ import annotations
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from .settings import settings

# Make all functions available for import
__all__ = ['SessionLocal', 'engine', 'init_db', 'test_connection']

logger = logging.getLogger("maritime-api")

# Supabase can drop idle conns; pre_ping + recycle helps.
engine = create_engine(
    settings.sqlalchemy_url,
    pool_pre_ping=True,
    pool_recycle=300,
    # Add these for better debugging
    echo=False,  # Set to True for SQL query logging
    connect_args={
        "connect_timeout": 10,  # Connection timeout in seconds
    }
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

def test_connection() -> bool:
    """Test database connection before creating tables"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            logger.info("Database connection test successful")
            return True
    except OperationalError as e:
        logger.error(f"Database connection test failed: {e}")
        return False

def init_db() -> None:
    """Initialize database tables with better error handling"""
    try:
        # First test the connection
        if not test_connection():
            logger.error("Skipping table creation due to connection failure")
            logger.warning("Database initialization failed, but API will start. Some endpoints may not work.")
            return
        
        # Safe if tables already exist
        from .models import Base
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables initialized successfully")
        
    except OperationalError as e:
        logger.error(f"DB init failed (create_all): {e}")
        logger.warning("Startup complete, but DB init failed (see logs).")
        # Don't raise - let the app start even if DB is down
        # Individual endpoints will fail with proper error messages
    except Exception as e:
        logger.error(f"Unexpected error during DB init: {e}")
        logger.warning("Startup complete, but DB init failed (see logs).")
