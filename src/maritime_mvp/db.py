from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .settings import settings

engine = create_engine(str(settings.database_url))
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
