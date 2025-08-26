from __future__ import annotations
import os
import logging
from pydantic_settings import BaseSettings
from pydantic import Field
from urllib.parse import quote, quote_plus, urlparse, urlunparse

logger = logging.getLogger("maritime-api")

class Settings(BaseSettings):
    # Prefer a full DATABASE_URL; or supply PG* parts and we'll build it.
    database_url: str | None = Field(default=None, alias="DATABASE_URL")

    pg_host: str | None = Field(default=None, alias="PGHOST")
    pg_port: int = Field(default=5432, alias="PGPORT")
    pg_user: str | None = Field(default=None, alias="PGUSER")
    pg_password: str | None = Field(default=None, alias="PGPASSWORD")
    pg_db: str | None = Field(default=None, alias="PGDATABASE")

    psix_wsdl: str = Field(default="https://cgmix.uscg.mil/xml/PSIXData.asmx?WSDL", alias="PSIX_WSDL")
    psix_verify_ssl: bool = Field(default=False, alias="PSIX_VERIFY_SSL")
    request_timeout: int = Field(default=30, alias="REQUEST_TIMEOUT")

    model_config = {"env_file": ".env", "extra": "ignore"}

    @property
    def sqlalchemy_url(self) -> str:
        if self.database_url:
            # Parse and fix the DATABASE_URL if needed
            parsed = urlparse(self.database_url)
            
            # Log parsed components (without password)
            logger.info(f"DB target → user={parsed.username} host={parsed.hostname} port={parsed.port} db={parsed.path.lstrip('/')}")
            logger.info("DB config source → DATABASE_URL")
            
            # If the URL has a password, ensure it's properly encoded
            if parsed.password:
                # Re-encode the password to handle special characters
                encoded_password = quote_plus(parsed.password)
                
                # Reconstruct the URL with the encoded password
                # Format: postgresql://user:password@host:port/database?params
                if parsed.query:
                    fixed_url = f"{parsed.scheme}://{parsed.username}:{encoded_password}@{parsed.hostname}:{parsed.port}{parsed.path}?{parsed.query}"
                else:
                    fixed_url = f"{parsed.scheme}://{parsed.username}:{encoded_password}@{parsed.hostname}:{parsed.port}{parsed.path}"
                
                return fixed_url
            
            # If no password in URL, return as-is
            return self.database_url
            
        # Build from components if DATABASE_URL not provided
        if self.pg_host and self.pg_user and self.pg_password and self.pg_db:
            logger.info(f"DB target → user={self.pg_user} host={self.pg_host} port={self.pg_port} db={self.pg_db}")
            logger.info("DB config source → PG* environment variables")
            
            # Properly encode password for URL
            encoded_password = quote_plus(self.pg_password)
            encoded_user = quote_plus(self.pg_user)
            
            return (
                f"postgresql+psycopg2://{encoded_user}:{encoded_password}"
                f"@{self.pg_host}:{self.pg_port}/{self.pg_db}?sslmode=require"
            )
        
        raise RuntimeError("DATABASE_URL or PG* vars must be set")

settings = Settings()
