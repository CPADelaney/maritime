from __future__ import annotations
from pydantic_settings import BaseSettings
from pydantic import Field
from urllib.parse import quote

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
            return self.database_url
        if self.pg_host and self.pg_user and self.pg_password and self.pg_db:
            return (
                f"postgresql+psycopg2://{quote(self.pg_user)}:{quote(self.pg_password)}"
                f"@{self.pg_host}:{self.pg_port}/{self.pg_db}?sslmode=require"
            )
        raise RuntimeError("DATABASE_URL or PG* vars must be set")

settings = Settings()
