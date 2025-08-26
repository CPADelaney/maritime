from __future__ import annotations
from pydantic_settings import BaseSettings
from pydantic import Field
from urllib.parse import quote

class Settings(BaseSettings):
    # If both DATABASE_URL and PG* are present, we will PREFER PG* (fixes Render confusion).
    database_url: str | None = Field(default=None, alias="DATABASE_URL")

    pg_host: str | None = Field(default=None, alias="PGHOST")
    pg_port: int | None = Field(default=None, alias="PGPORT")
    pg_user: str | None = Field(default=None, alias="PGUSER")
    pg_password: str | None = Field(default=None, alias="PGPASSWORD")
    pg_db: str | None = Field(default=None, alias="PGDATABASE")

    # Optional helper: if your PGUSER is just "postgres" and you're on Supabase pooler,
    # set SUPABASE_PROJECT_REF=wqurepavtbknyfpwcbmw and we append it automatically.
    supabase_project_ref: str | None = Field(default=None, alias="SUPABASE_PROJECT_REF")

    psix_wsdl: str = Field(default="https://cgmix.uscg.mil/xml/PSIXData.asmx?WSDL", alias="PSIX_WSDL")
    psix_verify_ssl: bool = Field(default=False, alias="PSIX_VERIFY_SSL")
    request_timeout: int = Field(default=30, alias="REQUEST_TIMEOUT")

    model_config = {"env_file": ".env", "extra": "ignore"}

    def _pg_url(self) -> str | None:
        if not (self.pg_host and self.pg_user and self.pg_password and self.pg_db):
            return None

        user = self.pg_user.strip()
        # Auto-append Supabase project ref if missing and host is a pooler
        if "pooler.supabase.com" in self.pg_host and "." not in user and self.supabase_project_ref:
            user = f"{user}.{self.supabase_project_ref.strip()}"

        host = self.pg_host.strip()
        port = int(self.pg_port or 5432)
        db   = self.pg_db.strip()

        return (
            f"postgresql+psycopg2://{quote(user)}:{quote(self.pg_password)}"
            f"@{host}:{port}/{db}?sslmode=require"
        )

    @property
    def sqlalchemy_url(self) -> str:
        # **Prefer PG*** if present and complete:
        url = self._pg_url()
        if url:
            return url
        # Fallback to DATABASE_URL
        if self.database_url:
            return self.database_url
        raise RuntimeError("Provide either PG* variables or DATABASE_URL")

settings = Settings()
