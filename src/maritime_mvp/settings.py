from pydantic_settings import BaseSettings
from pydantic import AnyUrl, Field

class Settings(BaseSettings):
    database_url: AnyUrl = Field(alias="DATABASE_URL")
    psix_wsdl: str = Field(default="https://cgmix.uscg.mil/xml/PSIXData.asmx?WSDL", alias="PSIX_WSDL")
    psix_verify_ssl: bool = Field(default=False, alias="PSIX_VERIFY_SSL")
    request_timeout: int = Field(default=30)

    model_config = {
        "env_file": ".env",
        "extra": "ignore",
    }

settings = Settings()
