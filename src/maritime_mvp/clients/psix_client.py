# src/maritime_mvp/clients/psix_client.py
from __future__ import annotations
import os
import logging
from typing import Optional, Any, Dict
from zeep import Client, Settings
from zeep.transports import Transport
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

PSIX_WSDL = os.getenv("PSIX_WSDL", "https://cgmix.uscg.mil/xml/PSIXData.asmx?WSDL")
VERIFY_SSL = os.getenv("PSIX_VERIFY_SSL", "false").lower() in ("1", "true", "yes", "y")
TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

class PsixClient:
    """PSIX SOAP client with proper session handling."""

    def __init__(self, wsdl: str | None = None, verify_ssl: bool | None = None, timeout: int | None = None):
        wsdl = wsdl or PSIX_WSDL
        verify_ssl = VERIFY_SSL if verify_ssl is None else verify_ssl
        timeout = TIMEOUT if timeout is None else timeout
        
        # Create a requests session with retry logic
        session = requests.Session()
        session.verify = verify_ssl
        
        # Add retry logic for resilience
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Create transport with the session
        transport = Transport(session=session, timeout=timeout)
        
        # Configure Zeep settings
        settings = Settings(
            strict=False,
            xml_huge_tree=True,
            xsd_ignore_sequence_order=True
        )
        
        try:
            self.client = Client(wsdl=wsdl, transport=transport, settings=settings)
            logger.info(f"PSIX client initialized with WSDL: {wsdl}")
        except Exception as e:
            logger.error(f"Failed to initialize PSIX client: {e}")
            raise

    def get_vessel_summary(self, *, vessel_id: int | None = None, vessel_name: str = "",
                           call_sign: str = "", vin: str = "", hin: str = "", flag: str = "",
                           service: str = "", build_year: str = "") -> Dict[str, Any]:
        """Get vessel summary from PSIX."""
        try:
            vid = vessel_id if vessel_id is not None else ""
            result = self.client.service.getVesselSummary(
                VesselID=vid,
                VesselName=vessel_name,
                CallSign=call_sign,
                VIN=vin,
                HIN=hin,
                Flag=flag,
                Service=service,
                BuildYear=build_year,
            )
            logger.info(f"PSIX search for vessel_name='{vessel_name}' returned results")
            return result
        except Exception as e:
            logger.error(f"PSIX getVesselSummary failed: {e}")
            # Return empty result instead of raising to prevent cascading failures
            return {"Table": []}

    def search_by_name(self, name: str) -> Dict[str, Any]:
        """Search for vessels by name."""
        return self.get_vessel_summary(vessel_name=name)

    def list_cases(self, vessel_id: int) -> Dict[str, Any]:
        """List cases for a vessel."""
        try:
            return self.client.service.getVesselCases(VesselID=vessel_id)
        except Exception as e:
            logger.error(f"PSIX getVesselCases failed: {e}")
            return {}

    def list_deficiencies(self, vessel_id: int, activity_number: str) -> Dict[str, Any]:
        """List deficiencies for a vessel."""
        try:
            return self.client.service.getVesselDeficiencies(
                VesselID=vessel_id, 
                ActivityNumber=activity_number
            )
        except Exception as e:
            logger.error(f"PSIX getVesselDeficiencies failed: {e}")
            return {}

    def __del__(self):
        """Cleanup transport session on deletion."""
        try:
            if hasattr(self, 'client') and hasattr(self.client, 'transport'):
                if hasattr(self.client.transport, 'session'):
                    self.client.transport.session.close()
        except:
            pass  # Ignore errors during cleanup
