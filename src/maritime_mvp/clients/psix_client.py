# src/maritime_mvp/clients/psix_client.py
"""
PSIX client - working version with VesselID=0 fix
"""
from __future__ import annotations
import os
import logging
from typing import Optional, Any, Dict, List
import requests
from xml.etree import ElementTree as ET
import re
import warnings
import html as _html

warnings.filterwarnings('ignore', message='Unverified HTTPS request')

logger = logging.getLogger(__name__)

PSIX_URL = os.getenv("PSIX_URL", "https://cgmix.uscg.mil/xml/PSIXData.asmx")
VERIFY_SSL = os.getenv("PSIX_VERIFY_SSL", "false").lower() in ("1", "true", "yes", "y")
TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

class PsixClient:
    """PSIX SOAP client using direct HTTP requests."""
    
    def __init__(self, url: str | None = None, verify_ssl: bool | None = None, timeout: int | None = None):
        self.url = url or PSIX_URL
        self.verify_ssl = VERIFY_SSL if verify_ssl is None else verify_ssl
        self.timeout = TIMEOUT if timeout is None else timeout
        self.session = requests.Session()
        self.session.verify = self.verify_ssl
        self.session.headers.update({
            'Content-Type': 'text/xml; charset=utf-8',
            'User-Agent': 'MaritimeMVP/0.2'
        })
        logger.info(f"PSIX client initialized with URL: {self.url}")

    def get_vessel_summary(self, *, vessel_id: int | None = None, vessel_name: str = "",
                          call_sign: str = "", vin: str = "", hin: str = "", flag: str = "",
                          service: str = "", build_year: str = "") -> Dict[str, Any]:
        """Get vessel summary from PSIX using SOAP request."""
        
        # CRITICAL: Use 0 when VesselID is not provided, not empty string!
        vid = vessel_id if vessel_id is not None else 0
        
        # Try the known working namespace first
        namespaces = [
            "https://cgmix.uscg.mil",    # This one works
            "http://cgmix.uscg.mil",     # Fallback
        ]
        
        for ns in namespaces:
            # Build SOAP envelope
            soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
            <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
                          xmlns:xsd="http://www.w3.org/2001/XMLSchema" 
                          xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
              <soap:Body>
                <getVesselSummary xmlns="{ns}">
                  <VesselID>{vid}</VesselID>
                  <VesselName>{vessel_name}</VesselName>
                  <CallSign>{call_sign}</CallSign>
                  <VIN>{vin}</VIN>
                  <HIN>{hin}</HIN>
                  <Flag>{flag}</Flag>
                  <Service>{service}</Service>
                  <BuildYear>{build_year}</BuildYear>
                </getVesselSummary>
              </soap:Body>
            </soap:Envelope>"""
            
            logger.debug(f"Trying namespace: {ns} with VesselID={vid}")
            
            try:
                # Make SOAP request
                soap_action = f'"{ns}/getVesselSummary"'
                response = self.session.post(
                    self.url,
                    data=soap_body,
                    headers={'SOAPAction': soap_action},
                    timeout=self.timeout
                )
                
                response_text = response.text
                
                # Check if we got a result element
                if re.search(r"getVesselSummaryResult", response_text, re.IGNORECASE):
                    logger.info(f"Got response with namespace: {ns}")
                    vessels = self._parse_response(response_text)
                    if vessels:
                        logger.info(f"Successfully parsed {len(vessels)} vessels")
                        return {"Table": vessels}
                    else:
                        logger.debug(f"Response had result element but no vessels parsed")
                        # Log a snippet for debugging
                        result_match = re.search(
                            r"<getVesselSummaryResult[^>]*>(.*?)</getVesselSummaryResult>",
                            response_text, re.IGNORECASE | re.DOTALL
                        )
                        if result_match:
                            snippet = result_match.group(1)[:250]
                            logger.debug(f"Result snippet: {snippet}")
                        
            except Exception as e:
                logger.debug(f"Namespace {ns} failed: {e}")
                continue
        
        # If all attempts failed, return empty result (not mock data in production)
        logger.warning(f"No results found for vessel_name='{vessel_name}'")
        return {"Table": []}

    def _parse_response(self, response_text: str) -> List[Dict[str, Any]]:
        """Parse PSIX response, handling escaped XML."""
        # Try to extract the result element content
        match = re.search(
            r"<getVesselSummaryResult[^>]*>(.*?)</getVesselSummaryResult>",
            response_text, re.IGNORECASE | re.DOTALL
        )
        
        if match:
            payload = match.group(1)
            # Check if the content is HTML-escaped
            if "&lt;" in payload:
                payload = _html.unescape(payload)
            
            # Try to parse the unescaped content
            vessels = self._extract_vessels(payload)
            if vessels:
                return vessels
        
        # Fallback: try to extract vessels from the whole response
        return self._extract_vessels(response_text)

    def _extract_vessels(self, xml_text: str) -> List[Dict[str, Any]]:
        """Extract vessel data from XML, handles Table, Table1, Table2, etc."""
        vessels = []
        
        # Match <Table>, <Table1>, <Table2>, etc.
        table_pattern = r"<Table\d*[^>]*>(.*?)</Table\d*>"
        tables = re.findall(table_pattern, xml_text, re.IGNORECASE | re.DOTALL)
        
        for table_content in tables:
            vessel = {}
            
            # Extract all common vessel fields
            fields = [
                "VesselID", "VesselName", "CallSign", "IMONumber", "OfficialNumber",
                "Flag", "VesselType", "GrossTonnage", "NetTonnage", "DeadWeight",
                "YearBuilt", "Builder", "HullMaterial", "PropulsionType",
                "DocumentationExpirationDate", "COIExpirationDate",
                "Owner", "Operator", "ManagingOwner"
            ]
            
            for field in fields:
                pattern = fr"<{field}[^>]*>(.*?)</{field}>"
                match = re.search(pattern, table_content, re.IGNORECASE | re.DOTALL)
                if match:
                    value = match.group(1).strip()
                    # Skip empty or "None" values
                    if value and value.lower() != "none":
                        # Clean CDATA if present
                        if value.startswith("<![CDATA["):
                            value = value[9:-3]
                        vessel[field] = value
            
            # Only add vessels that have at least a name
            if vessel.get("VesselName"):
                vessels.append(vessel)
        
        return vessels

    def search_by_name(self, name: str) -> Dict[str, Any]:
        """Search for vessels by name."""
        return self.get_vessel_summary(vessel_name=name)

    def list_cases(self, vessel_id: int) -> Dict[str, Any]:
        """List cases for a vessel - stub for compatibility."""
        logger.warning("list_cases not implemented in HTTP client")
        return {"Cases": []}

    def list_deficiencies(self, vessel_id: int, activity_number: str) -> Dict[str, Any]:
        """List deficiencies for a vessel - stub for compatibility."""
        logger.warning("list_deficiencies not implemented in HTTP client")
        return {"Deficiencies": []}
