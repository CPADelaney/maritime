# src/maritime_mvp/clients/psix_client.py (Diagnostic version - temporary)
"""
PSIX client with diagnostic logging to debug empty responses.
"""
from __future__ import annotations
import os
import logging
from typing import Optional, Any, Dict, List
import requests
from xml.etree import ElementTree as ET
import re
import warnings

warnings.filterwarnings('ignore', message='Unverified HTTPS request')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Force debug level

PSIX_URL = os.getenv("PSIX_URL", "https://cgmix.uscg.mil/xml/PSIXData.asmx")
VERIFY_SSL = os.getenv("PSIX_VERIFY_SSL", "false").lower() in ("1", "true", "yes", "y")
TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

class PsixClient:
    """PSIX SOAP client with diagnostic logging."""
    
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
        """Get vessel summary from PSIX with full diagnostic output."""
        
        # Build SOAP envelope
        soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
                      xmlns:xsd="http://www.w3.org/2001/XMLSchema" 
                      xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
          <soap:Body>
            <getVesselSummary xmlns="https://cgmix.uscg.mil/">
              <VesselID>{vessel_id if vessel_id else ''}</VesselID>
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
        
        logger.info(f"SENDING SOAP REQUEST FOR: vessel_name='{vessel_name}'")
        logger.debug(f"Full SOAP request:\n{soap_body}")
        
        try:
            # Make SOAP request
            response = self.session.post(
                self.url,
                data=soap_body,
                headers={'SOAPAction': 'https://cgmix.uscg.mil/getVesselSummary'},
                timeout=self.timeout
            )
            
            logger.info(f"PSIX Response Status: {response.status_code}")
            logger.info(f"PSIX Response Headers: {dict(response.headers)}")
            
            # Log the full response for debugging
            response_text = response.text
            logger.info(f"PSIX Response Length: {len(response_text)} characters")
            
            # Log first 2000 characters of response
            logger.info(f"PSIX Response Preview:\n{response_text[:2000]}")
            
            # Check if it's actually SOAP
            if 'soap:Envelope' in response_text or 'Envelope' in response_text:
                logger.info("Response appears to be SOAP")
                
                # Look for any vessel data patterns
                if 'VesselName' in response_text:
                    logger.info("Found VesselName in response!")
                    # Extract all vessel names for debugging
                    vessel_names = re.findall(r'<VesselName[^>]*>(.*?)</VesselName>', response_text, re.IGNORECASE)
                    logger.info(f"Vessel names found: {vessel_names}")
                    
                if 'Table' in response_text:
                    logger.info("Found Table elements in response")
                    table_count = response_text.count('<Table')
                    logger.info(f"Number of Table elements: {table_count}")
                    
                if 'getVesselSummaryResult' in response_text:
                    logger.info("Found getVesselSummaryResult")
                    # Extract the result content
                    result_match = re.search(r'<getVesselSummaryResult[^>]*>(.*?)</getVesselSummaryResult>', 
                                           response_text, re.DOTALL | re.IGNORECASE)
                    if result_match:
                        result_content = result_match.group(1)[:500]
                        logger.info(f"Result content preview: {result_content}")
                        
                # Check for SOAP fault
                if 'soap:Fault' in response_text or 'faultstring' in response_text:
                    logger.error("SOAP FAULT detected!")
                    fault_match = re.search(r'<faultstring[^>]*>(.*?)</faultstring>', response_text)
                    if fault_match:
                        logger.error(f"Fault message: {fault_match.group(1)}")
                        
            elif response_text.startswith('<!DOCTYPE'):
                logger.warning("Response is HTML, not SOAP!")
                # Extract title to see what page we got
                title_match = re.search(r'<title>(.*?)</title>', response_text)
                if title_match:
                    logger.warning(f"HTML page title: {title_match.group(1)}")
            else:
                logger.warning(f"Unknown response format. First 100 chars: {response_text[:100]}")
            
            # Try to parse anyway and see what we get
            vessels = self._attempt_parse(response_text)
            logger.info(f"FINAL RESULT: Found {len(vessels)} vessels")
            
            return {"Table": vessels}
            
        except requests.exceptions.Timeout:
            logger.error(f"PSIX request timed out after {self.timeout} seconds")
            return {"Table": [], "error": "Request timed out"}
        except requests.exceptions.RequestException as e:
            logger.error(f"PSIX request failed: {e}")
            return {"Table": [], "error": str(e)}
        except Exception as e:
            logger.error(f"Unexpected error in PSIX request: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"Table": [], "error": str(e)}

    def _attempt_parse(self, response_text: str) -> List[Dict[str, Any]]:
        """Try multiple parsing strategies."""
        vessels = []
        
        # Strategy 1: Look for Table XML blocks
        table_pattern = r'<Table[^>]*>(.*?)</Table>'
        tables = re.findall(table_pattern, response_text, re.DOTALL | re.IGNORECASE)
        logger.info(f"Regex found {len(tables)} Table blocks")
        
        for i, table_content in enumerate(tables[:5]):  # First 5 tables
            vessel = {}
            # Extract fields
            for field in ['VesselName', 'CallSign', 'Flag', 'VesselType', 'IMONumber']:
                pattern = f'<{field}[^>]*>(.*?)</{field}>'
                match = re.search(pattern, table_content, re.IGNORECASE)
                if match:
                    vessel[field] = match.group(1).strip()
            
            if vessel:
                logger.info(f"Table {i} parsed: {vessel}")
                vessels.append(vessel)
        
        # Strategy 2: If no tables found, look for vessel names directly
        if not vessels:
            name_pattern = r'<VesselName[^>]*>([^<]+)</VesselName>'
            names = re.findall(name_pattern, response_text, re.IGNORECASE)
            logger.info(f"Found {len(names)} vessel names directly")
            
            for name in names[:10]:  # First 10
                if name and not name.startswith('<'):
                    vessels.append({'VesselName': name.strip()})
                    logger.info(f"Added vessel: {name.strip()}")
        
        return vessels

    def search_by_name(self, name: str) -> Dict[str, Any]:
        """Search for vessels by name."""
        return self.get_vessel_summary(vessel_name=name)
