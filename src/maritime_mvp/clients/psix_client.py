# src/maritime_mvp/clients/psix_client.py
"""
PSIX client using direct HTTP/SOAP requests without zeep dependency.
Compatible with Python 3.13+
"""
from __future__ import annotations
import os
import logging
from typing import Optional, Any, Dict, List
import requests
from xml.etree import ElementTree as ET
import json

logger = logging.getLogger(__name__)

PSIX_URL = os.getenv("PSIX_URL", "https://cgmix.uscg.mil/xml/PSIXData.asmx")
VERIFY_SSL = os.getenv("PSIX_VERIFY_SSL", "false").lower() in ("1", "true", "yes", "y")
TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

class PsixClient:
    """PSIX SOAP client using direct HTTP requests (no zeep dependency)."""
    
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

    def _parse_soap_response(self, xml_text: str) -> Dict[str, Any]:
        """Parse SOAP XML response into a dictionary."""
        try:
            # Remove namespace prefixes for easier parsing
            xml_text = xml_text.replace('xmlns:', 'xmlnamespace:')
            root = ET.fromstring(xml_text)
            
            # Find the result element (it's deeply nested in SOAP responses)
            # Typical path: Envelope > Body > getVesselSummaryResponse > getVesselSummaryResult
            result_elem = None
            for elem in root.iter():
                if 'getVesselSummaryResult' in elem.tag:
                    result_elem = elem
                    break
            
            if result_elem is None:
                logger.warning("No result element found in SOAP response")
                return {"Table": []}
            
            # The result contains a DataSet with a Table
            vessels = []
            for table_elem in result_elem.iter():
                if table_elem.tag.endswith('Table'):
                    vessel = {}
                    for field in table_elem:
                        # Get the field name without namespace
                        field_name = field.tag.split('}')[-1] if '}' in field.tag else field.tag
                        vessel[field_name] = field.text
                    if vessel:
                        vessels.append(vessel)
            
            return {"Table": vessels}
            
        except ET.ParseError as e:
            logger.error(f"Failed to parse SOAP response: {e}")
            # Try to extract any vessel data using regex as fallback
            vessels = self._fallback_parse(xml_text)
            return {"Table": vessels}
        except Exception as e:
            logger.error(f"Unexpected error parsing SOAP response: {e}")
            return {"Table": []}

    def _fallback_parse(self, xml_text: str) -> List[Dict[str, Any]]:
        """Fallback parser using regex to extract vessel data."""
        import re
        vessels = []
        
        # Look for Table elements
        table_pattern = r'<Table[^>]*>(.*?)</Table>'
        tables = re.findall(table_pattern, xml_text, re.DOTALL)
        
        for table in tables:
            vessel = {}
            # Extract common fields
            patterns = {
                'VesselName': r'<VesselName[^>]*>(.*?)</VesselName>',
                'CallSign': r'<CallSign[^>]*>(.*?)</CallSign>',
                'Flag': r'<Flag[^>]*>(.*?)</Flag>',
                'VesselType': r'<VesselType[^>]*>(.*?)</VesselType>',
                'IMONumber': r'<IMONumber[^>]*>(.*?)</IMONumber>',
                'OfficialNumber': r'<OfficialNumber[^>]*>(.*?)</OfficialNumber>',
                'GrossTonnage': r'<GrossTonnage[^>]*>(.*?)</GrossTonnage>',
                'NetTonnage': r'<NetTonnage[^>]*>(.*?)</NetTonnage>',
            }
            
            for field, pattern in patterns.items():
                match = re.search(pattern, table)
                if match:
                    vessel[field] = match.group(1).strip()
            
            if vessel:
                vessels.append(vessel)
        
        return vessels

    def get_vessel_summary(self, *, vessel_id: int | None = None, vessel_name: str = "",
                          call_sign: str = "", vin: str = "", hin: str = "", flag: str = "",
                          service: str = "", build_year: str = "") -> Dict[str, Any]:
        """Get vessel summary from PSIX using SOAP request."""
        
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
        
        try:
            # Make SOAP request
            response = self.session.post(
                self.url,
                data=soap_body,
                headers={'SOAPAction': 'https://cgmix.uscg.mil/getVesselSummary'},
                timeout=self.timeout
            )
            response.raise_for_status()
            
            # Parse response
            result = self._parse_soap_response(response.text)
            logger.info(f"PSIX search for vessel_name='{vessel_name}' returned {len(result.get('Table', []))} results")
            return result
            
        except requests.exceptions.Timeout:
            logger.error(f"PSIX request timed out after {self.timeout} seconds")
            return {"Table": [], "error": "Request timed out"}
        except requests.exceptions.RequestException as e:
            logger.error(f"PSIX request failed: {e}")
            return {"Table": [], "error": str(e)}
        except Exception as e:
            logger.error(f"Unexpected error in PSIX request: {e}")
            return {"Table": [], "error": str(e)}

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
