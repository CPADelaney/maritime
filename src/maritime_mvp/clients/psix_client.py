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
import re
import warnings

# Suppress SSL warnings since government sites often have cert issues
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

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

    def _clean_xml(self, xml_text: str) -> str:
        """Clean and prepare XML for parsing."""
        # Remove BOM if present
        if xml_text.startswith('\ufeff'):
            xml_text = xml_text[1:]
        
        # Remove XML declaration if duplicated
        xml_text = re.sub(r'<\?xml[^>]+\?>\s*<\?xml[^>]+\?>', '<?xml version="1.0"?>', xml_text)
        
        # Fix common namespace issues
        # Add namespace declarations if missing
        if 'xmlns:soap' not in xml_text and 'soap:' in xml_text:
            xml_text = xml_text.replace(
                '<soap:Envelope',
                '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"'
            )
        
        # Remove problematic namespace prefixes from content
        xml_text = re.sub(r'<(\w+):', r'<', xml_text)
        xml_text = re.sub(r'</(\w+):', r'</', xml_text)
        
        return xml_text

    def _parse_soap_response(self, xml_text: str) -> Dict[str, Any]:
        """Parse SOAP XML response into a dictionary."""
        try:
            # Clean the XML first
            xml_text = self._clean_xml(xml_text)
            
            # Try standard parsing
            root = ET.fromstring(xml_text)
            
            # Find the result element - look for anything with 'Result' in the tag
            vessels = []
            for elem in root.iter():
                if 'getVesselSummaryResult' in elem.tag or elem.tag == 'getVesselSummaryResult':
                    # Parse the nested XML result
                    result_text = elem.text
                    if result_text:
                        # The result contains another XML document
                        vessels = self._parse_vessel_data(result_text)
                    break
            
            # If no result found, try looking for Table elements directly
            if not vessels:
                for table_elem in root.iter():
                    if table_elem.tag == 'Table' or table_elem.tag.endswith('Table'):
                        vessel = {}
                        for field in table_elem:
                            field_name = field.tag.split('}')[-1] if '}' in field.tag else field.tag
                            vessel[field_name] = field.text
                        if vessel:
                            vessels.append(vessel)
            
            return {"Table": vessels}
            
        except ET.ParseError as e:
            logger.error(f"Failed to parse SOAP response: {e}")
            # Try regex fallback
            vessels = self._fallback_parse(xml_text)
            return {"Table": vessels}
        except Exception as e:
            logger.error(f"Unexpected error parsing SOAP response: {e}")
            return {"Table": []}

    def _parse_vessel_data(self, xml_string: str) -> List[Dict[str, Any]]:
        """Parse the inner vessel data XML."""
        vessels = []
        try:
            # Clean this XML too
            xml_string = self._clean_xml(xml_string)
            
            # Parse the vessel data
            vessel_root = ET.fromstring(xml_string)
            
            for table in vessel_root.findall('.//Table'):
                vessel = {}
                for field in table:
                    field_name = field.tag.split('}')[-1] if '}' in field.tag else field.tag
                    vessel[field_name] = field.text or ''
                if vessel:
                    vessels.append(vessel)
        except Exception as e:
            logger.warning(f"Failed to parse vessel data XML: {e}")
            # Fall back to regex
            vessels = self._regex_extract_vessels(xml_string)
        
        return vessels

    def _regex_extract_vessels(self, text: str) -> List[Dict[str, Any]]:
        """Extract vessel data using regex as last resort."""
        vessels = []
        
        # Find all Table blocks
        table_pattern = r'<Table[^>]*>(.*?)</Table>'
        tables = re.findall(table_pattern, text, re.DOTALL | re.IGNORECASE)
        
        for table_content in tables:
            vessel = {}
            
            # Common vessel fields to extract
            fields = [
                'VesselID', 'VesselName', 'CallSign', 'IMONumber', 'OfficialNumber',
                'Flag', 'VesselType', 'GrossTonnage', 'NetTonnage', 'DeadWeight',
                'YearBuilt', 'Builder', 'HullMaterial', 'PropulsionType',
                'DocumentationExpirationDate', 'COIExpirationDate'
            ]
            
            for field in fields:
                pattern = f'<{field}[^>]*>(.*?)</{field}>'
                match = re.search(pattern, table_content, re.IGNORECASE)
                if match:
                    value = match.group(1).strip()
                    # Clean CDATA if present
                    if value.startswith('<![CDATA['):
                        value = value[9:-3]
                    vessel[field] = value
            
            if vessel:
                vessels.append(vessel)
        
        return vessels

    def _fallback_parse(self, xml_text: str) -> List[Dict[str, Any]]:
        """Ultimate fallback parser using aggressive regex."""
        vessels = []
        
        # Try to find any vessel data
        # Look for vessel names first
        name_pattern = r'<VesselName[^>]*>(.*?)</VesselName>'
        names = re.findall(name_pattern, xml_text, re.IGNORECASE)
        
        for name in names:
            if name and not name.startswith('<'):
                # Found a vessel name, try to find associated data
                vessel = {'VesselName': name.strip()}
                
                # Try to find other fields near this vessel name
                # This is crude but works as last resort
                vessel_section = xml_text[max(0, xml_text.find(name) - 500):xml_text.find(name) + 500]
                
                # Extract other fields from this section
                callsign_match = re.search(r'<CallSign[^>]*>(.*?)</CallSign>', vessel_section, re.IGNORECASE)
                if callsign_match:
                    vessel['CallSign'] = callsign_match.group(1).strip()
                
                flag_match = re.search(r'<Flag[^>]*>(.*?)</Flag>', vessel_section, re.IGNORECASE)
                if flag_match:
                    vessel['Flag'] = flag_match.group(1).strip()
                
                type_match = re.search(r'<VesselType[^>]*>(.*?)</VesselType>', vessel_section, re.IGNORECASE)
                if type_match:
                    vessel['VesselType'] = type_match.group(1).strip()
                
                vessels.append(vessel)
        
        logger.info(f"Fallback parser found {len(vessels)} vessels")
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
            
            # Log a sample of the response for debugging
            logger.debug(f"PSIX response sample: {response.text[:500]}")
            
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
