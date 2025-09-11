# src/maritime_mvp/clients/psix_client.py
"""
PSIX client – robust parser (diffgram/NewDataSet, escaped XML, Table/Table1, etc.)
Parses ALL columns from each row and normalizes common display fields.
"""
from __future__ import annotations
import os, re, html as _html, logging, requests, warnings
from typing import Any, Dict, List, Optional

from lxml import etree as ET

warnings.filterwarnings("ignore", message="Unverified HTTPS request")
logger = logging.getLogger(__name__)

PSIX_URL = os.getenv("PSIX_URL", "https://cgmix.uscg.mil/xml/PSIXData.asmx")
VERIFY_SSL = os.getenv("PSIX_VERIFY_SSL", "false").lower() in ("1", "true", "yes", "y")
TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

def _ln(tag: str) -> str:
    """local-name() helper for XPath fragments"""
    return f"local-name()='{tag}'"


class PsixClient:
    def __init__(self, url: str | None = None, verify_ssl: bool | None = None, timeout: int | None = None):
        self.url = url or PSIX_URL
        self.verify_ssl = VERIFY_SSL if verify_ssl is None else verify_ssl
        self.timeout = TIMEOUT if timeout is None else timeout
        self.session = requests.Session()
        self.session.verify = self.verify_ssl
        self.session.headers.update({
            "Content-Type": "text/xml; charset=utf-8",
            "Accept": "text/xml, application/soap+xml",
            "User-Agent": "MaritimeMVP/0.3"
        })
        logger.info(f"PSIX client initialized with URL: {self.url}")

    # --------------------------- public API ---------------------------

    def get_vessel_summary(
        self,
        *,
        vessel_id: int | None = None,
        vessel_name: str = "",
        call_sign: str = "",
        vin: str = "",
        hin: str = "",
        flag: str = "",
        service: str = "",
        build_year: str = ""
    ) -> Dict[str, Any]:
        """
        Call getVesselSummary. IMPORTANT: when not looking up a specific ID,
        send <VesselID>0</VesselID> or the service returns nothing.
        """
        vid = vessel_id if vessel_id is not None else 0

        # Known working namespaces for SOAPAction + body ns
        namespaces = ["https://cgmix.uscg.mil", "http://cgmix.uscg.mil"]

        for ns in namespaces:
            soap_action = f'"{ns}/getVesselSummary"'
            body = f"""<?xml version="1.0" encoding="utf-8"?>
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

            try:
                resp = self.session.post(self.url, data=body, headers={"SOAPAction": soap_action}, timeout=self.timeout)
                txt = resp.text

                # Extract the inner result payload
                m = re.search(
                    r"<getVesselSummaryResult[^>]*>(.*?)</getVesselSummaryResult>",
                    txt, re.IGNORECASE | re.DOTALL
                )
                if not m:
                    continue

                payload = m.group(1)

                # Unescape if the dataset is HTML-escaped
                if "&lt;" in payload or "&amp;lt;" in payload:
                    payload = _html.unescape(payload)

                rows = self._extract_rows(payload)
                if rows:
                    logger.info(f"Got response with namespace: {ns} — parsed {len(rows)} vessels")
                    return {"Table": rows}

                snippet = re.sub(r"\s+", " ", payload)[:400]
                logger.debug(f"PSIX result had no rows. Decoded snippet: {snippet}")

            except Exception as e:
                logger.debug(f"PSIX call failed for ns={ns}: {e}")

        logger.warning(f"No results found for vessel_name={vessel_name!r} vessel_id={vessel_id!r}")
        return {"Table": []}

    def search_by_name(self, name: str) -> Dict[str, Any]:
        return self.get_vessel_summary(vessel_name=name)

    def list_cases(self, vessel_id: int) -> Dict[str, Any]:
        logger.warning("list_cases not implemented in HTTP client")
        return {"Cases": []}

    def list_deficiencies(self, vessel_id: int, activity_number: str) -> Dict[str, Any]:
        logger.warning("list_deficiencies not implemented in HTTP client")
        return {"Deficiencies": []}

    # --------------------------- parsing ---------------------------

    def _extract_rows(self, xml_payload: str) -> List[Dict[str, Any]]:
        """
        Accepts:
          - diffgram/NewDataSet XML
          - plain NewDataSet XML
          - fragments with <Table>, <Table1>, ...
        Returns a list of dicts with ALL fields per row + normalized display fields.
        """
        rows: List[Dict[str, Any]] = []

        frag = self._slice_to_dataset(xml_payload)

        parser = LET.XMLParser(recover=True, huge_tree=True)
        try:
            root = LET.fromstring(frag.encode("utf-8"), parser=parser)
        except Exception:
            # last resort: wrap with a dummy root
            root = LET.fromstring(f"<root>{frag}</root>".encode("utf-8"), parser=parser)

        # Prefer rows under NewDataSet/* (Table, Table1, ...)
        dataset_nodes = root.xpath(".//*[local-name()='NewDataSet']") or [root]
        for ds in dataset_nodes:
            row_elems = ds.xpath(".//*[starts-with(local-name(), 'Table')]")
            if not row_elems:
                # fallback: any node that looks like a record (has a VesselName child)
                row_elems = ds.xpath(f".//*[./*[{_ln('VesselName')}]]")

            for elem in row_elems:
                rec = self._elem_to_record(elem)
                if rec:
                    self._normalize_row(rec)
                    rows.append(rec)

        # Fallback: scan the whole tree for record-ish nodes
        if not rows:
            for elem in root.xpath(f".//*[./*[{_ln('VesselName')}]]"):
                rec = self._elem_to_record(elem)
                if rec:
                    self._normalize_row(rec)
                    rows.append(rec)

        # Keep only rows that at least have a name or id
        rows = [r for r in rows if r.get("VesselName") or r.get("VesselID")]
        return rows

    def _elem_to_record(self, elem: LET._Element) -> Dict[str, Any]:
        """
        Convert a <Table…> node into a flat dict of ALL child <Field> values.
        """
        rec: Dict[str, Any] = {}

        # Prefer direct children as columns, but handle nested safely
        for child in elem:
            tag = self._local(child.tag)
            if not tag:
                continue
            # pull all text inside child (covers typed/nested nodes)
            val = "".join(child.itertext()).strip()
            if not val or val.lower() == "none":
                continue
            # strip CDATA if present
            if val.startswith("<![CDATA[") and val.endswith("]]>"):
                val = val[9:-3]
            rec[tag] = val

        # If for some reason the row uses deeper nesting, add those too (without clobbering)
        for sub in elem.xpath(".//*"):
            tag = self._local(sub.tag)
            if not tag or tag in rec:
                continue
            val = "".join(sub.itertext()).strip()
            if val and val.lower() != "none":
                rec[tag] = val

        return rec

    def _soap_call(self, op: str, body_xml: str) -> Dict[str, Any]:
        namespaces = ["https://cgmix.uscg.mil", "http://cgmix.uscg.mil"]
        for ns in namespaces:
            soap_action = f'"{ns}/{op}"'
            body = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                   xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <{op} xmlns="{ns}">{body_xml}</{op}>
      </soap:Body>
    </soap:Envelope>"""
            try:
                resp = self.session.post(self.url, data=body, headers={"SOAPAction": soap_action}, timeout=self.timeout)
                resp.raise_for_status()
            except requests.RequestException as e:
                logger.debug("PSIX %s call failed (ns=%s): %s", op, ns, e)
                continue
    
            txt = resp.text
            # Parse and find the *Result node by local-name (handles prefixes)
            try:
                root = ET.fromstring(txt.encode("utf-8"), parser=ET.XMLParser(recover=True, huge_tree=True))
                result_nodes = root.xpath(f".//*[local-name()='{op}Result']")
                if not result_nodes:
                    continue
                payload = "".join(result_nodes[0].itertext())
            except Exception as e:
                logger.debug("PSIX %s result parse error: %s", op, e)
                continue
    
            if "&lt;" in payload or "&amp;lt;" in payload:
                payload = _html.unescape(payload)
    
            rows = self._extract_rows(payload)
            return {"Table": rows}
        return {"Table": []}
    
    def get_vessel_particulars(self, vessel_id: int) -> Dict[str, Any]:
        return self._soap_call("getVesselParticulars", f"<VesselID>{vessel_id}</VesselID>")

    def get_vessel_dimensions(self, vessel_id: int) -> Dict[str, Any]:
        return self._soap_call("getVesselDimensions", f"<VesselID>{vessel_id}</VesselID>")

    def get_vessel_tonnage(self, vessel_id: int) -> Dict[str, Any]:
        return self._soap_call("getVesselTonnage", f"<VesselID>{vessel_id}</VesselID>")

    def get_vessel_documents(self, vessel_id: int) -> Dict[str, Any]:
        return self._soap_call("getVesselDocuments", f"<VesselID>{vessel_id}</VesselID>")

    def _normalize_row(self, rec: Dict[str, Any]) -> None:
        """
        Populate common display keys from PSIX's actual column names, if missing.
        (We do not overwrite existing values.)
        """
        def first(*keys: str) -> Optional[str]:
            for k in keys:
                v = rec.get(k)
                if v:
                    return v
            return None

        # IDs & name
        rec.setdefault("VesselID", first("VesselID", "VesselNumber", "ID", "id", "vesselid"))
        rec.setdefault("VesselName", first("VesselName", "Name", "name", "vesselname", "Vessel_Name"))

        # Display fields
        rec.setdefault("CallSign", first("CallSign", "VesselCallSign", "RadioCallSign", "Call_Sign", "callsign", "radio_callsign"))
        rec.setdefault("Flag", first(
            "Flag", "CountryLookupName", "CountryOfRegistry", "FlagName",
            "FlagOfRegistry", "FlagState", "FlagCountry", "Country", "FlagCode", "flag"
        ))
        rec.setdefault("VesselType", first(
            "VesselType", "ServiceType", "VesselService", "VesselTypeDescription", "ShipType", "Type", "vesseltype"
        ))
        rec.setdefault("GrossTonnage", first("GrossTonnage", "GrossTons", "GT", "Gross_Tonnage", "grosstonnage", "GrossRegisteredTonnage"))
        rec.setdefault("YearBuilt", first("YearBuilt", "ConstructionCompletedYear", "BuildYear", "YearOfBuild"))
        rec.setdefault("Status", first("Status", "StatusLookupName", "VesselStatus"))

        # Common identifiers (handy for the UI)
        rec.setdefault("IMONumber", first("IMONumber", "IMO", "IMO_Number"))
        rec.setdefault("OfficialNumber", first("OfficialNumber", "USOfficialNumber", "US_Official_Number"))
        rec.setdefault("PrimaryIdentification", first("PrimaryIdentification", "OfficialNumber", "IMONumber"))

    @staticmethod
    def _local(tag: str) -> str:
        """Strip XML namespace from a tag name."""
        if not tag:
            return ""
        return re.sub(r"^\{.*\}", "", tag)

    def _slice_to_dataset(self, s: str) -> str:
        """
        Pull out the most relevant XML fragment from the result string:
        diffgram → NewDataSet → Table…; otherwise return original string.
        """
        # diffgram wrapper
        m = re.search(r"<diffgr:diffgram[^>]*>.*?</diffgr:diffgram>", s, re.IGNORECASE | re.DOTALL)
        if m:
            return m.group(0)
        # plain NewDataSet
        m = re.search(r"<NewDataSet[^>]*>.*?</NewDataSet>", s, re.IGNORECASE | re.DOTALL)
        if m:
            return m.group(0)
        # or at least the tables
        m = re.search(r"(?:<Table\d?[^>]*>.*?</Table\d?>)+", s, re.IGNORECASE | re.DOTALL)
        if m:
            return f"<NewDataSet>{m.group(0)}</NewDataSet>"
        return s
