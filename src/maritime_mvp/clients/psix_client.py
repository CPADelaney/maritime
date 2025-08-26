# src/maritime_mvp/clients/psix_client.py
"""
PSIX client – robust parser (diffgram/NewDataSet, escaped XML, Table/Table1, etc.)
"""
from __future__ import annotations
import os, re, html as _html, logging, requests, warnings
from typing import Any, Dict, List, Optional

from lxml import etree as LET  # lxml is already in your requirements

warnings.filterwarnings("ignore", message="Unverified HTTPS request")
logger = logging.getLogger(__name__)

PSIX_URL = os.getenv("PSIX_URL", "https://cgmix.uscg.mil/xml/PSIXData.asmx")
VERIFY_SSL = os.getenv("PSIX_VERIFY_SSL", "false").lower() in ("1", "true", "yes", "y")
TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

FIELDS = [
    "VesselID","VesselName","CallSign","IMONumber","OfficialNumber","Flag",
    "VesselType","GrossTonnage","NetTonnage","DeadWeight","YearBuilt","Builder",
    "HullMaterial","PropulsionType","DocumentationExpirationDate","COIExpirationDate",
    "Owner","Operator","ManagingOwner"
]

def _ln(tag: str) -> str:
    """local-name() helper for XPath fragments"""
    return f'local-name()="{tag}"'

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
        # MUST send 0 when searching by anything other than a specific ID
        vid = vessel_id if vessel_id is not None else 0

        # These two namespaces work in practice
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

                # Find the inner <getVesselSummaryResult> … </…>
                m = re.search(r"<getVesselSummaryResult[^>]*>(.*?)</getVesselSummaryResult>",
                              txt, re.IGNORECASE | re.DOTALL)
                if not m:
                    continue

                payload = m.group(1)
                # If it’s HTML-escaped XML (&lt;…&gt;), unescape it
                if "&lt;" in payload or "&amp;lt;" in payload:
                    payload = _html.unescape(payload)

                rows = self._extract_rows(payload)
                if rows:
                    logger.info(f"Got response with namespace: {ns} — parsed {len(rows)} vessels")
                    return {"Table": rows}

                # No rows? log a short safe snippet so we can see structure
                snippet = re.sub(r"\s+", " ", payload)[:400]
                logger.debug(f"PSIX result had no rows. Decoded snippet: {snippet}")

            except Exception as e:
                logger.debug(f"PSIX call failed for ns={ns}: {e}")

        logger.warning(f"No results found for vessel_name={vessel_name!r} vessel_id={vessel_id!r}")
        return {"Table": []}

    # ---------------- parsing ----------------

    def _extract_rows(self, xml_payload: str) -> List[Dict[str, Any]]:
        """
        Accepts either:
          - full diffgram/NewDataSet XML
          - raw DataSet XML
          - a fragment containing multiple <Table>, <Table1>, …
        Returns a list of {field:value} dicts.
        """
        rows: List[Dict[str, Any]] = []

        # Try to locate diffgram/NewDataSet fragment if the payload contains extra wrappers
        frag = self._slice_to_dataset(xml_payload)

        parser = LET.XMLParser(recover=True, huge_tree=True)
        try:
            root = LET.fromstring(frag.encode("utf-8"), parser=parser)
        except Exception:
            # last resort: wrap with a dummy root
            root = LET.fromstring(f"<root>{frag}</root>".encode("utf-8"), parser=parser)

        # Strategy A: rows under NewDataSet/* (often Table, Table1, …)
        dataset_nodes = root.xpath(".//*[local-name()='NewDataSet']") or [root]
        for ds in dataset_nodes:
            row_elems = ds.xpath(".//*[starts-with(local-name(), 'Table')]")
            if not row_elems:
                # fallback: any element that looks like a record (has a VesselName child)
                row_elems = ds.xpath(f".//*[./*[{_ln('VesselName')}]]")

            for elem in row_elems:
                rows.append(self._elem_to_record(elem))

        # Strategy B: if still empty, scan entire tree for record-ish nodes
        if not rows:
            for elem in root.xpath(f".//*[./*[{_ln('VesselName')}]]"):
                rows.append(self._elem_to_record(elem))

        # Keep only rows that at least have a name or id
        rows = [r for r in rows if r.get("VesselName") or r.get("VesselID")]
        return rows

    def _elem_to_record(self, elem: LET._Element) -> Dict[str, Any]:
        rec: Dict[str, Any] = {}
        for f in FIELDS:
            hit = elem.xpath(f".//*[local-name()='{f}']/text()")
            if hit:
                val = (hit[0] or "").strip()
                if val and val.lower() != "none":
                    rec[f] = val
        return rec

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

    # ------------- simple convenience -------------
    def search_by_name(self, name: str) -> Dict[str, Any]:
        return self.get_vessel_summary(vessel_name=name)

    def list_cases(self, vessel_id: int) -> Dict[str, Any]:
        logger.warning("list_cases not implemented in HTTP client")
        return {"Cases": []}

    def list_deficiencies(self, vessel_id: int, activity_number: str) -> Dict[str, Any]:
        logger.warning("list_deficiencies not implemented in HTTP client")
        return {"Deficiencies": []}
