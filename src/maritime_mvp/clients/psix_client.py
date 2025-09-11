# src/maritime_mvp/clients/psix_client.py
"""
PSIX client — resilient SOAP wrapper + robust XML parser.

Features:
- Namespace-agnostic SOAP result parsing (handles https/http namespaces and prefixes).
- Unescapes embedded DataSet payloads (diffgram/NewDataSet/TableN).
- Normalizes common columns (VesselID/Name/CallSign/Flag/Type/IMO/etc.).
- Exposes core ops used by the app:
  * getVesselSummary (with <VesselID>0</VesselID> when searching)
  * getVesselParticulars
  * getVesselDimensions
  * getVesselTonnage
  * getVesselDocuments

References:
- PSIX SOAP ops and request/response examples: getVesselTonnage, getVesselDocuments
  [cgmix.uscg.mil](https://cgmix.uscg.mil/xml/PSIXData.asmx?op=getVesselTonnage),
  [cgmix.uscg.mil](https://cgmix.uscg.mil/xml/PSIXData.asmx?op=getVesselDocuments)
- PSIX Vessel Search UI (for manual verification)
  [cgmix.uscg.mil](https://cgmix.uscg.mil/PSIX/PSIXSearch.aspx)
- PSIX search behavior (wildcards, max rows)
  [cgmix.uscg.mil](https://cgmix.uscg.mil/PSIX/Definitions.aspx)
"""
from __future__ import annotations

import os
import re
import time
import html as _html
import logging
import warnings
from typing import Any, Dict, List, Optional, Tuple

import requests
from lxml import etree as ET

warnings.filterwarnings("ignore", message="Unverified HTTPS request")
logger = logging.getLogger(__name__)

# ---------------- Configuration ----------------
PSIX_URL = os.getenv("PSIX_URL", "https://cgmix.uscg.mil/xml/PSIXData.asmx")
VERIFY_SSL = os.getenv("PSIX_VERIFY_SSL", "false").lower() in ("1", "true", "yes", "y")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

# Simple in-process TTL cache for idempotent calls
_CACHE_TTL = int(os.getenv("PSIX_CACHE_TTL", "600"))  # seconds
_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}

# Namespaces PSIX commonly uses; try https first, then http
_NS_TRY = ["https://cgmix.uscg.mil", "http://cgmix.uscg.mil"]


def _ln(tag: str) -> str:
    """XPath helper for local-name() matching."""
    return f"local-name()='{tag}'"


def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    if not _CACHE_TTL:
        return None
    v = _CACHE.get(key)
    if not v:
        return None
    exp, data = v
    if exp <= time.time():
        _CACHE.pop(key, None)
        return None
    return data


def _cache_set(key: str, value: Dict[str, Any], ttl: Optional[int] = None) -> None:
    if ttl is None:
        ttl = _CACHE_TTL
    if ttl <= 0:
        return
    _CACHE[key] = (time.time() + ttl, value)


class PsixClient:
    def __init__(
        self,
        url: str | None = None,
        verify_ssl: bool | None = None,
        timeout: int | None = None,
        retries: int = 1,
        backoff_s: float = 0.5,
    ) -> None:
        self.url = url or PSIX_URL
        self.verify_ssl = VERIFY_SSL if verify_ssl is None else verify_ssl
        self.timeout = REQUEST_TIMEOUT if timeout is None else timeout
        self.retries = max(0, retries)
        self.backoff_s = max(0.0, backoff_s)

        self.session = requests.Session()
        self.session.verify = self.verify_ssl
        self.session.headers.update({
            "Content-Type": "text/xml; charset=utf-8",
            "Accept": "text/xml, application/soap+xml",
            "User-Agent": "MaritimeMVP/0.4 (+PSIX)",
        })

        logger.info("PSIX client initialized url=%s verify_ssl=%s timeout=%ss", self.url, self.verify_ssl, self.timeout)

    # ---------------- SOAP core ----------------

    def _soap_envelope(self, ns: str, op: str, inner_xml: str) -> str:
        return f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <{op} xmlns="{ns}">{inner_xml}</{op}>
  </soap:Body>
</soap:Envelope>"""

    def _post_soap(self, op: str, body_xml: str) -> Dict[str, Any]:
        """
        Post a SOAP request for `op` with `body_xml` content under the op node.
        - Tries https/http namespaces.
        - Returns {'Table': [...]} or {'Table': []} on failure.
        """
        # Cache key
        ck = f"psix::{op}::{body_xml}"
        cached = _cache_get(ck)
        if cached is not None:
            return cached

        last_err: Optional[str] = None
        for ns in _NS_TRY:
            soap_action = f'"{ns}/{op}"'
            envelope = self._soap_envelope(ns, op, body_xml)

            attempt = 0
            while True:
                try:
                    resp = self.session.post(self.url, data=envelope, headers={"SOAPAction": soap_action}, timeout=self.timeout)
                    resp.raise_for_status()
                    txt = resp.text or ""

                    # Parse XML; find <*Result> by local-name
                    root = ET.fromstring(txt.encode("utf-8"), parser=ET.XMLParser(recover=True, huge_tree=True))
                    result_nodes = root.xpath(f".//*[local-name()='{op}Result']")
                    if not result_nodes:
                        last_err = f"{op}Result node not found (ns={ns})"
                        break

                    payload = "".join(result_nodes[0].itertext())
                    if "&lt;" in payload or "&amp;lt;" in payload:
                        payload = _html.unescape(payload)

                    rows = self._extract_rows(payload)
                    data = {"Table": rows}
                    if rows:
                        _cache_set(ck, data)           # cache non-empty for normal TTL
                    else:
                        # either don't cache empties or cache briefly
                        # _cache_set(ck, data, ttl=30)  # 30s, optional
                        pass
                    return data

                except requests.RequestException as e:
                    last_err = f"HTTP error: {e!s}"
                except Exception as e:
                    last_err = f"Parse error: {e!s}"

                if attempt >= self.retries:
                    break
                attempt += 1
                time.sleep(self.backoff_s * attempt)

        if last_err:
            logger.debug("PSIX _post_soap(%s) failed: %s", op, last_err)
        return {"Table": []}

    # ---------------- Public API ----------------

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
        build_year: str = "",
    ) -> Dict[str, Any]:
        """
        Wrapper for getVesselSummary.
        Note: When not looking up a specific ID, PSIX requires <VesselID>0</VesselID> in the request.
        """
        vid = int(vessel_id) if vessel_id is not None else 0
        # Minimal XML; values may be empty strings
        inner = (
            f"<VesselID>{vid}</VesselID>"
            f"<VesselName>{_html.escape(vessel_name or '')}</VesselName>"
            f"<CallSign>{_html.escape(call_sign or '')}</CallSign>"
            f"<VIN>{_html.escape(vin or '')}</VIN>"
            f"<HIN>{_html.escape(hin or '')}</HIN>"
            f"<Flag>{_html.escape(flag or '')}</Flag>"
            f"<Service>{_html.escape(service or '')}</Service>"
            f"<BuildYear>{_html.escape(build_year or '')}</BuildYear>"
        )
        return self._post_soap("getVesselSummary", inner)

    def get_vessel_particulars(self, vessel_id: int) -> Dict[str, Any]:
        inner = f"<VesselID>{int(vessel_id)}</VesselID>"
        return self._post_soap("getVesselParticulars", inner)

    def get_vessel_dimensions(self, vessel_id: int) -> Dict[str, Any]:
        inner = f"<VesselID>{int(vessel_id)}</VesselID>"
        return self._post_soap("getVesselDimensions", inner)

    def get_vessel_tonnage(self, vessel_id: int) -> Dict[str, Any]:
        inner = f"<VesselID>{int(vessel_id)}</VesselID>"
        return self._post_soap("getVesselTonnage", inner)

    def get_vessel_documents(self, vessel_id: int) -> Dict[str, Any]:
        inner = f"<VesselID>{int(vessel_id)}</VesselID>"
        return self._post_soap("getVesselDocuments", inner)

    # Stubs for other PSIX ops you may add later:
    # def get_vessel_cases(self, vessel_id: int) -> Dict[str, Any]: ...
    # def get_operation_controls(self, activity_id: int) -> Dict[str, Any]: ...
    # def get_vessel_deficiencies(self, vessel_id: int) -> Dict[str, Any]: ...

    # ---------------- Parsing helpers ----------------

    def _extract_rows(self, xml_payload: str) -> List[Dict[str, Any]]:
        """
        Accepts:
          - diffgram/NewDataSet
          - plain NewDataSet
          - inline Table/TableN fragments
        Returns a list of dicts; all raw fields preserved; common display fields normalized.
        """
        frag = self._slice_to_dataset(xml_payload)

        parser = ET.XMLParser(recover=True, huge_tree=True)
        try:
            root = ET.fromstring(frag.encode("utf-8"), parser=parser)
        except Exception:
            root = ET.fromstring(f"<root>{frag}</root>".encode("utf-8"), parser=parser)

        rows: List[Dict[str, Any]] = []

        dataset_nodes = root.xpath(".//*[local-name()='NewDataSet']") or [root]
        for ds in dataset_nodes:
            row_elems = ds.xpath(".//*[starts-with(local-name(), 'Table')]")
            if not row_elems:
                # Fallback: any node with a VesselName child
                row_elems = ds.xpath(f".//*[./*[{_ln('VesselName')}]]")

            for elem in row_elems:
                rec = self._elem_to_record(elem)
                if rec:
                    self._normalize_row(rec)
                    rows.append(rec)

        if not rows:
            # Global fallback: any record-ish node with VesselName
            for elem in root.xpath(f".//*[./*[{_ln('VesselName')}]]"):
                rec = self._elem_to_record(elem)
                if rec:
                    self._normalize_row(rec)
                    rows.append(rec)

        # Keep rows with at least a name or id
        rows = [r for r in rows if r.get("VesselName") or r.get("VesselID")]
        return rows

    def _elem_to_record(self, elem: ET._Element) -> Dict[str, Any]:
        rec: Dict[str, Any] = {}

        # Direct children as columns
        for child in elem:
            tag = self._local(child.tag)
            if not tag:
                continue
            val = "".join(child.itertext()).strip()
            if not val or val.lower() == "none":
                continue
            # Strip CDATA
            if val.startswith("<![CDATA[") and val.endswith("]]>"):
                val = val[9:-3]
            rec[tag] = val

        # Deeper descendants (without clobbering existing)
        for sub in elem.xpath(".//*"):
            tag = self._local(sub.tag)
            if not tag or tag in rec:
                continue
            val = "".join(sub.itertext()).strip()
            if val and val.lower() != "none":
                rec[tag] = val

        return rec

    def _normalize_row(self, rec: Dict[str, Any]) -> None:
        """Populate common display keys if missing."""
        def first(*keys: str) -> Optional[str]:
            for k in keys:
                v = rec.get(k)
                if v:
                    return v
            return None

        # IDs & name
        rec.setdefault("VesselID", first("VesselID", "VesselId", "VesselNumber", "ID", "id", "vesselid"))
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

        # Common identifiers
        rec.setdefault("IMONumber", first("IMONumber", "IMO", "IMO_Number"))
        rec.setdefault("OfficialNumber", first("OfficialNumber", "USOfficialNumber", "US_Official_Number"))
        rec.setdefault("PrimaryIdentification", first("PrimaryIdentification", "OfficialNumber", "IMONumber"))

    @staticmethod
    def _local(tag: str) -> str:
        if not tag:
            return ""
        return re.sub(r"^\{.*\}", "", tag)

    def _slice_to_dataset(self, s: str) -> str:
        """
        Pull the inner dataset-like fragment:
        - diffgr:diffgram ... /diffgr:diffgram
        - NewDataSet ... /NewDataSet
        - Concatenated TableN rows
        """
        m = re.search(r"<diffgr:diffgram[^>]*>.*?</diffgr:diffgram>", s, re.IGNORECASE | re.DOTALL)
        if m:
            return m.group(0)
        m = re.search(r"<NewDataSet[^>]*>.*?</NewDataSet>", s, re.IGNORECASE | re.DOTALL)
        if m:
            return m.group(0)
        m = re.search(r"(?:<Table\d?[^>]*>.*?</Table\d?>)+", s, re.IGNORECASE | re.DOTALL)
        if m:
            return f"<NewDataSet>{m.group(0)}</NewDataSet>"
        return s
