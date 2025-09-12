"""
PSIX client — resilient SOAP wrapper + robust XML parser.

- Preserves embedded diffgram/NewDataSet XML inside <*Result> when present.
- Falls back to unescaped XML string payloads.
- Optional fallback to *XMLString operations.
- Namespace-agnostic parsing and Table/TableN extraction.

USCG PSIX web service returns .NET DataSets from the * dataset ops, and an XML
string from the *XMLString ops (documented on cgmix.uscg.mil) — behavior can vary
by operation and server patch level. See e.g. getOperationControls docs:
https://cgmix.uscg.mil/xml/PSIXData.asmx?op=getOperationControls
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

# Namespaces/roots to try (asmx servers can be picky about SOAPAction)
_NS_TRY = [
    "https://cgmix.uscg.mil",
    "http://cgmix.uscg.mil",
]
# SOAPAction variants to try for each ns/op
def _soap_actions(ns: str, op: str) -> List[str]:
    return [
        f"{ns}/{op}",
        f"{ns}/xml/PSIXData.asmx/{op}",
        f"{ns}/PSIXData.asmx/{op}",
    ]


def _ln(tag: str) -> str:
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
        try_xmlstring_fallback: bool = True,
    ) -> None:
        self.url = url or PSIX_URL
        self.verify_ssl = VERIFY_SSL if verify_ssl is None else verify_ssl
        self.timeout = REQUEST_TIMEOUT if timeout is None else timeout
        self.retries = max(0, retries)
        self.backoff_s = max(0.0, backoff_s)
        self.try_xmlstring_fallback = bool(try_xmlstring_fallback)

        self.session = requests.Session()
        self.session.verify = self.verify_ssl
        self.session.headers.update({
            "Content-Type": "text/xml; charset=utf-8",
            "Accept": "text/xml, application/soap+xml",
            "User-Agent": "MaritimeMVP/0.5 (+PSIX)",
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

    def _post_soap_once(self, op: str, body_xml: str) -> Optional[str]:
        """
        Try posting a SOAP request once per ns/action combo; return raw response text on success, else None.
        """
        for ns in _NS_TRY:
            envelope = self._soap_envelope(ns, op, body_xml)
            for action in _soap_actions(ns, op):
                try:
                    resp = self.session.post(
                        self.url,
                        data=envelope,
                        headers={"SOAPAction": f'"{action}"'},
                        timeout=self.timeout,
                    )
                    resp.raise_for_status()
                    return resp.text or ""
                except requests.RequestException:
                    continue
        return None

    def _post_soap(self, op: str, body_xml: str) -> Dict[str, Any]:
        """
        Post SOAP for `op` and parse the DataSet rows robustly:
        - Prefer embedded XML (diffgram/NewDataSet) under <*Result>.
        - Fallback to unescaping string payloads.
        - Optional retry using the *XMLString op if no rows found.
        Returns {'Table': [...]} or {'Table': []}.
        """
        # Cache
        ck = f"psix::{op}::{body_xml}"
        cached = _cache_get(ck)
        if cached is not None:
            return cached

        attempts = max(1, self.retries + 1)
        last_err: Optional[str] = None

        for attempt in range(attempts):
            try:
                raw = self._post_soap_once(op, body_xml)
                if not raw:
                    last_err = "no HTTP response or all SOAPAction variants failed"
                    raise RuntimeError(last_err)

                root = ET.fromstring(raw.encode("utf-8"), parser=ET.XMLParser(recover=True, huge_tree=True))
                result_nodes = root.xpath(f".//*[local-name()='{op}Result']")
                if not result_nodes:
                    last_err = f"{op}Result node not found"
                    raise RuntimeError(last_err)

                result_node = result_nodes[0]

                # 1) Embedded XML dataset under Result?
                ds_node = None
                # Prefer diffgram, then NewDataSet
                diff_nodes = result_node.xpath(".//*[local-name()='diffgram']")
                nds_nodes = result_node.xpath(".//*[local-name()='NewDataSet']")
                if diff_nodes:
                    ds_node = diff_nodes[0]
                elif nds_nodes:
                    ds_node = nds_nodes[0]

                if ds_node is not None:
                    payload = ET.tostring(ds_node, encoding="unicode")
                else:
                    # 2) String content variant (unescape)
                    # Some ASMXs wrap it in <string> or put text directly in *Result
                    str_nodes = result_node.xpath(".//*[local-name()='string']") or [result_node]
                    payload = "".join(str_nodes[0].itertext()) if str_nodes else "".join(result_node.itertext())
                    if payload and ("&lt;" in payload or "&amp;lt;" in payload):
                        payload = _html.unescape(payload)

                rows = self._extract_rows(payload)
                data = {"Table": rows}
                if rows:
                    _cache_set(ck, data)
                else:
                    # Helpful debug
                    snippet = (payload or "")[:240].replace("\n", " ").strip()
                    logger.debug("PSIX %s returned no rows; payload head=%r", op, snippet)
                return data

            except Exception as e:
                last_err = f"{type(e).__name__}: {e!s}"
                if attempt < attempts - 1:
                    time.sleep(self.backoff_s * (attempt + 1))

        # Primary op failed or produced no rows; optionally try *XMLString variant once
        if self.try_xmlstring_fallback and not op.endswith("XMLString"):
            try_op = f"{op}XMLString"
            try_ck = f"psix::{try_op}::{body_xml}"
            cached2 = _cache_get(try_ck)
            if cached2 is not None:
                return cached2

            try:
                raw = self._post_soap_once(try_op, body_xml)
                if raw:
                    root = ET.fromstring(raw.encode("utf-8"), parser=ET.XMLParser(recover=True, huge_tree=True))
                    result_nodes = root.xpath(f".//*[local-name()='{try_op}Result']")
                    if result_nodes:
                        txt = "".join(result_nodes[0].itertext())
                        if txt and ("&lt;" in txt or "&amp;lt;" in txt):
                            txt = _html.unescape(txt)
                        rows = self._extract_rows(txt)
                        data = {"Table": rows}
                        if rows:
                            _cache_set(try_ck, data)
                        else:
                            snippet = (txt or "")[:240].replace("\n", " ").strip()
                            logger.debug("PSIX %s fallback returned no rows; payload head=%r", try_op, snippet)
                        return data
            except Exception as e:  # pragma: no cover
                logger.debug("PSIX %s XMLString fallback failed: %s", op, e)

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
        getVesselSummary (dataset), with <VesselID>0</VesselID> when searching by attributes.
        Service docs and field list are published by USCG PSIX (returns .NET DataSet)
        on cgmix.uscg.mil.
        """
        vid = int(vessel_id) if vessel_id is not None else 0
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

    # ---------------- Parsing helpers ----------------

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

    def _elem_to_record(self, elem: ET._Element) -> Dict[str, Any]:
        rec: Dict[str, Any] = {}

        # Direct children -> columns
        for child in elem:
            tag = self._local(child.tag)
            if not tag:
                continue
            val = "".join(child.itertext()).strip()
            if not val or val.lower() == "none":
                continue
            # Strip CDATA wrapper if present
            if val.startswith("<![CDATA[") and val.endswith("]]>"):
                val = val[9:-3]
            rec[tag] = val

        # Non-clobbering descendants (fill gaps only)
        for sub in elem.xpath(".//*"):
            tag = self._local(sub.tag)
            if not tag or tag in rec:
                continue
            val = "".join(sub.itertext()).strip()
            if val and val.lower() != "none":
                rec[tag] = val

        return rec

    def _normalize_row(self, rec: Dict[str, Any]) -> None:
        """Populate common display keys if missing (case/alias tolerant)."""
        def first(*keys: str) -> Optional[str]:
            for k in keys:
                v = rec.get(k)
                if v:
                    return v
            return None

        rec.setdefault("VesselID", first("VesselID", "VesselId", "VesselNumber", "ID", "id", "vesselid"))
        rec.setdefault("VesselName", first("VesselName", "Name", "name", "vesselname", "Vessel_Name"))

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

        rec.setdefault("IMONumber", first("IMONumber", "IMO", "IMO_Number"))
        rec.setdefault("OfficialNumber", first("OfficialNumber", "USOfficialNumber", "US_Official_Number"))
        rec.setdefault("PrimaryIdentification", first("PrimaryIdentification", "OfficialNumber", "IMONumber"))

    def _extract_rows(self, xml_payload: str) -> List[Dict[str, Any]]:
        """
        Accepts:
          - diffgram/NewDataSet subtree
          - plain NewDataSet
          - concatenated Table/TableN fragments
          - or an outer wrapper with those inside
        Returns a list of dicts with normalized display fields.
        """
        frag = self._slice_to_dataset(xml_payload or "")

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

        rows = [r for r in rows if r.get("VesselName") or r.get("VesselID")]
        return rows
