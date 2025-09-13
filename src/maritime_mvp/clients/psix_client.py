# src/maritime_mvp/clients/psix_client.py
"""
PSIX client — resilient SOAP wrapper + robust XML parser.

- Preserves embedded diffgram/NewDataSet XML inside <*Result> when present.
- Falls back to unescaped XML string payloads.
- Optional fallback to *XMLString operations.
- Namespace-agnostic parsing and Table/TableN extraction.

USCG PSIX web service returns .NET DataSets from the * dataset ops, and an XML
string from the *XMLString ops (documented on cgmix.uscg.mil).
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
        self.debug_callsign = (os.getenv("PSIX_DEBUG_CALLSIGN") or "").strip().upper() or None
        self.debug_dir = os.getenv("PSIX_DEBUG_DIR")
        self._debug_vids: set[int] = set()

        logger.info("PSIX client initialized url=%s verify_ssl=%s timeout=%ss", self.url, self.verify_ssl, self.timeout)

    # ---------------- Small utils ----------------
    @staticmethod
    def _digits(s: str) -> str:
        return re.sub(r"\D", "", s or "")

    @staticmethod
    def _looks_like_imo(num: str) -> bool:
        s = PsixClient._digits(num)
        if len(s) != 7:
            return False
        chk = sum(int(s[i]) * (7 - i) for i in range(6)) % 10
        return chk == int(s[6])

    def _debug_write(self, op: str, payload: str, vid: Optional[int] = None, tag: Optional[str] = None) -> None:
        if not payload:
            return
        try:
            if self.debug_dir:
                os.makedirs(self.debug_dir, exist_ok=True)
                ts = time.strftime("%Y%m%d-%H%M%S")
                vid_part = f"vid{vid}_" if vid is not None else ""
                tag_part = f"{tag}_" if tag else ""
                fn = os.path.join(self.debug_dir, f"psix_raw_{op}_{vid_part}{tag_part}{ts}.xml")
                with open(fn, "w", encoding="utf-8") as f:
                    f.write(payload)
                logger.info("PSIX raw saved: %s", fn)
            else:
                snippet = (payload[:1000]).replace("\n", " ")
                logger.debug(
                    "PSIX RAW %s %s%s: %s",
                    op,
                    f"vid={vid} " if vid is not None else "",
                    f"[{tag}] " if tag else "",
                    snippet,
                )
        except Exception:
            logger.exception("Failed to write PSIX raw payload for %s", op)


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
        Post SOAP for op and parse the DataSet rows robustly:
        - Prefer embedded XML (diffgram/NewDataSet) under <*Result>.
        - Fallback to unescaping string payloads.
        - If the dataset op returns zero rows, try the *XMLString variant once.
        - For non-summary ops, mine rows from arbitrary table-like XML if needed.
        Debugging:
          Set PSIX_DEBUG_CALLSIGN to capture raw summary payloads matching that callsign
          and all subsequent ops for that VesselID. Optionally set PSIX_DEBUG_DIR to save XML.
        Returns {'Table': [...]} or {'Table': []}. Only non-empty is cached.
        """
        ck = f"psix::{op}::{body_xml}"
        cached = _cache_get(ck)
        if cached is not None:
            return cached
    
        # --- inline debug helpers (no external methods required) ---
        dbg_callsign = (os.getenv("PSIX_DEBUG_CALLSIGN") or "").strip().upper() or None
        dbg_dir = os.getenv("PSIX_DEBUG_DIR")
        if not hasattr(self, "_debug_vids"):
            self._debug_vids: set[int] = set()
    
        def _debug_write(payload: str, vid: Optional[int] = None, tag: Optional[str] = None) -> None:
            if not payload:
                return
            try:
                if dbg_dir:
                    os.makedirs(dbg_dir, exist_ok=True)
                    ts = time.strftime("%Y%m%d-%H%M%S")
                    vid_part = f"_vid{vid}" if vid is not None else ""
                    tag_part = f"_{tag}" if tag else ""
                    fn = os.path.join(dbg_dir, f"psix_raw_{op}{vid_part}{tag_part}_{ts}.xml")
                    with open(fn, "w", encoding="utf-8") as f:
                        f.write(payload)
                    logger.info("PSIX raw saved: %s", fn)
                else:
                    snippet = (payload[:1000]).replace("\n", " ")
                    logger.debug("PSIX RAW %s %s%s: %s",
                                 op,
                                 f"vid={vid} " if vid is not None else "",
                                 f"[{tag}] " if tag else "",
                                 snippet)
            except Exception:
                logger.exception("Failed to write PSIX raw payload for %s", op)
    
        # For per-VID debug capture
        vid_in_body: Optional[int] = None
        try:
            mvid = re.search(r"<\s*VesselID\s*>\s*(\d+)\s*<\s*/\s*VesselID\s*>", body_xml, re.I)
            if mvid:
                vid_in_body = int(mvid.group(1))
        except Exception:
            vid_in_body = None
    
        # --- local row-mining fallback for non-summary ops (dims/tons/doc shapes) ---
        def _mine_rows_from_any(xml_text: str) -> List[Dict[str, Any]]:
            if not xml_text:
                return []
            try:
                parser = ET.XMLParser(recover=True, huge_tree=True)
                root = ET.fromstring(xml_text.encode("utf-8"), parser=parser)
            except Exception:
                return []
            # Prefer NewDataSet if present; else search whole tree
            scopes = root.xpath(".//*[local-name()='NewDataSet']") or [root]
            out: List[Dict[str, Any]] = []
            for scope in scopes:
                # Candidate "row" = element with >=2 element children whose children are leaves
                for elem in scope.xpath(".//*"):
                    kids = [c for c in elem if isinstance(c.tag, str)]
                    if len(kids) < 2:
                        continue
                    # Ensure children are mostly leaf-like (no nested element children)
                    if any(len([gc for gc in k if isinstance(gc.tag, str)]) for k in kids):
                        continue
                    rec: Dict[str, Any] = {}
                    for k in kids:
                        tag = re.sub(r"^\{.*\}", "", k.tag or "")
                        if not tag:
                            continue
                        val = "".join(k.itertext()).strip()
                        if val and val.lower() != "none":
                            rec[tag] = val
                    if rec:
                        out.append(rec)
            return out
    
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
    
                # Prefer embedded DataSet XML
                ds_node = None
                diff_nodes = result_node.xpath(".//*[local-name()='diffgram']")
                nds_nodes = result_node.xpath(".//*[local-name()='NewDataSet']")
                if diff_nodes:
                    ds_node = diff_nodes[0]
                elif nds_nodes:
                    ds_node = nds_nodes[0]
    
                if ds_node is not None:
                    payload = ET.tostring(ds_node, encoding="unicode")
                else:
                    # Fallback to string payload (possibly escaped XML)
                    str_nodes = result_node.xpath(".//*[local-name()='string']") or [result_node]
                    payload = "".join(str_nodes[0].itertext()) if str_nodes else "".join(result_node.itertext())
                    if payload and ("&lt;" in payload or "&amp;lt;" in payload):
                        payload = _html.unescape(payload)
    
                rows = self._extract_rows(payload)
    
                # Debug: record summary that matches callsign and remember VesselID(s)
                if op == "getVesselSummary" and dbg_callsign and rows:
                    csu = dbg_callsign
                    matched_vids: set[int] = set()
                    for r in rows:
                        rcs = str(r.get("CallSign") or r.get("VesselCallSign") or "").strip().upper()
                        if rcs and rcs == csu:
                            rid = r.get("VesselID") or r.get("VesselId") or r.get("vesselid")
                            try:
                                matched_vids.add(int(str(rid)))
                            except Exception:
                                pass
                    if matched_vids:
                        self._debug_vids.update(matched_vids)
                        _debug_write(payload, None, tag=f"callsign_{csu}")
    
                # Debug: record any op payload targeting a VID we care about
                if vid_in_body is not None and vid_in_body in self._debug_vids:
                    _debug_write(payload, vid_in_body)
    
                # Non-summary ops: if nothing extracted, try row-mining fallback
                if not rows and op != "getVesselSummary":
                    mined = _mine_rows_from_any(payload)
                    if mined:
                        rows = mined
    
                if rows:
                    data = {"Table": rows}
                    _cache_set(ck, data)  # cache only non-empty
                    return data
    
                # No rows from dataset op → try XMLString variant once
                snippet = (payload or "")[:240].replace("\n", " ").strip()
                logger.debug("PSIX %s returned no rows; payload head=%r", op, snippet)
    
                if self.try_xmlstring_fallback and not op.endswith("XMLString"):
                    try_op = f"{op}XMLString"
                    try_ck = f"psix::{try_op}::{body_xml}"
                    cached2 = _cache_get(try_ck)
                    if cached2 is not None:
                        return cached2
    
                    raw2 = self._post_soap_once(try_op, body_xml)
                    if raw2:
                        root2 = ET.fromstring(raw2.encode("utf-8"), parser=ET.XMLParser(recover=True, huge_tree=True))
                        result_nodes2 = root2.xpath(f".//*[local-name()='{try_op}Result']")
                        if result_nodes2:
                            txt = "".join(result_nodes2[0].itertext())
                            if txt and ("&lt;" in txt or "&amp;lt;" in txt):
                                txt = _html.unescape(txt)
                            rows2 = self._extract_rows(txt)
    
                            if not rows2 and op != "getVesselSummary":
                                mined2 = _mine_rows_from_any(txt)
                                if mined2:
                                    rows2 = mined2
    
                            # Debug for XMLString path too
                            if op == "getVesselSummary" and dbg_callsign and rows2:
                                csu = dbg_callsign
                                matched_vids2: set[int] = set()
                                for r in rows2:
                                    rcs = str(r.get("CallSign") or r.get("VesselCallSign") or "").strip().upper()
                                    if rcs and rcs == csu:
                                        rid = r.get("VesselID") or r.get("VesselId") or r.get("vesselid")
                                        try:
                                            matched_vids2.add(int(str(rid)))
                                        except Exception:
                                            pass
                                if matched_vids2:
                                    self._debug_vids.update(matched_vids2)
                                    _debug_write(txt, None, tag=f"callsign_{csu}")
    
                            if vid_in_body is not None and vid_in_body in self._debug_vids:
                                _debug_write(txt, vid_in_body)
    
                            data2 = {"Table": rows2}
                            if rows2:
                                _cache_set(try_ck, data2)  # cache only non-empty
                                return data2
                            else:
                                snippet2 = (txt or "")[:240].replace("\n", " ").strip()
                                logger.debug("PSIX %s fallback returned no rows; head=%r", try_op, snippet2)
    
                # Still nothing; return empty (and do NOT cache)
                return {"Table": []}
    
            except Exception as e:
                last_err = f"{type(e).__name__}: {e!s}"
                if attempt < attempts - 1:
                    time.sleep(self.backoff_s * (attempt + 1))
    
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
        Note: When not looking up a specific ID, PSIX requires <VesselID>0</VesselID>.
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

        # Common identifiers
        rec.setdefault("IMONumber", first("IMONumber", "IMO", "IMO_Number"))
        rec.setdefault("OfficialNumber", first("OfficialNumber", "USOfficialNumber", "US_Official_Number"))

        # Primary Identification: make sure we capture PSIX 'Identification'
        pid = first("PrimaryIdentification", "Identification", "Primary_ID", "PrimaryId")
        if pid:
            rec.setdefault("PrimaryIdentification", pid)

            # If IMO missing, infer from PID when valid
            if not rec.get("IMONumber") and self._looks_like_imo(pid):
                rec["IMONumber"] = self._digits(pid)

            # If Official missing and PID is a 6–7 digit number different from the IMO, use it
            if not rec.get("OfficialNumber"):
                d = self._digits(pid)
                if d and (len(d) in (6, 7)) and (rec.get("IMONumber") != d):
                    rec["OfficialNumber"] = d

    def _extract_rows(self, xml_payload: str, keep_all: bool = False) -> List[Dict[str, Any]]:
        """
        Accepts:
          - diffgram/NewDataSet subtree
          - plain NewDataSet
          - concatenated Table/TableN fragments
          - or an outer wrapper with those inside
    
        When keep_all=True, return all parsed rows (even if they lack VesselName/ID).
        This is required for dataset ops like dimensions/tonnage that often omit those fields.
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
                # Fallback: any node with a VesselName child (helps for summary-ish payloads)
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
    
        # Only filter out nameless/ID-less rows for summary; keep everything for details ops
        if not keep_all:
            rows = [r for r in rows if r.get("VesselName") or r.get("VesselID")]
    
        return rows
