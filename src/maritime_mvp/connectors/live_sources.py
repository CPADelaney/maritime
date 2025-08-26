# src/maritime_mvp/connectors/live_sources.py
from __future__ import annotations
import re
import time
import logging
from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional, Tuple, List
import httpx
from lxml import html
from zeep.helpers import serialize_object

# Reuse your existing SOAP client
from ..clients.psix_client import PsixClient

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 20
UA = "MaritimeMVP/0.2 (+https://maritime-mvp.onrender.com)"
CACHE_TTL_S = 900  # 15 min default

# Simple in-process TTL cache; swap with Redis later
_cache: Dict[str, Tuple[float, Any]] = {}

def _get_cached(key: str) -> Optional[Any]:
    v = _cache.get(key)
    if not v:
        return None
    exp, data = v
    if exp < time.time():
        _cache.pop(key, None)
        return None
    return data

def _set_cached(key: str, value: Any, ttl_s: int = CACHE_TTL_S) -> None:
    _cache[key] = (time.time() + ttl_s, value)

@dataclass
class VesselDoc:
    name: str
    expires_on: Optional[str]
    status: Optional[str]
    source: str

@dataclass
class PilotageInfo:
    provider: str
    url: str
    title: Optional[str]
    draft_limits: Optional[Dict[str, Any]]
    boarding_grounds: Optional[str]
    vhf_channel: Optional[str]
    advance_notice: Optional[str]

@dataclass
class LiveBundle:
    vessel: Dict[str, Any]              # PSIX summary row
    documents: List[VesselDoc]          # expiries/statuses we can infer
    pilotage: Dict[str, Any]            # snapshot of relevant pilot page(s)
    marine_exchange: Dict[str, Any]     # snapshot of MX page
    misp: Dict[str, Any]                # CA ballast fee snapshot
    cofr: Dict[str, Any]                # COFR search snapshot/link(s)
    alerts: List[str]                   # warnings about missing/expiring docs

# ---- generic HTML helpers ----------------------------------------------------

def fetch_html(url: str, *, ttl: int = CACHE_TTL_S, parse_extra: bool = False) -> Dict[str, Any]:
    """Fetch and lightly parse HTML pages for relevant maritime info."""
    ck = f"html::{url}"
    cached = _get_cached(ck)
    if cached:
        return cached
    
    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT, headers={"User-Agent": UA}) as c:
            r = c.get(url, follow_redirects=True)
            r.raise_for_status()
            tree = html.fromstring(r.text)
            title = (tree.xpath("//title/text()") or [""])[0].strip()
            
            # Extract visible text
            text_bits = tree.xpath("//body//*[not(self::script or self::style)]/text()")
            text = " ".join(t.strip() for t in text_bits if t.strip())
            
            # Look for common maritime patterns
            extra = {}
            if parse_extra:
                # VHF channels
                vhf_match = re.search(r"VHF.*?Channel[s]?\s*(\d+[A-B]?)", text, re.IGNORECASE)
                if vhf_match:
                    extra["vhf_channel"] = vhf_match.group(1)
                
                # Draft limits
                draft_match = re.search(r"(?:maximum|max).*?draft.*?(\d+\.?\d*)\s*(?:feet|ft|meters|m)", 
                                      text, re.IGNORECASE)
                if draft_match:
                    extra["max_draft"] = draft_match.group(1)
                
                # Advance notice requirements
                notice_match = re.search(r"(\d+)\s*(?:hours?|hrs?).*?advance.*?notice", 
                                       text, re.IGNORECASE)
                if notice_match:
                    extra["advance_notice_hours"] = notice_match.group(1)
                
                # Find PDF links (often contain tariffs)
                pdf_links = tree.xpath("//a[contains(@href, '.pdf')]/@href")
                if pdf_links:
                    extra["pdf_links"] = pdf_links[:5]  # limit to first 5
            
            snap = {
                "url": url,
                "title": title,
                "text_sample": (text[:1500] + "…") if len(text) > 1500 else text,
                "fetched_at": int(time.time()),
                **extra
            }
            _set_cached(ck, snap, ttl)
            return snap
    except Exception as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return {"url": url, "error": str(e), "fetched_at": int(time.time())}

# ---- PSIX enhanced wrapper ---------------------------------------------------

def psix_summary_by_name(name: str) -> Dict[str, Any]:
    """Get vessel summary from PSIX by name with caching."""
    ck = f"psix::name::{name.lower()}"
    if (v := _get_cached(ck)) is not None:
        return v
    
    try:
        client = PsixClient()
        raw = client.search_by_name(name)
        data = serialize_object(raw, dict)
        rows = (data or {}).get("Table") or []
        summary = rows[0] if rows else {}
        
        # Enhance with computed fields
        if summary:
            summary["_psix_match_count"] = len(rows)
            summary["_psix_fetched_at"] = int(time.time())
            
        _set_cached(ck, summary, 600)
        return summary
    except Exception as e:
        logger.error(f"PSIX search failed for {name}: {e}")
        return {"error": str(e), "vessel_name": name}

def psix_summary_by_id(vessel_id: int) -> Dict[str, Any]:
    """Get vessel summary from PSIX by ID with caching."""
    ck = f"psix::id::{vessel_id}"
    if (v := _get_cached(ck)) is not None:
        return v
    
    try:
        client = PsixClient()
        raw = client.get_vessel_summary(vessel_id=vessel_id)
        data = serialize_object(raw, dict)
        rows = (data or {}).get("Table") or []
        summary = rows[0] if rows else {}
        
        if summary:
            summary["_psix_fetched_at"] = int(time.time())
            
        _set_cached(ck, summary, 600)
        return summary
    except Exception as e:
        logger.error(f"PSIX lookup failed for ID {vessel_id}: {e}")
        return {"error": str(e), "vessel_id": vessel_id}

def _doc_field(row: Dict[str, Any], *keys: str) -> Optional[str]:
    """Extract field from PSIX row with case-insensitive key matching."""
    for k in keys:
        val = row.get(k) or row.get(k.lower())
        if val:
            return str(val)
    return None

def extract_docs_from_psix_row(row: Dict[str, Any]) -> List[VesselDoc]:
    """Extract document information from PSIX vessel data."""
    docs: List[VesselDoc] = []
    
    # Certificate of Documentation
    doc_exp = _doc_field(row, "DocumentationExpirationDate", "documentationexpirationdate")
    doc_stat = _doc_field(row, "DocumentationStatus", "documentationstatus")
    if doc_exp or doc_stat:
        docs.append(VesselDoc(
            name="USCG Certificate of Documentation",
            expires_on=doc_exp,
            status=doc_stat,
            source="PSIX",
        ))
    
    # Certificate of Inspection
    coi_exp = _doc_field(row, "COIExpirationDate", "coiexpirationdate")
    if coi_exp:
        docs.append(VesselDoc(
            name="USCG Certificate of Inspection",
            expires_on=coi_exp,
            status=None,
            source="PSIX",
        ))
    
    # Safety Management Certificate
    smc_exp = _doc_field(row, "SMCExpirationDate", "smcexpirationdate")
    if smc_exp:
        docs.append(VesselDoc(
            name="Safety Management Certificate",
            expires_on=smc_exp,
            status=None,
            source="PSIX",
        ))
    
    return docs

# ---- Regional Pilotage & Marine Exchange Registry ---------------------------

REGISTRY = {
    # Bay Area
    "sf_pilots": {
        "url": "https://sfbarpilots.com/new-operational/",
        "provider": "San Francisco Bar Pilots",
        "vhf": "10",
        "boarding": "SF Pilot Station"
    },
    "sf_mx": {
        "url": "https://www.sfmx.org/bay-area-committees/hsc/",
        "provider": "San Francisco Marine Exchange"
    },
    
    # Southern California
    "la_pilot": {
        "url": "https://www.portoflosangeles.org/business/pilot-service",
        "provider": "Los Angeles Pilot Service",
        "vhf": "73",
        "boarding": "LA/LB Pilot Station"
    },
    "lb_pilot": {
        "url": "https://www.jacobsenpilot.com/pilotage/",
        "provider": "Jacobsen Pilot Service",
        "vhf": "73",
        "boarding": "LA/LB Pilot Station"
    },
    "socal_mx": {
        "url": "https://mxsocal.org/",
        "provider": "Marine Exchange of Southern California"
    },
    
    # Puget Sound
    "ps_pilots": {
        "url": "https://www.pspilots.org/dispatch-information/general-guidelines-for-vessels/",
        "provider": "Puget Sound Pilots",
        "vhf": "13",
        "boarding": "Port Angeles Pilot Station"
    },
    "ps_mx": {
        "url": "https://marexps.com/",
        "provider": "Marine Exchange of Puget Sound"
    },
    
    # Columbia River
    "cr_pilots": {
        "url": "https://colrip.com/",
        "provider": "Columbia River Pilots",
        "vhf": "16/13",
        "boarding": "Astoria Pilot Station"
    },
    "cr_mx": {
        "url": "https://www.pdxmex.com/resources/",
        "provider": "Columbia River Marine Exchange"
    },
    
    # Other California
    "oak_pilot": {
        "url": "https://sfbarpilots.com/new-operational/",
        "provider": "San Francisco Bar Pilots (Oakland)",
        "vhf": "10"
    },
    "stockton_pilot": {
        "url": "https://sfbarpilots.com/new-operational/",
        "provider": "San Francisco Bar Pilots (Stockton)",
        "vhf": "10"
    },
    "sd_pilot": {
        "url": "https://www.sdmaritime.com/pilotage/",
        "provider": "San Diego Harbor Pilots",
        "vhf": "14"
    }
}

def pilot_snapshot_for_region(region: str) -> Dict[str, Any]:
    """Fetch pilotage information for a specific region."""
    pilots = {}
    
    if region == "bay_area":
        keys = ["sf_pilots", "oak_pilot", "stockton_pilot"]
    elif region == "socal":
        keys = ["la_pilot", "lb_pilot", "sd_pilot"]
    elif region == "puget":
        keys = ["ps_pilots"]
    elif region == "columbia":
        keys = ["cr_pilots"]
    else:
        return {}
    
    for key in keys:
        if key in REGISTRY:
            info = REGISTRY[key]
            snap = fetch_html(info["url"], parse_extra=True)
            snap.update({
                "provider": info["provider"],
                "vhf_channel": info.get("vhf"),
                "boarding_grounds": info.get("boarding")
            })
            pilots[key] = snap
    
    return pilots

def mx_snapshot_for_region(region: str) -> Dict[str, Any]:
    """Fetch Marine Exchange information for a specific region."""
    mx_map = {
        "bay_area": "sf_mx",
        "socal": "socal_mx",
        "puget": "ps_mx",
        "columbia": "cr_mx"
    }
    
    key = mx_map.get(region)
    if key and key in REGISTRY:
        info = REGISTRY[key]
        snap = fetch_html(info["url"], parse_extra=True)
        snap["provider"] = info["provider"]
        return {"primary": snap}
    return {}

# ---- California MISP (Marine Invasive Species Program) ----------------------

MISP_INFO = {
    "program": "California Marine Invasive Species Program (MISP)",
    "sites": [
        "https://www.slc.ca.gov/misp/",
        "https://cdtfa.ca.gov/taxes-and-fees/marine-invasive-species-fee/",
    ],
    "current_fee": "$1000 per voyage (300+ GT vessels)",
    "exemptions": ["Military", "Law enforcement", "Research vessels"]
}

MONEY_RE = re.compile(r"\$[\d,]+(?:\.\d{2})?")

def fetch_misp_snapshot() -> Dict[str, Any]:
    """Fetch current MISP fee information from California sources."""
    snaps = []
    for url in MISP_INFO["sites"]:
        snap = fetch_html(url)
        snaps.append(snap)
    
    # Extract dollar amounts from CDTFA page
    dollars = []
    if len(snaps) > 1:
        dollars = MONEY_RE.findall(snaps[-1].get("text_sample", ""))
    
    return {
        "program": MISP_INFO["program"],
        "current_fee": MISP_INFO["current_fee"],
        "exemptions": MISP_INFO["exemptions"],
        "pages": snaps,
        "possible_amounts_seen": list(dict.fromkeys(dollars))[:6],  # de-dupe & cap
        "effective_date": "2020-01-01"
    }

# ---- COFR (Certificate of Financial Responsibility) -------------------------

COFR_URLS = {
    "search": "https://publicsearch.npfc.uscg.mil/COFR/Default.aspx",
    "active_list": "https://www.uscg.mil/Mariners/National-Pollution-Funds-Center/COFRs/ECOFR-Active-Vessel-Status/",
    "api_check": "https://cgmix.uscg.mil/xml/COFRData.asmx?WSDL"  # if exists
}

def cofr_snapshot(vessel_name: Optional[str], imo_or_official_no: Optional[str]) -> Dict[str, Any]:
    """Get COFR lookup information and guidance."""
    snap = fetch_html(COFR_URLS["search"], parse_extra=True)
    
    # Build search guidance
    guidance = []
    if vessel_name:
        guidance.append(f"Search by vessel name: '{vessel_name}'")
    if imo_or_official_no:
        guidance.append(f"Search by IMO/Official #: '{imo_or_official_no}'")
    
    # Check if vessel needs COFR (>300 GT tankers, all vessels >400 GT)
    cofr_required = {
        "tankers_over_300gt": True,
        "vessels_over_400gt": True,
        "exceptions": ["Public vessels", "Oil spill response vessels"]
    }
    
    return {
        "entrypoint": COFR_URLS["search"],
        "active_list": COFR_URLS["active_list"],
        "page": snap,
        "query": {"vessel_name": vessel_name, "id": imo_or_official_no},
        "search_guidance": guidance,
        "requirements": cofr_required
    }

# ---- Additional Document Checks ----------------------------------------------

def check_document_alerts(docs: List[VesselDoc]) -> List[str]:
    """Generate alerts for missing or expiring documents."""
    alerts = []
    
    # Check for expiring docs
    from datetime import datetime, timedelta
    today = datetime.now().date()
    warning_days = 30
    
    for doc in docs:
        if doc.expires_on:
            try:
                exp_date = datetime.strptime(doc.expires_on[:10], "%Y-%m-%d").date()
                days_until = (exp_date - today).days
                
                if days_until < 0:
                    alerts.append(f"⚠️ {doc.name} EXPIRED {abs(days_until)} days ago")
                elif days_until <= warning_days:
                    alerts.append(f"⚠️ {doc.name} expires in {days_until} days")
            except (ValueError, TypeError):
                pass
    
    # Check for missing critical docs
    doc_names = [d.name.lower() for d in docs]
    if "certificate of documentation" not in " ".join(doc_names):
        alerts.append("📋 Certificate of Documentation not found in PSIX")
    
    return alerts

# ---- Region Detection --------------------------------------------------------

def choose_region(port_code: Optional[str], port_name: Optional[str], 
                 state: Optional[str], is_cascadia: Optional[bool]) -> str:
    """Determine the maritime region based on port information."""
    
    # Port code mapping (your internal codes)
    code_map = {
        "LALB": "socal",
        "SFBAY": "bay_area",
        "PUGET": "puget",
        "COLRIV": "columbia",
        "STKN": "bay_area",
        "OAK": "bay_area",
        "SDG": "socal"
    }
    
    if port_code and port_code in code_map:
        return code_map[port_code]
    
    # Name-based detection
    name = (port_name or "").lower()
    if any(x in name for x in ["san francisco", "oakland", "richmond", "stockton", "sacramento"]):
        return "bay_area"
    if any(x in name for x in ["los angeles", "long beach", "san diego", "hueneme"]):
        return "socal"
    if any(x in name for x in ["seattle", "tacoma", "everett", "olympia", "bellingham"]):
        return "puget"
    if any(x in name for x in ["portland", "astoria", "columbia", "vancouver usa"]):
        return "columbia"
    
    # State-based fallback
    st = (state or "").upper()
    if st == "CA":
        return "bay_area"  # default California
    elif st == "WA" or is_cascadia:
        return "puget"
    elif st == "OR":
        return "columbia"
    
    return "bay_area"  # final default

# ---- Main Orchestrator -------------------------------------------------------

def build_live_bundle(*,
                     vessel_name: Optional[str] = None,
                     vessel_id: Optional[int] = None,
                     port_code: Optional[str] = None,
                     port_name: Optional[str] = None,
                     state: Optional[str] = None,
                     is_cascadia: Optional[bool] = None,
                     imo_or_official_no: Optional[str] = None) -> Dict[str, Any]:
    """
    Build a comprehensive bundle of live maritime data for a vessel and port.
    
    This orchestrates calls to multiple sources and returns a unified response
    with vessel info, documents, pilotage, marine exchange, fees, and alerts.
    """
    
    logger.info(f"Building live bundle for vessel={vessel_name}, port={port_code}")
    
    # 1) Fetch vessel data from PSIX
    if vessel_id is not None:
        vrow = psix_summary_by_id(vessel_id)
    elif vessel_name:
        vrow = psix_summary_by_name(vessel_name)
    else:
        vrow = {}
    
    # Extract documents and check for alerts
    docs = extract_docs_from_psix_row(vrow) if vrow else []
    alerts = check_document_alerts(docs)
    
    # 2) Determine maritime region
    region = choose_region(port_code, port_name, state, is_cascadia)
    logger.info(f"Selected region: {region}")
    
    # 3) Fetch regional information
    pilot = pilot_snapshot_for_region(region)
    mx = mx_snapshot_for_region(region)
    
    # 4) California-specific fees
    misp = {}
    if (state or "").upper() == "CA":
        misp = fetch_misp_snapshot()
    
    # 5) COFR lookup
    cofr = cofr_snapshot(
        vessel_name=vessel_name or vrow.get("VesselName") or vrow.get("vesselname"),
        imo_or_official_no=imo_or_official_no or vrow.get("IMONumber") or vrow.get("OfficialNumber")
    )
    
    # 6) Build final bundle
    bundle = LiveBundle(
        vessel=vrow,
        documents=[asdict(d) for d in docs],
        pilotage=pilot,
        marine_exchange=mx,
        misp=misp,
        cofr=cofr,
        alerts=alerts
    )
    
    return asdict(bundle)

# ---- Utility Functions -------------------------------------------------------

def clear_cache():
    """Clear the in-process cache (useful for testing or forced refresh)."""
    global _cache
    _cache.clear()
    logger.info("Cache cleared")

def get_cache_stats() -> Dict[str, int]:
    """Get current cache statistics."""
    now = time.time()
    active = sum(1 for exp, _ in _cache.values() if exp > now)
    expired = len(_cache) - active
    return {
        "total_entries": len(_cache),
        "active_entries": active,
        "expired_entries": expired
    }
