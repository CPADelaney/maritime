from __future__ import annotations

import os
import logging
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Literal

from fastapi import FastAPI, HTTPException, Query, Response, Body, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select, text
from sqlalchemy.orm import Session

# v2 router (enhanced endpoints)
from .routes import router as v2_router

# Optional faster JSON (falls back gracefully if orjson isn't installed)
try:
    from fastapi.responses import ORJSONResponse as DefaultJSONResponse  # type: ignore
    _USE_ORJSON = True
except Exception:  # pragma: no cover
    DefaultJSONResponse = JSONResponse  # type: ignore
    _USE_ORJSON = False

from ..db import SessionLocal, init_db
from ..rules.fee_engine import (
    FeeEngine,
    EstimateContext,
    VesselSpecs,
    VoyageContext,
    VesselType,
)
from ..clients.psix_client import PsixClient
from ..connectors.live_sources import (
    build_live_bundle,
    clear_cache,
    get_cache_stats,
)
from ..models import Port, Fee, Source

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("maritime-api")

API_VERSION = "2.0.0"

# ---------- HTML fallbacks (declare BEFORE use) ----------
FALLBACK_LANDING_HTML = """
<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Maritime MVP</title>
<script src="https://cdn.tailwindcss.com"></script></head>
<body class="bg-gradient-to-br from-blue-50 to-indigo-100 min-h-screen">
<div class="container mx-auto px-4 py-16 max-w-4xl">
<div class="bg-white rounded-2xl shadow-xl p-8">
<div class="text-center mb-8">
<h1 class="text-4xl font-bold text-gray-800 mb-4">🚢 Maritime MVP</h1>
<p class="text-lg text-gray-600">Port Call Fee Estimation & Vessel Intelligence</p>
</div>
<div class="grid md:grid-cols-2 gap-6 mb-8">
  <a href="/app" class="block p-6 bg-blue-50 rounded-xl hover:bg-blue-100 transition">
    <h2 class="text-xl font-semibold text-blue-900 mb-2">📊 Launch Dashboard</h2>
    <p class="text-blue-700">Access the application</p>
  </a>
  <a href="/api/docs" class="block p-6 bg-green-50 rounded-xl hover:bg-green-100 transition">
    <h2 class="text-xl font-semibold text-green-900 mb-2">📚 API Docs</h2>
    <p class="text-green-700">Explore endpoints</p>
  </a>
</div>
<div class="border-t pt-6">
  <h3 class="font-semibold mb-3">Quick Links:</h3>
  <div class="flex flex-wrap gap-3">
    <a href="/health" class="px-3 py-1 bg-gray-100 rounded hover:bg-gray-200">Health</a>
    <a href="/ports" class="px-3 py-1 bg-gray-100 rounded hover:bg-gray-200">Ports</a>
    <a href="/vessels/search?name=MAERSK&limit=25" class="px-3 py-1 bg-gray-100 rounded hover:bg-gray-200">Sample Search</a>
  </div>
</div>
</div></div></body></html>
"""

FALLBACK_FRONTEND_HTML = """
<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Maritime MVP - Port Call Estimator</title>
<script src="https://cdn.tailwindcss.com"></script>
<script defer src="https://cdn.jsdelivr.net/npm/alpinejs@3.x.x/dist/cdn.min.js"></script>
</head>
<body class="bg-gray-50" x-data="maritimeApp()">
<div class="container mx-auto px-4 py-8 max-w-6xl">
  <div class="bg-white rounded-lg shadow mb-6 p-6">
    <h1 class="text-2xl font-bold text-gray-800">🚢 Maritime Port Call Estimator</h1>
    <p class="text-gray-600 mt-2">Search vessels, select ports, and calculate fees</p>
  </div>
  <div class="grid md:grid-cols-3 gap-6">
    <div class="bg-white rounded-lg shadow p-6">
      <h2 class="text-lg font-semibold mb-4">Vessel Search</h2>
      <input x-model="vesselName" @keyup.enter="searchVessel"
             type="text" placeholder="Enter vessel name..."
             class="w-full p-2 border rounded">
      <button @click="searchVessel"
              class="w-full mt-2 bg-blue-600 text-white py-2 rounded hover:bg-blue-700">
        Search
      </button>
      <div x-show="searching" class="mt-4 text-center text-gray-600">Searching...</div>
      <template x-if="totalCount !== null">
        <div class="mt-2 text-xs text-slate-500">Showing <span x-text="vesselResults.length"></span> of <span x-text="totalCount"></span> results</div>
      </template>
      <div x-show="vesselResults.length > 0" class="mt-3 max-h-48 overflow-y-auto">
        <template x-for="v in vesselResults" :key="(v.VesselID||v.OfficialNumber||v.IMONumber||v.VesselName)">
          <button @click="selectVessel(v)"
                  class="w-full text-left p-2 border-b hover:bg-gray-50">
            <div class="font-medium" x-text="v.VesselName || v.vesselname"></div>
            <div class="text-xs text-slate-500">
              <span x-text="'Call sign: ' + (v.CallSign||'—')"></span> ·
              <span x-text="'Flag: ' + (v.Flag||'—')"></span>
            </div>
          </button>
        </template>
      </div>
    </div>
    <div class="bg-white rounded-lg shadow p-6">
      <h2 class="text-lg font-semibold mb-4">Port & ETA</h2>
      <div class="mb-3">
        <label class="block text-sm font-medium mb-1">Selected Vessel</label>
        <input x-model="selectedVesselName" readonly class="w-full p-2 border rounded bg-gray-50">
      </div>
      <div class="mb-3">
        <label class="block text-sm font-medium mb-1">Port</label>
        <select x-model="selectedPort" class="w-full p-2 border rounded"></select>
      </div>
      <div class="mb-3">
        <label class="block text-sm font-medium mb-1">ETA</label>
        <input x-model="eta" type="date" class="w-full p-2 border rounded">
      </div>
      <div class="mb-3">
        <label class="block text-sm font-medium mb-1">Arrival Type</label>
        <select x-model="arrivalType" class="w-full p-2 border rounded">
          <option value="FOREIGN">Foreign</option>
          <option value="COASTWISE">Coastwise</option>
        </select>
      </div>
      <button @click="calculateEstimate" :disabled="!selectedPort || !eta"
              class="w-full bg-green-600 text-white py-2 rounded hover:bg-green-700 disabled:opacity-50">
        Calculate Estimate
      </button>
    </div>
    <div class="bg-white rounded-lg shadow p-6">
      <h2 class="text-lg font-semibold mb-4">Estimate Results</h2>
      <div x-show="!estimate" class="text-gray-500">No estimate calculated yet</div>
      <div x-show="estimate">
        <div class="space-y-2">
          <template x-for="item in (estimate?.line_items || [])" :key="item.code">
            <div class="flex justify-between text-sm">
              <span x-text="item.name"></span>
              <span class="font-mono" x-text="'$' + item.amount"></span>
            </div>
          </template>
        </div>
        <div class="mt-4 pt-4 border-t">
          <div class="flex justify-between font-bold">
            <span>Total</span>
            <span class="text-lg" x-text="'$' + (estimate?.total || '0.00')"></span>
          </div>
        </div>
        <p class="text-xs text-gray-500 mt-4" x-text="estimate?.disclaimer"></p>
      </div>
    </div>
  </div>
</div>
<script>
const API_BASE = window.location.origin;
function maritimeApp() {
  return {
    vesselName: '', vesselResults: [], totalCount: null,
    selectedVessel: null, selectedVesselName: '',
    selectedPort: '', eta: new Date().toISOString().split('T')[0],
    arrivalType: 'FOREIGN', ports: [], estimate: null, searching: false,
    async init() {
      try { const r = await fetch(API_BASE + '/ports'); this.ports = await r.json(); }
      catch (e) { console.error('Failed to load ports:', e); }
    },
    async searchVessel() {
      if (!this.vesselName.trim()) return;
      this.searching = true; this.vesselResults = []; this.totalCount = null;
      try {
        const r = await fetch(API_BASE + '/vessels/search?limit=25&name=' + encodeURIComponent(this.vesselName));
        const data = await r.json();
        this.vesselResults = Array.isArray(data.Table) ? data.Table : [];
        this.totalCount = data.total ?? this.vesselResults.length;
      } catch (e) { console.error('Search failed:', e); alert('Vessel search failed.'); }
      finally { this.searching = false; }
    },
    selectVessel(v) { this.selectedVessel = v; this.selectedVesselName = v.VesselName || v.vesselname || ''; this.vesselResults = []; },
    async calculateEstimate() {
      if (!this.selectedPort || !this.eta) { alert('Please select a port and ETA'); return; }
      const params = new URLSearchParams({ port_code: this.selectedPort, eta: this.eta, arrival_type: this.arrivalType });
      try { const r = await fetch(API_BASE + '/estimate?' + params); this.estimate = await r.json(); }
      catch (e) { console.error('Estimate failed:', e); alert('Failed to calculate estimate.'); }
    }
  }
}
</script></body></html>
"""

# ---------- App ----------
app = FastAPI(
    title="Maritime Port Call Estimator",
    version=API_VERSION,
    description="Port call fee estimator with live data integration and v2 comprehensive calculations",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    # Uses ORJSONResponse if available; otherwise JSONResponse.
    default_response_class=DefaultJSONResponse,  # type: ignore[arg-type]
)

# Mount v2 router (enhanced endpoints under /api/v2)
app.include_router(v2_router)

# ----- CORS -----
_allow = os.getenv("ALLOW_ORIGINS") or os.getenv("ALLOWED_ORIGINS", "*")
allow_origins: List[str] = [o.strip() for o in _allow.split(",") if o.strip()] if _allow else ["*"]
allow_all = (len(allow_origins) == 1 and allow_origins[0] == "*")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    # Browsers disallow credentials with "*"; use regex echo when fully open.
    allow_credentials=not allow_all,
    allow_methods=["*"],
    allow_headers=["*"],
    allow_origin_regex=".*" if allow_all else None,
)

from fastapi import Depends

def _first_nonempty(*vals):
    for v in vals:
        if v is None: continue
        s = str(v).strip()
        if s and s.lower() != "none":
            return s
    return None

def _q(q, *keys):
    # accept multiple casing/alias variants
    for k in keys:
        v = q.get(k)
        if v is not None:
            s = str(v).strip()
            if s and s.lower() != "none":
                return s
    return None

@app.get("/vessels/summary", tags=["Vessels"])
def vessels_summary(
    request: Request,
    vessel_id: Optional[int] = Query(None),
    callsign: Optional[str] = Query(None),        # keep for docs, but we’ll read raw query below
    vessel_name: Optional[str] = Query(None),
    flag: Optional[str] = Query(None),
    service: Optional[str] = Query(None),
    build_year: Optional[str] = Query(None),
):
    client = PsixClient()
    if vessel_id:
        return client.get_vessel_summary(vessel_id=vessel_id)

    q = request.query_params
    cs = _q(q, "callsign", "call_sign", "CallSign")
    nm = _q(q, "vessel_name", "name", "VesselName")

    return client.get_vessel_summary(
        vessel_id=None,
        vessel_name=nm or "",
        call_sign=cs or "",
        flag=flag or "",
        service=service or "",
        build_year=build_year or "",
    )

@app.get("/vessels/details", tags=["Vessels"])
def vessels_details(
    request: Request,
    vessel_id: Optional[int] = Query(None),
    callsign: Optional[str] = Query(None),
    vessel_name: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """
    Resolve VesselID via summary (if needed), then fetch particulars, dimensions,
    tonnage, and documents. Returns {"rows":[{...}]} with helpful fields:
      LOA_m, Beam_m, Depth_m (feet → meters), GrossTonnage, NetTonnage,
    plus raw PSIX rows under _dimension_rows, _tonnage_rows, _documents.
    """
    client = PsixClient()

    # Normalize query variants (callsign/CallSign/call_sign and vessel_name/VesselName/name)
    q = request.query_params
    cs = _q(q, "callsign", "call_sign", "CallSign") or callsign
    nm = _q(q, "vessel_name", "name", "VesselName") or vessel_name

    vid: Optional[int] = vessel_id
    summary_first: Dict[str, Any] = {}

    # 1) If no VesselID provided, resolve it via getVesselSummary (prefer exact callsign match)
    if not vid:
        summ = client.get_vessel_summary(
            vessel_id=None,
            vessel_name=nm or "",
            call_sign=cs or "",
        )
        srows = (summ or {}).get("Table") or []
        if not srows:
            return {"rows": []}  # best-effort empty

        summary_first = srows[0]
        if cs:
            csu = cs.strip().upper()
            match = next(
                (
                    r for r in srows
                    if str(r.get("CallSign", "")).strip().upper() == csu
                    or str(r.get("VesselCallSign", "")).strip().upper() == csu
                ),
                None
            )
            if match:
                summary_first = match

        raw_id = _first_nonempty(
            summary_first.get("VesselID"),
            summary_first.get("VesselId"),        # PSIX sometimes uses this casing
            summary_first.get("vesselid"),
            summary_first.get("VesselNumber"),
            summary_first.get("ID"),
            summary_first.get("id"),
        )
        try:
            vid = int(str(raw_id)) if raw_id is not None else None
        except Exception:
            vid = None

    # If still no ID, return summary-only (UI can still use name/callsign/flag)
    if not vid:
        return {"rows": [summary_first] if summary_first else []}

    # 2) Safe PSIX calls (don’t let one failure 500 the whole route)
    def _get_table(fn, _vid: int, label: str) -> List[Dict[str, Any]]:
        try:
            d = fn(_vid)  # each returns {"Table": [...]}
            return (d or {}).get("Table") or []
        except Exception:
            logger.exception("PSIX %s failed for VesselID=%s", label, _vid)
            return []

    parts = _get_table(client.get_vessel_particulars, vid, "particulars")
    dims  = _get_table(client.get_vessel_dimensions,  vid, "dimensions")
    tons  = _get_table(client.get_vessel_tonnage,     vid, "tonnage")
    docs  = _get_table(client.get_vessel_documents,   vid, "documents")
    summ  = _get_table(lambda v: client.get_vessel_summary(vessel_id=v), vid, "summary")

    # 3) Merge base details (summary + particulars)
    base: Dict[str, Any] = {}
    if summ:
        base.update(summ[0])
    if parts:
        base.update(parts[0])

    # 4) Best-of selection for dimensions across all rows (feet → meters)
    def to_float(x: Any) -> Optional[float]:
        try:
            return float(str(x).strip())
        except Exception:
            return None

    len_ft = [to_float(d.get("LengthInFeet"))  for d in dims if d.get("LengthInFeet")  is not None]
    brd_ft = [to_float(d.get("BreadthInFeet")) for d in dims if d.get("BreadthInFeet") is not None]
    dep_ft = [to_float(d.get("DepthInFeet"))   for d in dims if d.get("DepthInFeet")   is not None]

    extra: Dict[str, Any] = {}
    if any(v is not None for v in len_ft):
        L = max(v for v in len_ft if v is not None)
        extra["LengthInFeet"] = L
        extra["LOA_m"] = L * 0.3048
    if any(v is not None for v in brd_ft):
        B = max(v for v in brd_ft if v is not None)
        extra["BreadthInFeet"] = B
        extra["Beam_m"] = B * 0.3048
    if any(v is not None for v in dep_ft):
        D = max(v for v in dep_ft if v is not None)
        extra["DepthInFeet"] = D
        extra["Depth_m"] = D * 0.3048  # Depth is NOT Draft

    # 5) Tonnage: prefer rows labeled Gross/Net; otherwise use highest numeric
    def label(t: Dict[str, Any]) -> str:
        return str(t.get("TonnageTypeLookupName", "")).lower()

    gross_vals = [to_float(t.get("MeasureOfWeight")) for t in tons if "gross" in label(t)]
    net_vals   = [to_float(t.get("MeasureOfWeight")) for t in tons if "net"   in label(t)]
    if not any(v is not None for v in gross_vals):
        gross_vals = [to_float(t.get("MeasureOfWeight")) for t in tons]
    if not any(v is not None for v in net_vals):
        net_vals   = [to_float(t.get("MeasureOfWeight")) for t in tons]

    if any(v is not None for v in gross_vals):
        extra["GrossTonnage"] = max(v for v in gross_vals if v is not None)
    if any(v is not None for v in net_vals):
        extra["NetTonnage"] = max(v for v in net_vals if v is not None)

    logger.info(
        "PSIX details: vid=%s dims=%d tons=%d parts=%d docs=%d",
        vid, len(dims), len(tons), len(parts), len(docs)
    )

    merged = {
        **base,
        **extra,
        "VesselID": vid,
        "_documents": docs,
        "_tonnage_rows": tons,
        "_dimension_rows": dims,
    }
    return {"rows": [merged]}
# ----- Frontend mounting -----
def find_frontend_dir() -> Optional[Path]:
    candidates = [
        Path("frontend"),
        Path("../frontend"),
        Path("../../frontend"),
        Path(__file__).parent.parent.parent.parent / "frontend",
    ]
    for p in candidates:
        if p.exists() and p.is_dir():
            logger.info("Found frontend directory at: %s", p.resolve())
            return p.resolve()
    logger.warning("Frontend directory not found; falling back to inline HTML")
    return None

frontend_dir = find_frontend_dir()
if frontend_dir:
    app.mount("/static", StaticFiles(directory=str(frontend_dir), html=True), name="static")

@app.get("/api/v2/vessels/details", tags=["Vessels"])
def v2_vessels_details(
    request: Request,
    vessel_id: Optional[int] = Query(None),
    callsign: Optional[str] = Query(None),
    vessel_name: Optional[str] = Query(None),
):
    return vessels_details(request, vessel_id, callsign, vessel_name)

# ----- Startup -----
@app.on_event("startup")
def _startup():
    """Initialize database on startup (and optionally run Alembic)."""
    try:
        init_db()
        logger.info("Startup complete, DB initialized.")
    except Exception:
        logger.exception("DB init failed during startup; continuing without blocking app.")

    # Optional: run Alembic migrations if configured
    if os.getenv("ALEMBIC_AUTO", "0") in ("1", "true", "TRUE", "yes", "YES"):
        try:
            from alembic import command
            from alembic.config import Config
            cfg_path = Path("alembic.ini")
            if cfg_path.exists():
                alembic_cfg = Config(str(cfg_path))
                command.upgrade(alembic_cfg, "head")
                logger.info("Alembic migrations completed.")
            else:
                logger.warning("ALEMBIC_AUTO=1 but alembic.ini not found; skipping migrations.")
        except Exception:
            logger.exception("Alembic migration failed; continuing without blocking app.")

    if frontend_dir:
        logger.info("Frontend available at /app")
    else:
        logger.warning("Frontend not mounted - directory not found")

# ----- Root & Frontend -----
@app.get("/", include_in_schema=False, response_class=HTMLResponse, response_model=None)
def root() -> Response:
    if frontend_dir and (frontend_dir / "index.html").exists():
        return RedirectResponse(url="/app")
    return HTMLResponse(FALLBACK_LANDING_HTML)

@app.get("/app", include_in_schema=False, response_class=HTMLResponse, response_model=None)
def app_root() -> Response:
    if frontend_dir and (frontend_dir / "index.html").exists():
        return FileResponse(frontend_dir / "index.html")
    return HTMLResponse(FALLBACK_FRONTEND_HTML)

@app.get("/app/{path:path}", include_in_schema=False, response_class=HTMLResponse, response_model=None)
def app_static(path: str) -> Response:
    if frontend_dir:
        file_path = frontend_dir / path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
        index = frontend_dir / "index.html"
        if index.exists():
            return FileResponse(index)
    return HTMLResponse(FALLBACK_FRONTEND_HTML)

# ----- System -----
@app.get("/health", tags=["System"])
def health() -> Dict[str, Any]:
    db_ok = True
    try:
        with SessionLocal() as db:
            db.execute(text("SELECT 1")).scalar()
    except Exception:
        db_ok = False
    return {
        "ok": True,
        "version": API_VERSION,
        "db_ok": db_ok,
        "cache_stats": get_cache_stats(),
        "frontend_available": frontend_dir is not None,
        "features": [
            "legacy_estimator",
            "v2_comprehensive_estimator",
            "psix_search",
            "live_port_bundle",
            "alembic_optional",
        ],
    }

# ----- Ports -----
@app.get("/ports", tags=["Ports"])
def list_ports() -> List[Dict[str, Any]]:
    db: Session = SessionLocal()
    try:
        rows = db.execute(select(Port).order_by(Port.name)).scalars().all()
        return [
            {
                "code": p.code,
                "name": p.name,
                "state": p.state,
                "country": p.country,
                "region": p.region,
                "is_california": p.is_california,
                "is_cascadia": p.is_cascadia,
                "pilotage_url": p.pilotage_url,
                "mx_url": p.mx_url,
                "tariff_url": p.tariff_url,
            }
            for p in rows
        ]
    except Exception:
        logger.exception("Failed to list ports")
        raise HTTPException(status_code=500, detail="ports query failed")
    finally:
        db.close()

@app.get("/ports/{port_code}", tags=["Ports"])
def get_port(port_code: str) -> Dict[str, Any]:
    db: Session = SessionLocal()
    try:
        port = db.execute(select(Port).where(Port.code == port_code)).scalar_one_or_none()
        if not port:
            raise HTTPException(status_code=404, detail=f"Port {port_code} not found")
        return {
            "code": port.code,
            "name": port.name,
            "state": port.state,
            "country": port.country,
            "region": port.region,
            "is_california": port.is_california,
            "is_cascadia": port.is_cascadia,
            "pilotage_url": port.pilotage_url,
            "mx_url": port.mx_url,
            "tariff_url": port.tariff_url,
        }
    except HTTPException:
        raise
    except Exception:
        logger.exception("Failed to get port %s", port_code)
        raise HTTPException(status_code=500, detail="port query failed")
    finally:
        db.close()

# ----- Vessels (PSIX) -----
import time

# simple in-process cache (key -> (exp_ts, payload))
_SEARCH_CACHE: dict[str, tuple[float, dict]] = {}
_SEARCH_TTL = 300  # 5 minutes

def _search_cache_get(key: str) -> Optional[dict]:
    v = _SEARCH_CACHE.get(key)
    if not v:
        return None
    exp, data = v
    if exp <= time.time():
        _SEARCH_CACHE.pop(key, None)
        return None
    return data

def _search_cache_set(key: str, data: dict, ttl: int = _SEARCH_TTL) -> None:
    _SEARCH_CACHE[key] = (time.time() + ttl, data)

@app.get("/vessels/search", tags=["Vessels"])
def search_vessels(
    name: str = Query(..., description="Vessel name to search for"),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
) -> Any:
    """
    Search PSIX by name and return a paginated, trimmed list.
    Hardened to fail fast if PSIX is slow/unavailable and to cache results briefly.
    """
    started = time.time()
    cache_key = f"{name.strip().upper()}::{page}::{limit}"
    cached = _search_cache_get(cache_key)
    if cached is not None:
        return cached

    # Use a shorter timeout and no retries for interactive search
    client = PsixClient(timeout=12, retries=0)  # keep this tight to avoid proxy timeouts

    try:
        raw = client.search_by_name(name)  # {"Table": [...]}
    except Exception as e:
        logger.exception("PSIX search failed for name=%r", name)
        # Return a controlled 502 rather than letting the proxy time out
        raise HTTPException(status_code=502, detail="upstream PSIX search failed")

    rows = (raw or {}).get("Table") or []
    def _nm(r): return (r.get("VesselName") or r.get("vesselname") or "").upper()
    def _cs(r): return (r.get("CallSign") or r.get("callsign") or "").upper()
    rows.sort(key=lambda r: (_nm(r), _cs(r)))

    total = len(rows)
    pages = max((total + limit - 1) // limit, 1)
    page_ = min(max(page, 1), pages)
    start_idx = (page_ - 1) * limit
    end_idx = start_idx + limit
    page_rows = rows[start_idx:end_idx]

    def pick(r):
        return {
            "VesselID": r.get("VesselID") or r.get("vesselid"),
            "VesselName": r.get("VesselName") or r.get("vesselname"),
            "CallSign": r.get("CallSign") or r.get("callsign"),
            "Flag": r.get("Flag") or r.get("flag"),
            "VesselType": r.get("VesselType") or r.get("vesseltype"),
            "IMONumber": r.get("IMONumber") or r.get("imonumber"),
            "OfficialNumber": r.get("OfficialNumber") or r.get("officialnumber"),
            "GrossTonnage": r.get("GrossTonnage") or r.get("grosstonnage"),
            "NetTonnage": r.get("NetTonnage") or r.get("nettonnage"),
        }

    trimmed = [pick(r) for r in page_rows if (r.get("VesselName") or r.get("vesselname"))]
    start_human = (start_idx + 1) if total else 0
    end_human = min(end_idx, total)

    payload = {
        "Table": trimmed,
        "total": total,
        "count": len(trimmed),
        "page": page_,
        "limit": limit,
        "pages": pages,
        "has_prev": page_ > 1,
        "has_next": page_ < pages,
        "start": start_human,
        "end": end_human,
    }
    _search_cache_set(cache_key, payload)
    logger.info("PSIX search name=%r total=%d elapsed=%.2fs", name, total, time.time() - started)
    return payload
@app.get("/vessels/{vessel_id}", tags=["Vessels"])
def get_vessel_by_id(vessel_id: int) -> Dict[str, Any]:
    client = PsixClient()
    try:
        return client.get_vessel_summary(vessel_id=vessel_id)
    except Exception as e:
        logger.exception("PSIX lookup failed for ID %s", vessel_id)
        raise HTTPException(status_code=502, detail=f"PSIX lookup failed: {e!s}")

# ----- Fee Estimation (Legacy/simple) -----
@app.get("/estimate", tags=["Estimates"])
def estimate(
    port_code: str = Query(..., description="Port code (e.g., LALB, USOAK, USSFO)"),
    eta: date = Query(..., description="Estimated time of arrival"),
    arrival_type: Literal["FOREIGN", "COASTWISE"] = Query("FOREIGN"),
    net_tonnage: Optional[Decimal] = Query(None),
    ytd_cbp_paid: Decimal = Query(Decimal("0")),
    include_optional: bool = Query(False),
) -> Dict[str, Any]:
    db: Session = SessionLocal()
    try:
        port = db.execute(select(Port).where(Port.code == port_code)).scalar_one_or_none()
        if not port:
            raise HTTPException(status_code=404, detail=f"Port {port_code} not found")
        engine = FeeEngine(db)
        ctx = EstimateContext(
            port_code=port_code,
            arrival_date=eta,
            arrival_type=str(arrival_type),
            net_tonnage=net_tonnage,
            ytd_cbp_paid=ytd_cbp_paid,
        )
        items = engine.compute(ctx)
        total = sum((i.amount for i in items), Decimal("0.00"))

        optional_services: List[Dict[str, Any]] = []
        if include_optional:
            optional_services = [
                {"service": "Pilotage", "estimated_low": 5000, "estimated_high": 15000, "note": "Varies by size/draft"},
                {"service": "Tugboat Assist", "estimated_low": 3000, "estimated_high": 8000, "note": "Depends on maneuvering"},
                {"service": "Launch Service", "estimated_low": 500, "estimated_high": 1500, "note": "Crew/supplies"},
                {"service": "Line Handling", "estimated_low": 1000, "estimated_high": 2500, "note": "Mooring/unmooring"},
            ]

        return {
            "port_code": port_code,
            "port_name": port.name,
            "eta": str(eta),
            "arrival_type": str(arrival_type),
            "line_items": [
                {"code": i.code, "name": i.name, "amount": str(i.amount), "details": i.details}
                for i in items
            ],
            "optional_services": optional_services,
            "total": str(total),
            "total_with_optional_low": str(total + sum(s["estimated_low"] for s in optional_services)),
            "total_with_optional_high": str(total + sum(s["estimated_high"] for s in optional_services)),
            "disclaimer": "Estimate only. Verify against official tariffs/guidance and your negotiated contracts.",
        }
    except HTTPException:
        raise
    except Exception:
        logger.exception("Estimate failed")
        raise HTTPException(status_code=500, detail="estimate calculation failed")
    finally:
        db.close()

# ----- IMO/UN LOCODE search (uses locode column) -----
@app.get("/imo_ports/search", tags=["Ports"])
def search_imo_ports(q: str = Query(..., min_length=2), limit: int = Query(20, ge=1, le=100)):
    db: Session = SessionLocal()
    try:
        rows = db.execute(text("""
            SELECT locode, port_name, country_code, country_name
            FROM imo_ports
            WHERE locode ILIKE :q OR port_name ILIKE :q
            ORDER BY CASE WHEN locode ILIKE :starts THEN 0 ELSE 1 END, port_name
            LIMIT :limit
        """), {"q": f"%{q}%", "starts": f"{q}%", "limit": limit}).mappings().all()
        return list(rows)
    finally:
        db.close()

@app.get("/imo_ports/{locode}", tags=["Ports"])
def get_imo_port(locode: str):
    db: Session = SessionLocal()
    try:
        row = db.execute(text("""
            SELECT locode, port_name, country_code, country_name
            FROM imo_ports WHERE locode = :u
        """), {"u": locode}).mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="UN/LOCODE not found")
        return dict(row)
    finally:
        db.close()

# UN/LOCODE -> internal region mapping
UNLOCODE_TO_INTERNAL = {
    # Bay Area
    "USOAK": "SFBAY",
    "USSFO": "SFBAY",
    "USSTK": "STKN",
    "USSAC": "STKN",
    # Puget Sound
    "USSEA": "PUGET",
    "USTAC": "PUGET",
    # Columbia River
    "USPDX": "COLRIV",
    "USAST": "COLRIV",  # Astoria
    # Southern California (combined internal region)
    "USLAX": "LALB",
    "USLGB": "LALB",
}

# ----- Fee Estimation (v2 Comprehensive) -----
@app.post("/v2/estimate", tags=["Estimates"])
def estimate_v2(
    vessel_name: str = Body(..., embed=True),
    vessel_type: Literal[
        "container","tanker","bulk_carrier","cruise","roro","general_cargo","lng","vehicle_carrier"
    ] = Body("general_cargo", embed=True),
    gross_tonnage: Decimal = Body(Decimal("0"), ge=0, embed=True),
    net_tonnage: Decimal = Body(Decimal("0"), ge=0, embed=True),
    loa_meters: Decimal = Body(Decimal("0"), ge=0, embed=True),
    draft_meters: Decimal = Body(Decimal("0"), ge=0, embed=True),
    previous_port_code: str = Body(..., embed=True, description="UN/LOCODE like CNSHA or USLAX"),
    arrival_port_code: str = Body(..., embed=True, description="UN/LOCODE like USOAK, USSEA, USPDX"),
    next_port_code: Optional[str] = Body(None, embed=True),
    eta: Optional[datetime] = Body(None, embed=True),
    etd: Optional[datetime] = Body(None, embed=True),
    days_alongside: int = Body(2, ge=1, embed=True),
    ytd_cbp_paid: Decimal = Body(Decimal("0"), ge=0, embed=True),
    tonnage_year_paid: Decimal = Body(Decimal("0"), ge=0, embed=True),
) -> Dict[str, Any]:
    """
    Comprehensive estimator using vessel specs + voyage context.
    Supports arrival as UN/LOCODE and maps to internal `ports.code` when needed.
    POST JSON body with the fields above (flat body, embedded).
    """
    db: Session = SessionLocal()
    try:
        # ---- Resolve arrival port to an internal Port row ----
        requested_unloc = (arrival_port_code or "").strip().upper()
        resolved_code = requested_unloc  # assume caller used an internal code already

        # Try direct match against internal ports table
        port = db.execute(select(Port).where(Port.code == resolved_code)).scalar_one_or_none()

        # If not found, try UN/LOCODE → internal mapping
        used_mapping = False
        if not port:
            mapped = UNLOCODE_TO_INTERNAL.get(requested_unloc)
            if mapped:
                resolved_code = mapped
                port = db.execute(select(Port).where(Port.code == resolved_code)).scalar_one_or_none()
                used_mapping = port is not None

        if not port:
            raise HTTPException(
                status_code=404,
                detail=f"Arrival port '{arrival_port_code}' not supported yet (add to ports table or mapping)."
            )

        # ---- Build engine and contexts ----
        engine = FeeEngine(db)
        engine.ytd_cbp_paid = ytd_cbp_paid
        engine.tonnage_year_paid = tonnage_year_paid

        vtype = VesselType(vessel_type)
        vessel = VesselSpecs(
            name=vessel_name,
            vessel_type=vtype,
            gross_tonnage=gross_tonnage,
            net_tonnage=net_tonnage,
            loa_meters=loa_meters,
            draft_meters=draft_meters,
        )

        prev_unloc = (previous_port_code or "").strip().upper()
        next_unloc = ((next_port_code or "").strip().upper() or None)

        # IMPORTANT: pass internal port code to the FeeEngine
        voyage = VoyageContext(
            previous_port_code=prev_unloc,
            arrival_port_code=resolved_code,
            next_port_code=next_unloc,
            eta=eta or datetime.utcnow(),
            etd=etd,
            days_alongside=max(1, int(days_alongside or 1)),
        )

        result = engine.calculate_comprehensive(vessel, voyage)

        # ---- Quick totals convenience ----
        try:
            mand = Decimal(result["totals"]["mandatory"])
            low = Decimal(result["totals"]["optional_low"])
            high = Decimal(result["totals"]["optional_high"])
        except Exception:
            mand = Decimal("0"); low = Decimal("0"); high = Decimal("0")

        result["quick_totals"] = {
            "mandatory": str(mand),
            "with_optional_low": str(mand + low),
            "with_optional_high": str(mand + high),
        }

        # Echo both what the caller sent and what we resolved to internally
        result["port"] = {
            "arrival_unlocode": requested_unloc,
            "resolved_internal_code": port.code,
            "used_mapping": used_mapping,
            "name": port.name,
            "state": port.state,
            "country": port.country,
            "is_california": port.is_california,
            "is_cascadia": port.is_cascadia,
        }
        return result

    except HTTPException:
        raise
    except Exception:
        logger.exception("Comprehensive estimate failed")
        raise HTTPException(status_code=500, detail="v2 estimate calculation failed")
    finally:
        db.close()

# ----- Live Data Bundle -----
@app.get("/live/portbundle", tags=["Live Data"])
def live_port_bundle(
    vessel_name: Optional[str] = Query(None),
    vessel_id: Optional[int] = Query(None),
    port_code: Optional[str] = Query(None),
    port_name: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    is_cascadia: Optional[bool] = Query(None),
    imo_or_official_no: Optional[str] = Query(None),
) -> Dict[str, Any]:
    # If only code provided, enrich from DB
    if port_code and not (port_name or state or is_cascadia is not None):
        db: Session = SessionLocal()
        try:
            p = db.execute(select(Port).where(Port.code == port_code)).scalar_one_or_none()
            if p:
                port_name = p.name
                state = p.state
                is_cascadia = p.is_cascadia
        finally:
            db.close()
    try:
        return build_live_bundle(
            vessel_name=vessel_name,
            vessel_id=vessel_id,
            port_code=port_code,
            port_name=port_name,
            state=state,
            is_cascadia=is_cascadia,
            imo_or_official_no=imo_or_official_no,
        )
    except Exception as e:
        logger.exception("live bundle failed")
        raise HTTPException(status_code=502, detail=f"live data aggregation failed: {e!s}")

@app.get("/live/pilotage/{port_code}", tags=["Live Data"])
def get_pilotage_info(port_code: str) -> Dict[str, Any]:
    from ..connectors.live_sources import choose_region, pilot_snapshot_for_region
    db: Session = SessionLocal()
    try:
        port = db.execute(select(Port).where(Port.code == port_code)).scalar_one_or_none()
        if not port:
            raise HTTPException(status_code=404, detail=f"Port {port_code} not found")
        region = choose_region(port_code, port.name, port.state, port.is_cascadia)
        pilotage = pilot_snapshot_for_region(region)
        return {"port_code": port_code, "port_name": port.name, "region": region, "pilotage": pilotage}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Pilotage info failed for %s", port_code)
        raise HTTPException(status_code=500, detail=f"pilotage lookup failed: {e!s}")
    finally:
        db.close()

# ----- Fees -----
@app.get("/fees", tags=["Fees"])
def list_fees(
    scope: Optional[str] = Query(None),
    port_code: Optional[str] = Query(None),
    effective_date: date = Query(date.today()),
) -> List[Dict[str, Any]]:
    db: Session = SessionLocal()
    try:
        q = select(Fee)
        if scope:
            q = q.where(Fee.scope == scope)
        if port_code:
            q = q.where((Fee.applies_port_code == port_code) | (Fee.applies_port_code.is_(None)))
        q = q.where(Fee.effective_start <= effective_date)
        q = q.where((Fee.effective_end >= effective_date) | (Fee.effective_end.is_(None)))
        fees = db.execute(q.order_by(Fee.code, Fee.effective_start.desc())).scalars().all()
        return [
            {
                "id": f.id,
                "code": f.code,
                "name": f.name,
                "scope": f.scope,
                "unit": f.unit,
                "rate": str(f.rate),
                "currency": f.currency,
                "cap_amount": str(f.cap_amount) if f.cap_amount else None,
                "cap_period": f.cap_period,
                "applies_state": f.applies_state,
                "applies_port_code": f.applies_port_code,
                "applies_cascadia": f.applies_cascadia,
                "effective_start": str(f.effective_start),
                "effective_end": str(f.effective_end) if f.effective_end else None,
                "source_url": f.source_url,
                "authority": f.authority,
            }
            for f in fees
        ]
    except Exception:
        logger.exception("Failed to list fees")
        raise HTTPException(status_code=500, detail="fees query failed")
    finally:
        db.close()

# ----- Sources -----
@app.get("/sources", tags=["Sources"])
def list_sources() -> List[Dict[str, Any]]:
    db: Session = SessionLocal()
    try:
        sources = db.execute(select(Source).order_by(Source.type, Source.name)).scalars().all()
        return [
            {
                "id": s.id,
                "name": s.name,
                "url": s.url,
                "type": s.type,
                "effective_date": str(s.effective_date) if s.effective_date else None,
            }
            for s in sources
        ]
    except Exception:
        logger.exception("Failed to list sources")
        raise HTTPException(status_code=500, detail="sources query failed")
    finally:
        db.close()

# ----- Admin -----
@app.post("/admin/cache/clear", tags=["Admin"])
def clear_data_cache() -> Dict[str, str]:
    clear_cache()
    return {"message": "Cache cleared successfully"}

@app.get("/admin/cache/stats", tags=["Admin"])
def cache_statistics() -> Dict[str, Any]:
    return get_cache_stats()

# ----- System Stats & Feedback (optional, nice to have) -----
@app.get("/api/stats", tags=["System"])
def get_system_stats() -> Dict[str, Any]:
    db = SessionLocal()
    try:
        stats: Dict[str, Any] = {}

        # Basic DB liveness for callers that want one call:
        stats["db_ok"] = True
        try:
            _ = db.execute(text("SELECT 1")).scalar()
        except Exception:
            stats["db_ok"] = False

        # Port statistics (optional tables; guard with try)
        try:
            port_stats = db.execute(text("""
                SELECT 
                    COUNT(*) as total_ports,
                    COUNT(DISTINCT country) as countries,
                    COUNT(CASE WHEN country = 'US' THEN 1 END) as us_ports
                FROM ports
            """)).fetchone()
            stats["ports"] = {
                "total": int(port_stats[0]) if port_stats else 0,
                "countries": int(port_stats[1]) if port_stats else 0,
                "us_ports": int(port_stats[2]) if port_stats else 0,
            }
        except Exception:
            stats["ports"] = {"total": 0, "countries": 0, "us_ports": 0}

        # Fee statistics
        try:
            fee_stats = db.execute(text("""
                SELECT 
                    COUNT(DISTINCT code) as unique_fees,
                    COUNT(*) as total_fee_versions
                FROM fees
            """)).fetchone()
            stats["fees"] = {
                "unique_types": int(fee_stats[0]) if fee_stats else 0,
                "total_versions": int(fee_stats[1]) if fee_stats else 0,
            }
        except Exception:
            stats["fees"] = {"unique_types": 0, "total_versions": 0}

        return stats
    finally:
        db.close()

@app.post("/api/feedback", tags=["System"])
def submit_feedback(
    estimate_id: str = Body(...),
    actual_fees: Dict[str, Any] = Body(...),
    notes: Optional[str] = Body(None),
) -> Dict[str, Any]:
    """Stub feedback endpoint (store if table exists)."""
    db = SessionLocal()
    try:
        try:
            db.execute(text("""
                INSERT INTO estimate_feedback (
                    voyage_estimate_id, actual_mandatory_fees, actual_optional_fees, notes, created_at
                ) VALUES (:estimate_id, :mandatory, :optional, :notes, NOW())
            """), {
                "estimate_id": estimate_id,
                "mandatory": actual_fees.get("mandatory", 0),
                "optional": actual_fees.get("optional", 0),
                "notes": notes,
            })
            db.commit()
        except Exception:
            logger.warning("estimate_feedback table missing; feedback not stored.")
        return {"status": "ok"}
    finally:
        db.close()

# ----- Dev entrypoint -----
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, reload=True)
