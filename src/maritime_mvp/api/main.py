# src/maritime_mvp/api/main.py
from __future__ import annotations
import os
import logging
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..db import SessionLocal, init_db
from ..rules.fee_engine import FeeEngine, EstimateContext
from ..clients.psix_client import PsixClient
from ..connectors.live_sources import (
    build_live_bundle,
    clear_cache,
    get_cache_stats,
)
from ..models import Port, Fee, Source

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("maritime-api")

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
        <select x-model="selectedPort" class="w-full p-2 border rounded">
          <option value="">Select port...</option>
          <template x-for="p in ports" :key="p.code">
            <option :value="p.code" x-text="p.name + (p.state ? ' ('+p.state+')' : '')"></option>
          </template>
        </select>
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
        this.totalCount = data._count ?? this.vesselResults.length;
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
    title="Maritime MVP API",
    version="0.2.2",
    description="Port call fee estimator with live data integration",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)

# ----- CORS -----
_allow = os.getenv("ALLOW_ORIGINS")
allow_origins: List[str] = [o.strip() for o in _allow.split(",")] if _allow else ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

# ----- Startup -----
@app.on_event("startup")
def _startup():
    """Initialize database on startup."""
    try:
        init_db()  # ← removed safe=True to match current db.init_db signature
        logger.info("Startup complete, DB initialized.")
    except Exception:
        # Preserve the previous 'safe=True' behavior: log and keep booting.
        logger.exception("DB init failed during startup; continuing without blocking app.")
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
    return {
        "ok": True,
        "version": "0.2.2",
        "cache_stats": get_cache_stats(),
        "frontend_available": frontend_dir is not None,
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
@app.get("/vessels/search", tags=["Vessels"])
def search_vessels(
    name: str = Query(..., description="Vessel name to search for"),
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    limit: int = Query(25, ge=1, le=100, description="Rows per page (default 25)")
) -> Any:
    """
    Search PSIX by name and return a paginated, trimmed list.

    Response shape:
    {
      "Table": [...],
      "total": <int>,         # total matches
      "count": <int>,         # number returned (<= limit)
      "page": <int>,
      "limit": <int>,
      "pages": <int>,         # total pages
      "has_prev": <bool>,
      "has_next": <bool>,
      "start": <int>,         # 1-based start index in overall results
      "end": <int>            # 1-based end index in overall results
    }
    """
    client = PsixClient()
    try:
        raw = client.search_by_name(name)  # {"Table": [...]}
        rows = (raw or {}).get("Table") or []

        # Optional: stable sort by VesselName then CallSign
        def _nm(r): return (r.get("VesselName") or r.get("vesselname") or "").upper()
        def _cs(r): return (r.get("CallSign") or r.get("callsign") or "").upper()
        rows.sort(key=lambda r: (_nm(r), _cs(r)))

        total = len(rows)
        pages = max((total + limit - 1) // limit, 1)
        page = min(max(page, 1), pages)
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit

        page_rows = rows[start_idx:end_idx]

        # Trim fields to what the UI needs
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

        return {
            "Table": trimmed,
            "total": total,
            "count": len(trimmed),
            "page": page,
            "limit": limit,
            "pages": pages,
            "has_prev": page > 1,
            "has_next": page < pages,
            "start": start_human,
            "end": end_human,
        }
    except Exception as e:
        logger.exception("PSIX search failed")
        raise HTTPException(status_code=502, detail=f"PSIX search failed: {e!s}")

@app.get("/vessels/{vessel_id}", tags=["Vessels"])
def get_vessel_by_id(vessel_id: int) -> Dict[str, Any]:
    client = PsixClient()
    try:
        return client.get_vessel_summary(vessel_id=vessel_id)
    except Exception as e:
        logger.exception("PSIX lookup failed for ID %s", vessel_id)
        raise HTTPException(status_code=502, detail=f"PSIX lookup failed: {e!s}")

# ----- Fee Estimation -----
@app.get("/estimate", tags=["Estimates"])
def estimate(
    port_code: str = Query(..., description="Port code (e.g., LALB, SFBAY)"),
    eta: date = Query(..., description="Estimated time of arrival"),
    arrival_type: str = Query("FOREIGN", pattern="^(FOREIGN|COASTWISE)$"),
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
            arrival_type=arrival_type,
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
            "arrival_type": arrival_type,
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
