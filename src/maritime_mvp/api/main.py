# src/maritime_mvp/api/main.py (Updated - removed zeep imports)
from __future__ import annotations
import os
import logging
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
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
    psix_summary_by_name
)
from ..models import Port, Fee, Source

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("maritime-api")

# Create FastAPI app
app = FastAPI(
    title="Maritime MVP API", 
    version="0.2.1",
    description="Port call fee estimator with live data integration",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# ----- CORS Configuration -----
_allow = os.getenv("ALLOW_ORIGINS")
allow_origins: List[str] = [o.strip() for o in _allow.split(",")] if _allow else ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----- Static File Serving -----
def find_frontend_dir() -> Optional[Path]:
    """Find the frontend directory in various possible locations."""
    possible_paths = [
        Path("frontend"),
        Path("../frontend"),
        Path("../../frontend"),
        Path("../../../frontend"),
        Path(__file__).parent.parent.parent.parent / "frontend",
    ]
    
    for path in possible_paths:
        if path.exists() and path.is_dir():
            logger.info(f"Found frontend directory at: {path.absolute()}")
            return path.absolute()
    
    logger.warning("Frontend directory not found in any expected location")
    return None

# Mount frontend if available
frontend_dir = find_frontend_dir()
if frontend_dir:
    index_file = frontend_dir / "index.html"
    if not index_file.exists():
        logger.warning(f"index.html not found in {frontend_dir}, creating fallback")
        index_file.write_text(FALLBACK_FRONTEND_HTML)
    
    app.mount("/static", StaticFiles(directory=str(frontend_dir), html=True), name="static")
    logger.info(f"Frontend mounted at /static from {frontend_dir}")

# ----- Startup Event -----
@app.on_event("startup")
def _startup():
    """Initialize database on startup."""
    init_db()
    logger.info("Startup complete, DB initialized.")
    if frontend_dir:
        logger.info(f"Frontend available at /app")
    else:
        logger.warning("Frontend not mounted - directory not found")

# ----- Root & Frontend Routes -----
@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def root():
    """Serve the main application or redirect to it."""
    if frontend_dir and (frontend_dir / "index.html").exists():
        return FileResponse(frontend_dir / "index.html")
    else:
        return HTMLResponse(FALLBACK_LANDING_HTML)

@app.get("/app", response_class=HTMLResponse, include_in_schema=False)
async def app_root():
    """Serve the frontend application."""
    if frontend_dir and (frontend_dir / "index.html").exists():
        return FileResponse(frontend_dir / "index.html")
    else:
        return HTMLResponse(FALLBACK_FRONTEND_HTML)

@app.get("/app/{path:path}", include_in_schema=False)
async def serve_frontend(path: str):
    """Serve frontend files."""
    if frontend_dir:
        file_path = frontend_dir / path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(frontend_dir / "index.html")
    raise HTTPException(status_code=404, detail="Frontend not available")

# ----- API Routes -----
@app.get("/health", tags=["System"])
def health() -> Dict[str, Any]:
    """Health check endpoint."""
    return {
        "ok": True,
        "version": "0.2.1",
        "cache_stats": get_cache_stats(),
        "frontend_available": frontend_dir is not None
    }

@app.get("/api/docs", include_in_schema=False)
def api_docs():
    """Redirect to API documentation."""
    return RedirectResponse(url="/api/docs")

# ----- Ports -----
@app.get("/ports", tags=["Ports"])
def list_ports() -> List[Dict[str, Any]]:
    """List all available ports in the system."""
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
    """Get detailed information about a specific port."""
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
        logger.exception(f"Failed to get port {port_code}")
        raise HTTPException(status_code=500, detail="port query failed")
    finally:
        db.close()

# ----- Vessels -----
@app.get("/vessels/search", tags=["Vessels"])
def search_vessels(
    name: str = Query(..., description="Vessel name to search for"),
    use_cache: bool = Query(True, description="Use cached results if available")
) -> Any:
    """Search for vessels by name using PSIX."""
    if use_cache:
        try:
            result = psix_summary_by_name(name)
            if result and not result.get("error"):
                return {"Table": [result]} if result else {"Table": []}
        except Exception:
            logger.warning(f"Cache lookup failed for {name}, falling back to direct PSIX")
    
    client = PsixClient()
    try:
        result = client.search_by_name(name)
        # Result is already a dict with our new client
        return result
    except Exception as e:
        logger.exception("PSIX search failed")
        raise HTTPException(status_code=502, detail=f"PSIX search failed: {e!s}")

@app.get("/vessels/{vessel_id}", tags=["Vessels"])
def get_vessel_by_id(vessel_id: int) -> Any:
    """Get vessel information by PSIX vessel ID."""
    client = PsixClient()
    try:
        result = client.get_vessel_summary(vessel_id=vessel_id)
        return result
    except Exception as e:
        logger.exception(f"PSIX lookup failed for ID {vessel_id}")
        raise HTTPException(status_code=502, detail=f"PSIX lookup failed: {e!s}")

# ----- Fee Estimation -----
@app.get("/estimate", tags=["Estimates"])
def estimate(
    port_code: str = Query(..., description="Port code (e.g., LALB, SFBAY)"),
    eta: date = Query(..., description="Estimated time of arrival"),
    arrival_type: str = Query("FOREIGN", pattern="^(FOREIGN|COASTWISE)$", 
                             description="Type of arrival"),
    net_tonnage: Optional[Decimal] = Query(None, description="Net tonnage of vessel"),
    ytd_cbp_paid: Decimal = Query(Decimal("0"), 
                                  description="Year-to-date CBP fees already paid"),
    include_optional: bool = Query(False, 
                                  description="Include optional services in estimate"),
) -> Dict[str, Any]:
    """Calculate fee estimate for a port call."""
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
        
        optional_services = []
        if include_optional:
            optional_services = [
                {"service": "Pilotage", "estimated_low": 5000, "estimated_high": 15000,
                 "note": "Varies by vessel size and draft"},
                {"service": "Tugboat Assist", "estimated_low": 3000, "estimated_high": 8000,
                 "note": "Number of tugs depends on vessel size"},
                {"service": "Launch Service", "estimated_low": 500, "estimated_high": 1500,
                 "note": "For crew changes or supplies"},
                {"service": "Line Handling", "estimated_low": 1000, "estimated_high": 2500,
                 "note": "Mooring/unmooring services"},
            ]
        
        return {
            "port_code": port_code,
            "port_name": port.name,
            "eta": str(eta),
            "arrival_type": arrival_type,
            "line_items": [
                {
                    "code": i.code, 
                    "name": i.name, 
                    "amount": str(i.amount), 
                    "details": i.details
                }
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
async def live_port_bundle(
    vessel_name: Optional[str] = Query(None, description="Vessel name"),
    vessel_id: Optional[int] = Query(None, description="PSIX vessel ID"),
    port_code: Optional[str] = Query(None, description="Port code"),
    port_name: Optional[str] = Query(None, description="Port name (if code not provided)"),
    state: Optional[str] = Query(None, description="State (if port not provided)"),
    is_cascadia: Optional[bool] = Query(None, description="Is Cascadia region port"),
    imo_or_official_no: Optional[str] = Query(None, description="IMO or Official number"),
) -> Dict[str, Any]:
    """Get comprehensive live data bundle for a vessel and port."""
    try:
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

        bundle = build_live_bundle(
            vessel_name=vessel_name,
            vessel_id=vessel_id,
            port_code=port_code,
            port_name=port_name,
            state=state,
            is_cascadia=is_cascadia,
            imo_or_official_no=imo_or_official_no,
        )
        return bundle
    except Exception as e:
        logger.exception("live bundle failed")
        raise HTTPException(status_code=502, detail=f"live data aggregation failed: {e!s}")

@app.get("/live/pilotage/{port_code}", tags=["Live Data"])
def get_pilotage_info(port_code: str) -> Dict[str, Any]:
    """Get pilotage information for a specific port."""
    db: Session = SessionLocal()
    try:
        port = db.execute(select(Port).where(Port.code == port_code)).scalar_one_or_none()
        if not port:
            raise HTTPException(status_code=404, detail=f"Port {port_code} not found")
        
        from ..connectors.live_sources import choose_region, pilot_snapshot_for_region
        region = choose_region(port_code, port.name, port.state, port.is_cascadia)
        pilotage = pilot_snapshot_for_region(region)
        
        return {
            "port_code": port_code,
            "port_name": port.name,
            "region": region,
            "pilotage": pilotage
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Pilotage info failed for {port_code}")
        raise HTTPException(status_code=500, detail=f"pilotage lookup failed: {e!s}")
    finally:
        db.close()

# ----- Fees Management -----
@app.get("/fees", tags=["Fees"])
def list_fees(
    scope: Optional[str] = Query(None, description="Filter by scope (federal/state/port)"),
    port_code: Optional[str] = Query(None, description="Filter by port code"),
    effective_date: date = Query(date.today(), description="Show fees effective on this date"),
) -> List[Dict[str, Any]]:
    """List all fees or filter by criteria."""
    db: Session = SessionLocal()
    try:
        query = select(Fee)
        
        if scope:
            query = query.where(Fee.scope == scope)
        if port_code:
            query = query.where((Fee.applies_port_code == port_code) | 
                              (Fee.applies_port_code.is_(None)))
        
        query = query.where(Fee.effective_start <= effective_date)
        query = query.where((Fee.effective_end >= effective_date) | 
                          (Fee.effective_end.is_(None)))
        
        fees = db.execute(query.order_by(Fee.code, Fee.effective_start.desc())).scalars().all()
        
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

# ----- Data Sources -----
@app.get("/sources", tags=["Sources"])
def list_sources() -> List[Dict[str, Any]]:
    """List all data sources used by the system."""
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

# ----- Admin Endpoints -----
@app.post("/admin/cache/clear", tags=["Admin"])
def clear_data_cache() -> Dict[str, str]:
    """Clear the in-memory cache (admin only)."""
    clear_cache()
    return {"message": "Cache cleared successfully"}

@app.get("/admin/cache/stats", tags=["Admin"])
def cache_statistics() -> Dict[str, Any]:
    """Get cache statistics."""
    return get_cache_stats()

# ----- HTML Templates -----
FALLBACK_LANDING_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Maritime MVP</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gradient-to-br from-blue-50 to-indigo-100 min-h-screen">
    <div class="container mx-auto px-4 py-16 max-w-4xl">
        <div class="bg-white rounded-2xl shadow-xl p-8">
            <div class="text-center mb-8">
                <h1 class="text-4xl font-bold text-gray-800 mb-4">🚢 Maritime MVP</h1>
                <p class="text-lg text-gray-600">Port Call Fee Estimation & Vessel Intelligence Platform</p>
            </div>
            
            <div class="grid md:grid-cols-2 gap-6 mb-8">
                <a href="/app" class="block p-6 bg-blue-50 rounded-xl hover:bg-blue-100 transition">
                    <h2 class="text-xl font-semibold text-blue-900 mb-2">📊 Launch Dashboard</h2>
                    <p class="text-blue-700">Access the full Maritime MVP application</p>
                </a>
                
                <a href="/api/docs" class="block p-6 bg-green-50 rounded-xl hover:bg-green-100 transition">
                    <h2 class="text-xl font-semibold text-green-900 mb-2">📚 API Documentation</h2>
                    <p class="text-green-700">Explore REST API endpoints</p>
                </a>
            </div>
            
            <div class="border-t pt-6">
                <h3 class="font-semibold mb-3">Quick Links:</h3>
                <div class="flex flex-wrap gap-3">
                    <a href="/health" class="px-3 py-1 bg-gray-100 rounded hover:bg-gray-200">Health Check</a>
                    <a href="/ports" class="px-3 py-1 bg-gray-100 rounded hover:bg-gray-200">List Ports</a>
                    <a href="/vessels/search?name=EVER%20ACE" class="px-3 py-1 bg-gray-100 rounded hover:bg-gray-200">Sample Search</a>
                </div>
            </div>
        </div>
    </div>
</body>
</html>
"""

FALLBACK_FRONTEND_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Maritime MVP - Port Call Estimator</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script defer src="https://cdn.jsdelivr.net/npm/alpinejs@3.x.x/dist/cdn.min.js"></script>
</head>
<body class="bg-gray-50" x-data="maritimeApp()">
    <div class="container mx-auto px-4 py-8 max-w-6xl">
        <!-- Header -->
        <div class="bg-white rounded-lg shadow mb-6 p-6">
            <h1 class="text-2xl font-bold text-gray-800">🚢 Maritime Port Call Estimator</h1>
            <p class="text-gray-600 mt-2">Search vessels, select ports, and calculate fees</p>
        </div>

        <div class="grid md:grid-cols-3 gap-6">
            <!-- Vessel Search -->
            <div class="bg-white rounded-lg shadow p-6">
                <h2 class="text-lg font-semibold mb-4">Vessel Search</h2>
                <input x-model="vesselName" @keyup.enter="searchVessel" 
                       type="text" placeholder="Enter vessel name..."
                       class="w-full p-2 border rounded">
                <button @click="searchVessel" 
                        class="w-full mt-2 bg-blue-600 text-white py-2 rounded hover:bg-blue-700">
                    Search
                </button>
                
                <div x-show="searching" class="mt-4 text-center text-gray-600">
                    Searching...
                </div>
                
                <div x-show="vesselResults.length > 0" class="mt-4 max-h-40 overflow-y-auto">
                    <template x-for="vessel in vesselResults">
                        <button @click="selectVessel(vessel)"
                                class="w-full text-left p-2 border-b hover:bg-gray-50">
                            <span x-text="vessel.VesselName || vessel.vesselname"></span>
                        </button>
                    </template>
                </div>
            </div>

            <!-- Port & Parameters -->
            <div class="bg-white rounded-lg shadow p-6">
                <h2 class="text-lg font-semibold mb-4">Port & ETA</h2>
                
                <div class="mb-3">
                    <label class="block text-sm font-medium mb-1">Selected Vessel</label>
                    <input x-model="selectedVesselName" readonly 
                           class="w-full p-2 border rounded bg-gray-50">
                </div>
                
                <div class="mb-3">
                    <label class="block text-sm font-medium mb-1">Port</label>
                    <select x-model="selectedPort" class="w-full p-2 border rounded">
                        <option value="">Select port...</option>
                        <template x-for="port in ports">
                            <option :value="port.code" x-text="port.name + ' (' + port.state + ')'"></option>
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
                
                <button @click="calculateEstimate" 
                        :disabled="!selectedPort || !eta"
                        class="w-full bg-green-600 text-white py-2 rounded hover:bg-green-700 disabled:opacity-50">
                    Calculate Estimate
                </button>
            </div>

            <!-- Results -->
            <div class="bg-white rounded-lg shadow p-6">
                <h2 class="text-lg font-semibold mb-4">Estimate Results</h2>
                
                <div x-show="!estimate" class="text-gray-500">
                    No estimate calculated yet
                </div>
                
                <div x-show="estimate">
                    <div class="space-y-2">
                        <template x-for="item in estimate?.line_items || []">
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
                vesselName: '',
                vesselResults: [],
                selectedVessel: null,
                selectedVesselName: '',
                selectedPort: '',
                eta: new Date().toISOString().split('T')[0],
                arrivalType: 'FOREIGN',
                ports: [],
                estimate: null,
                searching: false,
                
                async init() {
                    // Load ports
                    try {
                        const response = await fetch(API_BASE + '/ports');
                        this.ports = await response.json();
                    } catch (error) {
                        console.error('Failed to load ports:', error);
                    }
                },
                
                async searchVessel() {
                    if (!this.vesselName.trim()) return;
                    
                    this.searching = true;
                    this.vesselResults = [];
                    
                    try {
                        const response = await fetch(API_BASE + '/vessels/search?name=' + encodeURIComponent(this.vesselName));
                        const data = await response.json();
                        this.vesselResults = data.Table || [];
                    } catch (error) {
                        console.error('Search failed:', error);
                        alert('Vessel search failed. Please try again.');
                    } finally {
                        this.searching = false;
                    }
                },
                
                selectVessel(vessel) {
                    this.selectedVessel = vessel;
                    this.selectedVesselName = vessel.VesselName || vessel.vesselname || '';
                    this.vesselResults = [];
                },
                
                async calculateEstimate() {
                    if (!this.selectedPort || !this.eta) {
                        alert('Please select a port and ETA');
                        return;
                    }
                    
                    const params = new URLSearchParams({
                        port_code: this.selectedPort,
                        eta: this.eta,
                        arrival_type: this.arrivalType
                    });
                    
                    try {
                        const response = await fetch(API_BASE + '/estimate?' + params);
                        this.estimate = await response.json();
                    } catch (error) {
                        console.error('Estimate failed:', error);
                        alert('Failed to calculate estimate. Please try again.');
                    }
                }
            }
        }
    </script>
</body>
</html>
"""
