# src/maritime_mvp/api/main.py
from __future__ import annotations
import os
import logging
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, JSONResponse
from sqlalchemy import select
from sqlalchemy.orm import Session
from zeep.helpers import serialize_object

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

app = FastAPI(
    title="Maritime MVP API", 
    version="0.2.0",
    description="Port call fee estimator with live data integration"
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

@app.on_event("startup")
def _startup():
    # Ensures tables exist; no-op if already created
    init_db()
    logger.info("Startup complete, DB initialized.")

# Root → docs
@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")

@app.get("/health", tags=["System"])
def health() -> Dict[str, Any]:
    """Health check endpoint."""
    return {
        "ok": True,
        "version": "0.2.0",
        "cache_stats": get_cache_stats()
    }

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
    """
    Search for vessels by name using PSIX.
    
    Returns vessel information from USCG PSIX database.
    """
    if use_cache:
        # Try the cached version first
        try:
            result = psix_summary_by_name(name)
            if result and not result.get("error"):
                return {"Table": [result]} if result else {"Table": []}
        except Exception:
            logger.warning(f"Cache lookup failed for {name}, falling back to direct PSIX")
    
    # Direct PSIX call
    client = PsixClient()
    try:
        raw = client.search_by_name(name)
        return serialize_object(raw, dict)
    except Exception as e:
        logger.exception("PSIX search failed")
        raise HTTPException(status_code=502, detail=f"PSIX search failed: {e!s}")

@app.get("/vessels/{vessel_id}", tags=["Vessels"])
def get_vessel_by_id(vessel_id: int) -> Any:
    """Get vessel information by PSIX vessel ID."""
    client = PsixClient()
    try:
        raw = client.get_vessel_summary(vessel_id=vessel_id)
        return serialize_object(raw, dict)
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
    """
    Calculate fee estimate for a port call.
    
    Returns itemized fees and total estimate based on current regulations.
    """
    db: Session = SessionLocal()
    try:
        # Check port exists
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
        
        # Add optional services if requested
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
            "disclaimer": "Estimate only. Verify against official tariffs/guidance and your negotiated contracts. Optional services are rough estimates and vary significantly.",
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
    """
    Get comprehensive live data bundle for a vessel and port.
    
    This endpoint aggregates data from multiple sources:
    - PSIX vessel information
    - Document status and expiries
    - Pilotage information
    - Marine Exchange data
    - MISP (California) fee information
    - COFR requirements
    - Alerts for missing/expiring documents
    """
    try:
        # If they pass a port_code, enrich with DB facts
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
        
        # Only show fees effective on the given date
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
    # TODO: Add authentication check here
    clear_cache()
    return {"message": "Cache cleared successfully"}

@app.get("/admin/cache/stats", tags=["Admin"])
def cache_statistics() -> Dict[str, Any]:
    """Get cache statistics."""
    return get_cache_stats()

# ----- Bulk Estimate (for fleet operators) -----

@app.post("/estimate/bulk", tags=["Estimates"])
def bulk_estimate(calls: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calculate estimates for multiple port calls.
    
    Useful for fleet operators planning multiple vessel movements.
    """
    if len(calls) > 20:
        raise HTTPException(status_code=400, detail="Maximum 20 calls per request")
    
    db: Session = SessionLocal()
    try:
        engine = FeeEngine(db)
        results = []
        total_all = Decimal("0.00")
        
        for call in calls:
            try:
                ctx = EstimateContext(
                    port_code=call["port_code"],
                    arrival_date=date.fromisoformat(call["eta"]),
                    arrival_type=call.get("arrival_type", "FOREIGN"),
                    net_tonnage=Decimal(str(call["net_tonnage"])) if call.get("net_tonnage") else None,
                    ytd_cbp_paid=Decimal(str(call.get("ytd_cbp_paid", "0"))),
                )
                items = engine.compute(ctx)
                total = sum((i.amount for i in items), Decimal("0.00"))
                
                results.append({
                    "vessel": call.get("vessel_name", "Unknown"),
                    "port_code": call["port_code"],
                    "eta": call["eta"],
                    "total": str(total),
                    "status": "success"
                })
                total_all += total
            except Exception as e:
                results.append({
                    "vessel": call.get("vessel_name", "Unknown"),
                    "port_code": call.get("port_code", "Unknown"),
                    "eta": call.get("eta", "Unknown"),
                    "error": str(e),
                    "status": "failed"
                })
        
        return {
            "results": results,
            "total_all_calls": str(total_all),
            "successful": sum(1 for r in results if r["status"] == "success"),
            "failed": sum(1 for r in results if r["status"] == "failed"),
        }
    except Exception:
        logger.exception("Bulk estimate failed")
        raise HTTPException(status_code=500, detail="bulk estimate failed")
    finally:
        db.close()

# ----- Export Estimates -----

@app.get("/estimate/export", tags=["Estimates"])
def export_estimate(
    port_code: str,
    eta: date,
    arrival_type: str = Query("FOREIGN"),
    net_tonnage: Optional[Decimal] = None,
    ytd_cbp_paid: Decimal = Decimal("0"),
    format: str = Query("json", pattern="^(json|csv)$"),
) -> Any:
    """Export estimate in different formats."""
    # First get the estimate
    estimate_data = estimate(
        port_code=port_code,
        eta=eta,
        arrival_type=arrival_type,
        net_tonnage=net_tonnage,
        ytd_cbp_paid=ytd_cbp_paid,
        include_optional=True
    )
    
    if format == "csv":
        import csv
        import io
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Header
        writer.writerow(["Port Call Fee Estimate"])
        writer.writerow([])
        writer.writerow(["Port", estimate_data["port_name"]])
        writer.writerow(["ETA", estimate_data["eta"]])
        writer.writerow(["Arrival Type", estimate_data["arrival_type"]])
        writer.writerow([])
        
        # Line items
        writer.writerow(["Code", "Description", "Amount"])
        for item in estimate_data["line_items"]:
            writer.writerow([item["code"], item["name"], item["amount"]])
        writer.writerow([])
        writer.writerow(["Total", "", estimate_data["total"]])
        
        # Optional services
        if estimate_data.get("optional_services"):
            writer.writerow([])
            writer.writerow(["Optional Services", "Low Estimate", "High Estimate"])
            for svc in estimate_data["optional_services"]:
                writer.writerow([svc["service"], svc["estimated_low"], svc["estimated_high"]])
        
        content = output.getvalue()
        from fastapi.responses import Response
        return Response(
            content=content,
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=estimate_{port_code}_{eta}.csv"
            }
        )
    
    return estimate_data
