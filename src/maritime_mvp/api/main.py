from __future__ import annotations
import os
import logging
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from sqlalchemy import select, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError
from zeep.helpers import serialize_object

from ..db import SessionLocal, init_db, test_connection
from ..rules.fee_engine import FeeEngine, EstimateContext
from ..clients.psix_client import PsixClient
from ..models import Port

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("maritime-api")

app = FastAPI(title="Maritime MVP API", version="0.1.4")

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
    """Initialize database on startup"""
    try:
        # Ensures tables exist; no-op if already created
        init_db()
        logger.info("Startup complete, DB initialized.")
    except Exception as e:
        logger.error(f"Startup error: {e}")
        logger.warning("API starting despite initialization issues. Some endpoints may not work.")

# Root → docs
@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")

@app.get("/health")
def health() -> Dict[str, Any]:
    """Health check endpoint with database status"""
    health_status = {
        "ok": True,
        "version": "0.1.4",
        "status": "healthy"
    }
    
    # Test database connection
    try:
        db_ok = test_connection()
        health_status["database"] = "connected" if db_ok else "disconnected"
        
        if not db_ok:
            health_status["status"] = "degraded"
            health_status["message"] = "Database connection failed, some features may be unavailable"
    except Exception as e:
        health_status["database"] = "error"
        health_status["status"] = "degraded"
        health_status["message"] = str(e)
    
    return health_status

@app.get("/ports")
def list_ports() -> List[Dict[str, Optional[str] | bool]]:
    """List all available ports"""
    db: Session = SessionLocal()
    try:
        rows = db.execute(select(Port).order_by(Port.name)).scalars().all()
        return [
            {
                "code": p.code,
                "name": p.name,
                "state": p.state,
                "region": p.region,
                "is_california": p.is_california,
                "is_cascadia": p.is_cascadia,
            }
            for p in rows
        ]
    except OperationalError as e:
        logger.error(f"Database error in list_ports: {e}")
        raise HTTPException(
            status_code=503, 
            detail="Database temporarily unavailable. Please try again later."
        )
    except Exception as e:
        logger.exception("Failed to list ports")
        raise HTTPException(status_code=500, detail="Failed to retrieve ports")
    finally:
        db.close()

@app.get("/vessels/search")
def search_vessels(name: str) -> Any:
    """Search for vessels by name using PSIX SOAP service"""
    if not name or len(name) < 2:
        raise HTTPException(status_code=400, detail="Please provide at least 2 characters for vessel name")
    
    client = PsixClient()
    try:
        raw = client.search_by_name(name)
        return serialize_object(raw, dict)
    except Exception as e:
        logger.exception("PSIX search failed")
        raise HTTPException(
            status_code=502, 
            detail=f"Vessel search service temporarily unavailable: {str(e)}"
        )

@app.get("/estimate")
def estimate(
    port_code: str,
    eta: date,
    arrival_type: str = Query("FOREIGN", pattern="^(FOREIGN|COASTWISE)$"),
    net_tonnage: Decimal | None = None,
    ytd_cbp_paid: Decimal = Decimal("0"),
) -> Dict[str, Any]:
    """Calculate port call fee estimate"""
    
    # Validate inputs
    if not port_code:
        raise HTTPException(status_code=400, detail="Port code is required")
    
    if net_tonnage is not None and net_tonnage < 0:
        raise HTTPException(status_code=400, detail="Net tonnage cannot be negative")
    
    if ytd_cbp_paid < 0:
        raise HTTPException(status_code=400, detail="YTD CBP paid cannot be negative")
    
    db: Session = SessionLocal()
    try:
        # Check if port exists
        port = db.execute(select(Port).where(Port.code == port_code)).scalar_one_or_none()
        if not port:
            raise HTTPException(status_code=404, detail=f"Port code '{port_code}' not found")
        
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
            "total": str(total),
            "currency": "USD",
            "disclaimer": "Estimate only. Verify against official tariffs/guidance and your negotiated contracts.",
        }
    except HTTPException:
        raise  # Re-raise HTTP exceptions as-is
    except OperationalError as e:
        logger.error(f"Database error in estimate: {e}")
        raise HTTPException(
            status_code=503, 
            detail="Database temporarily unavailable. Please try again later."
        )
    except Exception as e:
        logger.exception("Estimate calculation failed")
        raise HTTPException(status_code=500, detail="Failed to calculate estimate")
    finally:
        db.close()

@app.get("/api/status")
def api_status() -> Dict[str, Any]:
    """Detailed API status endpoint for monitoring"""
    db: Session = SessionLocal()
    status = {
        "api": "operational",
        "database": "unknown",
        "psix": "unknown",
        "ports_count": 0,
        "fees_count": 0,
    }
    
    # Check database
    try:
        # Test with a simple query
        result = db.execute(text("SELECT 1")).scalar()
        if result == 1:
            status["database"] = "operational"
            
            # Get counts
            from ..models import Fee
            port_count = db.execute(select(Port)).scalars().all()
            fee_count = db.execute(select(Fee)).scalars().all()
            status["ports_count"] = len(port_count)
            status["fees_count"] = len(fee_count)
    except Exception as e:
        status["database"] = f"error: {str(e)[:100]}"
    finally:
        db.close()
    
    # Check PSIX (quick test)
    try:
        client = PsixClient()
        # Try a minimal search
        client.search_by_name("TEST")
        status["psix"] = "operational"
    except Exception as e:
        status["psix"] = f"error: {str(e)[:100]}"
    
    return status

# Add a simple test endpoint
@app.get("/test")
def test() -> Dict[str, str]:
    """Simple test endpoint that doesn't require database"""
    return {
        "message": "Maritime API is running",
        "timestamp": str(date.today())
    }
