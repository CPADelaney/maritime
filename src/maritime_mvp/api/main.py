from __future__ import annotations
import os
import logging
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from sqlalchemy import select
from sqlalchemy.orm import Session
from zeep.helpers import serialize_object

from ..db import SessionLocal, init_db
from ..rules.fee_engine import FeeEngine, EstimateContext
from ..clients.psix_client import PsixClient
from ..models import Port

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("maritime-api")

app = FastAPI(title="Maritime MVP API", version="0.1.3")

from sqlalchemy.engine.url import make_url
from ..settings import settings
try:
    u = make_url(settings.sqlalchemy_url)
    logger.info("DB target → user=%s host=%s port=%s db=%s", u.username, u.host, u.port, u.database)
except Exception:
    logger.exception("Could not parse DB URL")

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

@app.get("/health")
def health() -> Dict[str, bool]:
    return {"ok": True}

@app.get("/ports")
def list_ports() -> List[Dict[str, Optional[str] | bool]]:
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
    except Exception:
        logger.exception("Failed to list ports")
        raise HTTPException(status_code=500, detail="ports query failed")
    finally:
        db.close()

@app.get("/vessels/search")
def search_vessels(name: str) -> Any:
    client = PsixClient()
    try:
        raw = client.search_by_name(name)
        return serialize_object(raw, dict)
    except Exception as e:
        logger.exception("PSIX search failed")
        raise HTTPException(status_code=502, detail=f"PSIX search failed: {e!s}")

@app.get("/estimate")
def estimate(
    port_code: str,
    eta: date,
    arrival_type: str = Query("FOREIGN", pattern="^(FOREIGN|COASTWISE)$"),
    net_tonnage: Decimal | None = None,
    ytd_cbp_paid: Decimal = Decimal("0"),
) -> Dict[str, Any]:
    db: Session = SessionLocal()
    try:
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
            "eta": str(eta),
            "arrival_type": arrival_type,
            "line_items": [
                {"code": i.code, "name": i.name, "amount": str(i.amount), "details": i.details}
                for i in items
            ],
            "total": str(total),
            "disclaimer": "Estimate only. Verify against official tariffs/guidance and your negotiated contracts.",
        }
    except Exception:
        logger.exception("Estimate failed")
        raise HTTPException(status_code=500, detail="estimate failed")
    finally:
        db.close()
