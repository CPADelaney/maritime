# src/maritime_mvp/api/main.py
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException, Query
from sqlalchemy.orm import Session
from zeep.helpers import serialize_object

from ..db import SessionLocal
from ..rules.fee_engine import FeeEngine, EstimateContext
from ..clients.psix_client import PsixClient


app = FastAPI(title="Maritime MVP API", version="0.1.0")


@app.get("/health")
def health() -> Dict[str, bool]:
    return {"ok": True}


@app.get("/vessels/search")
def search_vessels(name: str) -> Any:
    """
    Proxy search against USCG PSIX (SOAP). We serialize zeep objects to plain dicts/lists.
    """
    client = PsixClient()
    try:
        raw = client.search_by_name(name)
        return serialize_object(raw, dict)
    except Exception as e:  # pragma: no cover - network/remote errors
        raise HTTPException(status_code=502, detail=f"PSIX search failed: {e!s}")


@app.get("/estimate")
def estimate(
    port_code: str,
    eta: date,
    arrival_type: str = Query("FOREIGN", pattern="^(FOREIGN|COASTWISE)$"),
    net_tonnage: Decimal | None = None,
    ytd_cbp_paid: Decimal = Decimal("0"),
) -> Dict[str, Any]:
    """
    Compute line-item estimates for a port call.
    """
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
        line_items: List[Dict[str, Any]] = [
            {
                "code": i.code,
                "name": i.name,
                "amount": str(i.amount),  # serialize Decimal
                "details": i.details,
            }
            for i in items
        ]

        return {
            "port_code": port_code,
            "eta": str(eta),
            "arrival_type": arrival_type,
            "line_items": line_items,
            "total": str(total),  # serialize Decimal
            "disclaimer": "Estimate only. Verify against official tariffs/guidance and your negotiated contracts.",
        }
    except Exception as e:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"estimate failed: {e!s}")
    finally:
        db.close()
