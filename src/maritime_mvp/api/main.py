from fastapi import FastAPI, Query
from datetime import date
from decimal import Decimal
from sqlalchemy.orm import Session
from ..db import SessionLocal
from ..rules.fee_engine import FeeEngine, EstimateContext
from ..clients.psix_client import PsixClient

app = FastAPI(title="Maritime MVP API", version="0.1.0")

@app.get("/vessels/search")
def search_vessels(name: str):
    client = PsixClient()
    return client.search_by_name(name)

@app.get("/estimate")
def estimate(
    port_code: str,
    eta: date,
    arrival_type: str = Query("FOREIGN", pattern="^(FOREIGN|COASTWISE)$"),
    net_tonnage: Decimal | None = None,
    ytd_cbp_paid: Decimal = Decimal("0"),
):
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
        total = sum([i.amount for i in items], Decimal("0.00"))
        return {
            "port_code": port_code,
            "eta": str(eta),
            "arrival_type": arrival_type,
            "line_items": [i.__dict__ for i in items],
            "total": str(total),
            "disclaimer": "Estimate only. Verify against official tariffs/guidance and your negotiated contracts.",
        }
    finally:
        db.close()
