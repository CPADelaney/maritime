# src/maritime_mvp/api/routes.py
"""Enhanced API routes with comprehensive fee calculation and document management.

Notes:
- All SQL uses bound parameters (no f-strings) to avoid injection.
- Multi-port endpoint uses embedded body models; JSON must include {"request": {...}, "vessel": {...}}.
- Historical trends endpoint is clearly labeled as mock data.
"""

from __future__ import annotations

import logging
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import List, Optional, Dict, Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Body, Depends
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db import SessionLocal
from ..rules.enhanced_fee_engine import (
    EnhancedFeeEngine,
    VesselSpecs,
    VoyageContext,
    VesselType,
)

logger = logging.getLogger("maritime-api")

router = APIRouter(prefix="/api/v2", tags=["Enhanced Maritime API"])

# ============ Pydantic Models ============


class PortInfo(BaseModel):
    locode: str = Field(..., example="USOAK")
    port_name: str = Field(..., example="Oakland, CA")
    country_code: str = Field(..., example="US")
    region: Optional[str] = None


class VesselInput(BaseModel):
    name: str = Field(..., example="EVER GIVEN")
    imo_number: Optional[str] = Field(None, example="9811000")
    vessel_type: str = Field("general_cargo", example="container")
    gross_tonnage: Decimal = Field(..., example=220940)
    net_tonnage: Decimal = Field(..., example=109999)
    loa_meters: Decimal = Field(..., example=400)
    beam_meters: Decimal = Field(..., example=59)
    draft_meters: Decimal = Field(..., example=16)


class VoyageInput(BaseModel):
    previous_port_code: str = Field(..., example="CNSHA")
    arrival_port_code: str = Field(..., example="USOAK")
    next_port_code: Optional[str] = Field(None, example="USSEA")
    eta: datetime = Field(..., example="2025-09-15T08:00:00")
    etd: Optional[datetime] = Field(None, example="2025-09-17T18:00:00")
    days_alongside: int = Field(2, example=2)


class ComprehensiveEstimateRequest(BaseModel):
    vessel: VesselInput
    voyage: VoyageInput
    ytd_cbp_paid: Decimal = Field(0, example=0)
    tonnage_year_paid: Decimal = Field(0, example=0)
    include_optional_services: bool = Field(True)


class DocumentRequirement(BaseModel):
    document_name: str
    document_code: str
    is_mandatory: bool
    lead_time_hours: int
    authority: str
    description: Optional[str] = None
    expiry_warning: Optional[str] = None


class PortSequenceRequest(BaseModel):
    vessel_name: str
    ports: List[str] = Field(..., example=["CNSHA", "USOAK", "USSEA", "USLAX"])
    start_date: date = Field(..., example="2025-09-01")
    days_between_ports: int = Field(14, example=14)
    days_in_port: int = Field(2, example=2)


# ============ Database Dependency ============


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ============ Helpers ============


def _parse_vessel_type(s: Optional[str]) -> VesselType:
    """Robust vessel type parsing with safe fallback."""
    try:
        return VesselType((s or "general_cargo").lower())
    except Exception:
        return VesselType.GENERAL_CARGO


def _dec(val: Any, default: str = "0") -> Decimal:
    """Best-effort Decimal conversion."""
    try:
        return Decimal(str(val))
    except Exception:
        return Decimal(default)


def _arrival_type(prev_port_code: Optional[str]) -> str:
    """Derive arrival type (FOREIGN vs COASTWISE) from previous port code."""
    if prev_port_code and prev_port_code.strip().upper().startswith("US"):
        return "COASTWISE"
    return "FOREIGN"


# ============ IMO Port Lookup ============


@router.get("/ports/search", response_model=List[PortInfo])
async def search_imo_ports(
    q: str = Query(..., min_length=2, description="Search by name or code"),
    country: Optional[str] = Query(None, description="Filter by country code (e.g., US)"),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Search IMO/UNLOCODE ports."""
    sql = text(
        """
        SELECT locode, port_name, country_code, country_name
        FROM imo_ports
        WHERE (port_name ILIKE :q OR locode ILIKE :q OR port_code ILIKE :q)
          AND (:country IS NULL OR country_code = :country)
        ORDER BY CASE WHEN locode = UPPER(:rawq) THEN 0 ELSE 1 END, port_name
        LIMIT :limit
        """
    )
    rows = db.execute(
        sql, {"q": f"%{q}%", "rawq": q, "country": country, "limit": limit}
    ).fetchall()

    return [
        PortInfo(locode=r[0], port_name=r[1], country_code=r[2], region=r[3]) for r in rows
    ]


@router.get("/ports/{locode}", response_model=PortInfo)
async def get_port_details(locode: str, db: Session = Depends(get_db)):
    """Get detailed information for a specific UN/LOCODE."""
    sql = text(
        """
        SELECT locode, port_name, country_code, country_name
        FROM imo_ports
        WHERE locode = :loc
        """
    )
    row = db.execute(sql, {"loc": locode.upper()}).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Port {locode} not found")

    return PortInfo(locode=row[0], port_name=row[1], country_code=row[2], region=row[3])


# ============ Comprehensive Fee Estimation ============


@router.post("/estimate/comprehensive")
async def calculate_comprehensive_estimate(
    request: ComprehensiveEstimateRequest, db: Session = Depends(get_db)
):
    """Calculate comprehensive port call fees for a vessel + voyage."""
    # Convert to internal types
    vessel_type = _parse_vessel_type(request.vessel.vessel_type)

    vessel = VesselSpecs(
        name=request.vessel.name,
        imo_number=request.vessel.imo_number,
        vessel_type=vessel_type,
        gross_tonnage=request.vessel.gross_tonnage,
        net_tonnage=request.vessel.net_tonnage,
        loa_meters=request.vessel.loa_meters,
        beam_meters=request.vessel.beam_meters,
        draft_meters=request.vessel.draft_meters,
    )

    voyage = VoyageContext(
        previous_port_code=request.voyage.previous_port_code,
        arrival_port_code=request.voyage.arrival_port_code,
        next_port_code=request.voyage.next_port_code,
        eta=request.voyage.eta,
        etd=request.voyage.etd,
        days_alongside=max(1, int(request.voyage.days_alongside or 1)),
    )

    engine = EnhancedFeeEngine(db)
    engine.ytd_cbp_paid = _dec(request.ytd_cbp_paid)
    engine.tonnage_year_paid = _dec(request.tonnage_year_paid)

    result = engine.calculate_comprehensive(vessel, voyage)

    # Persist summary (best-effort; do not fail API if table is missing)
    voyage_id = str(uuid4())
    try:
        db.execute(
            text(
                """
                INSERT INTO voyage_estimates (
                    id, vessel_name, vessel_type, imo_number,
                    previous_port_code, arrival_port_code, next_port_code,
                    gross_tonnage, net_tonnage, loa, beam, draft,
                    eta, etd, days_alongside,
                    total_mandatory_fees, total_optional_fees, confidence_score,
                    created_at
                ) VALUES (
                    :id, :vname, :vtype, :imo,
                    :prev, :arr, :next,
                    :gt, :nt, :loa, :beam, :draft,
                    :eta, :etd, :days,
                    :mand, :opt, :conf,
                    NOW()
                )
                """
            ),
            {
                "id": voyage_id,
                "vname": vessel.name,
                "vtype": vessel.vessel_type.value,
                "imo": vessel.imo_number,
                "prev": voyage.previous_port_code,
                "arr": voyage.arrival_port_code,
                "next": voyage.next_port_code,
                "gt": vessel.gross_tonnage,
                "nt": vessel.net_tonnage,
                "loa": vessel.loa_meters,
                "beam": vessel.beam_meters,
                "draft": vessel.draft_meters,
                "eta": voyage.eta,
                "etd": voyage.etd,
                "days": voyage.days_alongside,
                "mand": _dec(result.get("totals", {}).get("mandatory", "0")),
                "opt": _dec(result.get("totals", {}).get("optional_high", "0")),
                "conf": _dec(result.get("confidence", "0.9"), "0.9"),
            },
        )
        db.commit()
    except Exception:
        logger.warning(
            "voyage_estimates insert skipped (table missing or other error)", exc_info=True
        )

    # Attach an ID for caller-side reconciliation
    result["estimate_id"] = voyage_id
    result.setdefault("meta", {})["arrival_type"] = _arrival_type(
        request.voyage.previous_port_code
    )
    return result


# ============ Document Requirements ============


def _document_requirements_core(
    db: Session,
    port_code: str,
    vessel_type: Optional[str],
    previous_port: Optional[str],
) -> List[DocumentRequirement]:
    docs: List[DocumentRequirement] = []

    # Common US docs
    common_sql = text(
        """
        SELECT document_name, document_code, is_mandatory, 
               lead_time_hours, authority, description
        FROM port_documents
        WHERE port_code = 'ALL_US'
        """
    )
    for row in db.execute(common_sql).fetchall():
        docs.append(
            DocumentRequirement(
                document_name=row[0],
                document_code=row[1],
                is_mandatory=bool(row[2]),
                lead_time_hours=int(row[3] or 0),
                authority=row[4],
                description=row[5],
            )
        )

    # Port-specific
    port_sql = text(
        """
        SELECT document_name, document_code, is_mandatory,
               lead_time_hours, authority, description
        FROM port_documents
        WHERE port_code = :pc
        """
    )
    for row in db.execute(port_sql, {"pc": port_code}).fetchall():
        docs.append(
            DocumentRequirement(
                document_name=row[0],
                document_code=row[1],
                is_mandatory=bool(row[2]),
                lead_time_hours=int(row[3] or 0),
                authority=row[4],
                description=row[5],
            )
        )

    # Conditional docs
    if previous_port and not previous_port.strip().upper().startswith("US"):
        docs.append(
            DocumentRequirement(
                document_name="Customs Declaration for Foreign Arrival",
                document_code="CBP-1300",
                is_mandatory=True,
                lead_time_hours=24,
                authority="CBP",
                description="Required for all foreign arrivals",
            )
        )

    # Placeholder: PSIX/COFR expiry check (flag as warning)
    for d in docs:
        if d.document_code.upper() == "COFR":
            d.expiry_warning = "Verify COFR validity / expiry (PSIX/NPFC lookup)."

    # Could filter or mark based on vessel_type if needed in the future
    _ = vessel_type  # reserved for future logic
    return docs


@router.get("/documents/requirements", response_model=List[DocumentRequirement])
async def get_document_requirements(
    port_code: str = Query(..., description="UN/LOCODE (e.g., USOAK)"),
    vessel_type: Optional[str] = Query(None, description="Vessel type (optional)"),
    previous_port: Optional[str] = Query(None, description="Previous port code"),
    db: Session = Depends(get_db),
):
    """Get required documents for a port call."""
    return _document_requirements_core(db, port_code, vessel_type, previous_port)


# ============ Multi-Port Voyage Planning ============


@router.post("/voyage/multi-port")
async def calculate_multi_port_voyage(
    request: PortSequenceRequest = Body(..., embed=True),
    vessel: VesselInput = Body(..., embed=True),
    db: Session = Depends(get_db),
):
    """
    Calculate fees for a multi-port voyage.

    Body shape (because of embed=True):
    {
      "request": { ...PortSequenceRequest... },
      "vessel":  { ...VesselInput... }
    }
    """
    if len(request.ports) < 2:
        raise HTTPException(status_code=400, detail="At least two ports are required")

    voyage_legs: List[Dict[str, Any]] = []
    current_date = request.start_date

    vtype = _parse_vessel_type(vessel.vessel_type)

    for i in range(len(request.ports) - 1):
        prev_port = request.ports[i].strip().upper()
        arrival_port = request.ports[i + 1].strip().upper()
        next_port = request.ports[i + 2].strip().upper() if (i + 2) < len(request.ports) else None

        eta = datetime.combine(current_date, datetime.min.time())
        etd = datetime.combine(
            current_date + timedelta(days=request.days_in_port), datetime.min.time()
        )

        voyage = VoyageContext(
            previous_port_code=prev_port,
            arrival_port_code=arrival_port,
            next_port_code=next_port,
            eta=eta,
            etd=etd,
            days_alongside=max(1, int(request.days_in_port or 1)),
        )

        vessel_specs = VesselSpecs(
            name=vessel.name,
            imo_number=vessel.imo_number,
            vessel_type=vtype,
            gross_tonnage=vessel.gross_tonnage,
            net_tonnage=vessel.net_tonnage,
            loa_meters=vessel.loa_meters,
            beam_meters=vessel.beam_meters,
            draft_meters=vessel.draft_meters,
        )

        engine = EnhancedFeeEngine(db)
        leg_estimate = engine.calculate_comprehensive(vessel_specs, voyage)

        # Compute arrival type and weekend flag
        arr_type = _arrival_type(prev_port)
        weekend_arrival = eta.weekday() >= 5  # Sat/Sun

        # Document requirements (count only)
        docs = _document_requirements_core(db, arrival_port, vessel.vessel_type, prev_port)

        fees_totals = leg_estimate.get("totals", {})
        voyage_legs.append(
            {
                "leg": i + 1,
                "from_port": prev_port,
                "to_port": arrival_port,
                "eta": eta.isoformat(),
                "etd": etd.isoformat(),
                "fees": {
                    "mandatory": str(_dec(fees_totals.get("mandatory", "0"))),
                    "optional_low": str(_dec(fees_totals.get("optional_low", "0"))),
                    "optional_high": str(_dec(fees_totals.get("optional_high", "0"))),
                },
                "arrival_type": arr_type,
                "weekend_arrival": weekend_arrival,
                "documents_required": len(docs),
            }
        )

        # Advance to next leg’s start (in-port days + between-port days)
        current_date += timedelta(days=request.days_in_port + request.days_between_ports)

    # Totals
    total_mandatory = sum(_dec(leg["fees"]["mandatory"]) for leg in voyage_legs)
    total_optional_high = sum(_dec(leg["fees"]["optional_high"]) for leg in voyage_legs)

    return {
        "vessel_name": request.vessel_name,
        "voyage_summary": {
            "total_ports": len(request.ports),
            "total_legs": len(voyage_legs),
            "total_days": (current_date - request.start_date).days,
            "start_date": request.start_date.isoformat(),
            "end_date": current_date.isoformat(),
        },
        "legs": voyage_legs,
        "total_voyage_cost": {
            "mandatory": str(total_mandatory),
            "with_optional": str(total_mandatory + total_optional_high),
            "currency": "USD",
        },
        "optimization_suggestions": _get_voyage_optimizations(voyage_legs),
    }


def _get_voyage_optimizations(legs: List[Dict[str, Any]]) -> List[str]:
    """Suggest voyage optimizations based on the legs computed."""
    suggestions: List[str] = []

    # Weekend arrivals
    weekend_count = sum(1 for leg in legs if leg.get("weekend_arrival"))
    if weekend_count:
        suggestions.append(
            f"Avoid {weekend_count} weekend arrivals to reduce pilotage/port overtime charges."
        )

    # High-fee ports (simple threshold on mandatory)
    high_fee_ports = [leg for leg in legs if _dec(leg["fees"]["mandatory"]) > Decimal("15000")]
    if high_fee_ports:
        suggestions.append(
            f"Consider alternatives or scheduling changes for {len(high_fee_ports)} high-fee legs."
        )

    # Foreign/coastwise mix
    foreign_count = sum(1 for leg in legs if leg.get("arrival_type") == "FOREIGN")
    if foreign_count > 1:
        suggestions.append("Consider inserting a qualifying U.S. stop to utilize coastwise rates.")

    return suggestions


# ============ Historical Fee Comparison ============


@router.get("/fees/historical/{port_code}")
async def get_historical_fee_trends(
    port_code: str,
    vessel_type: Optional[str] = Query(None),
    months: int = Query(12, ge=1, le=60, description="Number of months to analyze"),
    db: Session = Depends(get_db),
):
    """Historical fee trends (mock data). Replace with real data source when available."""
    _ = db  # reserved for real implementation

    return {
        "mock": True,
        "port_code": port_code,
        "vessel_type": vessel_type,
        "period_months": months,
        "fee_trends": {
            "cbp_user_fee": {"current": 587.03, "previous_year": 571.81, "change_percent": 2.66},
            "aphis": {"current": 2903.73, "previous_year": 2827.00, "change_percent": 2.71},
            "pilotage": {"current_avg": 12000, "previous_year_avg": 11500, "change_percent": 4.35},
        },
        "seasonal_patterns": {
            "high_season": ["June", "July", "August"],
            "low_season": ["January", "February"],
            "congestion_surcharge_months": ["October", "November", "December"],
        },
        "recommendations": [
            "Book Q1 arrivals for 10–15% lower pilotage rates (region dependent).",
            "Expect 5–10% congestion surcharges during peak season.",
            "CBP fees typically reset Oct 1 (U.S. fiscal year).",
        ],
    }
