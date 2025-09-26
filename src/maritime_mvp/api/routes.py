# src/maritime_mvp/api/routes.py
"""
Enhanced API routes with comprehensive fee calculation and document management.

Notes:
- All SQL uses bound parameters (no f-strings).
- Multi-port endpoint uses embedded body models; JSON must include {"request": {...}, "vessel": {...}}.
- Port search/detail uses imo_ports when present; otherwise falls back to ports.
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
from ..rules.fee_engine import (
    FeeEngine,
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
    ytd_cbp_paid: Decimal = Field(Decimal("0"))
    tonnage_year_paid: Decimal = Field(Decimal("0"))
    include_optional_services: bool = Field(True)


class DocumentRequirement(BaseModel):
    document_name: str
    document_code: str
    is_mandatory: bool
    lead_time_hours: int
    authority: str
    description: Optional[str] = None


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
    try:
        return VesselType((s or "general_cargo").lower())
    except Exception:
        return VesselType.GENERAL_CARGO


def _dec(val: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(val))
    except Exception:
        return Decimal(default)


def _arrival_type(prev_port_code: Optional[str]) -> str:
    if prev_port_code and prev_port_code.strip().upper().startswith("US"):
        return "COASTWISE"
    return "FOREIGN"


def _table_exists(db: Session, table_name: str) -> bool:
    try:
        row = db.execute(text("SELECT to_regclass(:tname)"), {"tname": f"public.{table_name}"}).fetchone()
        return bool(row and row[0])
    except Exception:
        return False


def _use_imo_ports(db: Session) -> bool:
    return _table_exists(db, "imo_ports")


def _use_port_documents(db: Session) -> bool:
    return _table_exists(db, "port_documents")


def _has_voyage_estimates(db: Session) -> bool:
    return _table_exists(db, "voyage_estimates")


def _port_exists(db: Session, code: str) -> bool:
    row = db.execute(text("SELECT 1 FROM ports WHERE code = :c LIMIT 1"), {"c": code}).fetchone()
    return bool(row)


# UN/LOCODE → internal ports.code direct mapping and name-based fallback
_UNLOCODE_MAP = {
    # LA/LB
    "USLAX": "LALB",
    "USLGB": "LALB",
    # SF Bay
    "USOAK": "SFBAY",
    "USSFO": "SFBAY",
    # Puget Sound
    "USSEA": "PUGET",
    "USTAC": "PUGET",
    # Columbia River
    "USPDX": "COLRIV",
    "USAST": "COLRIV",
}

def _resolve_port_code(db: Session, locode_or_internal: str) -> str:
    """
    Resolve an incoming code (could be UN/LOCODE or internal ports.code) to an internal ports.code.
    - If already an internal code in ports, use it.
    - Else use the static UN/LOCODE map.
    - Else try imo_ports name-based heuristics to choose an internal region code.
    - Else raise 422 with instructions to add a mapping or use an internal code.
    """
    code = (locode_or_internal or "").strip().upper()
    if not code:
        raise HTTPException(status_code=422, detail="Missing port code")

    # Already an internal code?
    if _port_exists(db, code):
        return code

    # Direct UN/LOCODE map
    mapped = _UNLOCODE_MAP.get(code)
    if mapped and _port_exists(db, mapped):
        return mapped

    # Name-based mapping via imo_ports (if available)
    if _use_imo_ports(db):
        row = db.execute(
            text("SELECT port_name FROM imo_ports WHERE locode = :c"),
            {"c": code},
        ).fetchone()
        if row:
            name = (row[0] or "").lower()
            # Stockton (dedicated)
            if "stockton" in name:
                if _port_exists(db, "STKN"):
                    return "STKN"
                if _port_exists(db, "SFBAY"):
                    return "SFBAY"
            # Bay Area family
            if any(x in name for x in ["san francisco", "oakland", "richmond", "alameda", "redwood", "sacramento"]):
                if _port_exists(db, "SFBAY"):
                    return "SFBAY"
            # LA/LB
            if any(x in name for x in ["los angeles", "long beach", "san pedro", "hueneme"]):
                if _port_exists(db, "LALB"):
                    return "LALB"
            # Puget
            if any(x in name for x in ["seattle", "tacoma", "everett", "olympia", "bellingham", "anacortes"]):
                if _port_exists(db, "PUGET"):
                    return "PUGET"
            # Columbia River
            if any(x in name for x in ["portland", "astoria", "columbia", "vancouver", "longview", "kalama", "rainier"]):
                if _port_exists(db, "COLRIV"):
                    return "COLRIV"

    raise HTTPException(
        status_code=422,
        detail=f"Unsupported port code '{code}'. Use an internal code (one of ports.code) or add a UN/LOCODE mapping.",
    )


# ============ Ports: search and details ============

@router.get("/ports/search", response_model=List[PortInfo])
async def search_ports(
    q: str = Query(..., min_length=2, description="Search by name or code"),
    country: Optional[str] = Query(None, description="Filter by country code (e.g., US)"),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Search ports. Uses imo_ports if available; otherwise falls back to ports.
    """
    if _use_imo_ports(db):
        sql = text(
            """
            SELECT locode, port_name, country_code, region
            FROM imo_ports
            WHERE (port_name ILIKE :q OR locode ILIKE :q OR port_code ILIKE :q)
              AND (:country IS NULL OR country_code = :country)
            ORDER BY CASE WHEN locode = UPPER(:rawq) THEN 0 ELSE 1 END, port_name
            LIMIT :limit
            """
        )
        rows = db.execute(sql, {"q": f"%{q}%", "rawq": q, "country": country, "limit": limit}).fetchall()
        return [PortInfo(locode=r[0], port_name=r[1], country_code=r[2], region=r[3]) for r in rows]

    # Fallback to ports table (code, name, country, region)
    sql = text(
        """
        SELECT code, name, country, region
        FROM ports
        WHERE (name ILIKE :q OR code ILIKE :q)
          AND (:country IS NULL OR country = :country)
        ORDER BY CASE WHEN code = UPPER(:rawq) THEN 0 ELSE 1 END, name
        LIMIT :limit
        """
    )
    rows = db.execute(sql, {"q": f"%{q}%", "rawq": q, "country": country, "limit": limit}).fetchall()
    return [PortInfo(locode=r[0], port_name=r[1], country_code=r[2], region=r[3]) for r in rows]


@router.get("/ports/{locode}", response_model=PortInfo)
async def get_port_details(locode: str, db: Session = Depends(get_db)):
    """
    Get detailed information for a specific UN/LOCODE. Uses imo_ports if present; otherwise ports.
    """
    code = locode.upper()
    if _use_imo_ports(db):
        sql = text(
            """
            SELECT locode, port_name, country_code, region
            FROM imo_ports
            WHERE locode = :loc
            """
        )
        row = db.execute(sql, {"loc": code}).fetchone()
        if row:
            return PortInfo(locode=row[0], port_name=row[1], country_code=row[2], region=row[3])

    # Fallback to ports (treat given code as internal)
    sql2 = text(
        """
        SELECT code, name, country, region
        FROM ports
        WHERE code = :loc
        """
    )
    row2 = db.execute(sql2, {"loc": code}).fetchone()
    if row2:
        return PortInfo(locode=row2[0], port_name=row2[1], country_code=row2[2], region=row2[3])

    raise HTTPException(status_code=404, detail=f"Port {locode} not found")


# ============ Comprehensive Fee Estimation ============

@router.post("/estimate/comprehensive")
async def calculate_comprehensive_estimate(
    request: ComprehensiveEstimateRequest, db: Session = Depends(get_db)
):
    """
    Calculate comprehensive port call fees for a vessel + voyage.
    Persists a summary to voyage_estimates and returns the generated id.
    """
    vtype = _parse_vessel_type(request.vessel.vessel_type)
    vessel = VesselSpecs(
        name=request.vessel.name,
        imo_number=request.vessel.imo_number,
        vessel_type=vtype,
        gross_tonnage=_dec(request.vessel.gross_tonnage),
        net_tonnage=_dec(request.vessel.net_tonnage),
        loa_meters=_dec(request.vessel.loa_meters),
        beam_meters=_dec(request.vessel.beam_meters),
        draft_meters=_dec(request.vessel.draft_meters),
    )

    prev_code = (request.voyage.previous_port_code or "").strip().upper()
    arr_locode_or_internal = (request.voyage.arrival_port_code or "").strip().upper()
    next_code = (request.voyage.next_port_code or None)
    next_code = next_code.strip().upper() if next_code else None

    # Resolve arrival port to internal code for FeeEngine
    internal_port_code = _resolve_port_code(db, arr_locode_or_internal)

    voyage = VoyageContext(
        previous_port_code=prev_code,
        arrival_port_code=internal_port_code,
        next_port_code=next_code,
        eta=request.voyage.eta,
        etd=request.voyage.etd,
        days_alongside=max(1, int(request.voyage.days_alongside or 1)),
    )

    engine = FeeEngine(db)
    engine.ytd_cbp_paid = _dec(request.ytd_cbp_paid)
    engine.tonnage_year_paid = _dec(request.tonnage_year_paid)

    result = engine.calculate_comprehensive(vessel, voyage)

    # Honor include_optional_services by stripping optional calcs and recomputing totals
    if not request.include_optional_services:
        calcs = result.get("calculations", [])
        keep = [c for c in calcs if not c.get("is_optional")]
        mand = sum(_dec(c.get("final_amount", "0")) for c in keep)
        result["calculations"] = keep
        result["totals"] = {
            "mandatory": str(mand),
            "optional_low": "0.00",
            "optional_high": "0.00",
            "total_low": str(mand),
            "total_high": str(mand),
        }

    # Persist to voyage_estimates
    voyage_id = str(uuid4())
    if _has_voyage_estimates(db):
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
                        created_at, updated_at
                    )
                    VALUES (
                        :id, :vname, :vtype, :imo,
                        :prev, :arr, :next,
                        :gt, :nt, :loa, :beam, :draft,
                        :eta, :etd, :days,
                        :mand, :opt, :conf,
                        NOW(), NOW()
                    )
                    """
                ),
                {
                    "id": voyage_id,
                    "vname": vessel.name,
                    "vtype": vessel.vessel_type.value,
                    "imo": vessel.imo_number,
                    "prev": prev_code,
                    # store the presented arrival code (locode or internal?) → keep original UN/LOCODE if provided
                    "arr": arr_locode_or_internal,
                    "next": next_code,
                    "gt": _dec(vessel.gross_tonnage),
                    "nt": _dec(vessel.net_tonnage),
                    "loa": _dec(vessel.loa_meters),
                    "beam": _dec(vessel.beam_meters),
                    "draft": _dec(vessel.draft_meters),
                    "eta": request.voyage.eta,
                    "etd": request.voyage.etd,
                    "days": max(1, int(request.voyage.days_alongside or 1)),
                    "mand": _dec(result.get("totals", {}).get("mandatory", "0")),
                    "opt": _dec(result.get("totals", {}).get("optional_high", "0")),
                    "conf": _dec(result.get("confidence", "0.9"), "0.9"),
                },
            )
            db.commit()
        except Exception:
            logger.warning("voyage_estimates insert failed/skipped", exc_info=True)

    result["estimate_id"] = voyage_id
    result.setdefault("meta", {})["arrival_type"] = _arrival_type(prev_code)
    result.setdefault("meta", {})["arrival_port_internal_code"] = internal_port_code
    return result


# ============ Document Requirements ============

def _document_requirements_core(
    db: Session,
    port_code_input: str,
    vessel_type: Optional[str],
    previous_port: Optional[str],
) -> List[DocumentRequirement]:
    """
    Build document requirements from port_documents:
    - Includes ALL_US and specific port_code matches.
    - Honors applies_to_vessel_types (array) and applies_if_foreign (bool).
    - Always adds CBP-1300 for foreign arrivals.
    """
    docs: List[DocumentRequirement] = []
    if not _use_port_documents(db):
        # If table missing, just add the foreign-arrival rule when applicable
        if previous_port and not previous_port.strip().upper().startswith("US"):
            docs.append(
                DocumentRequirement(
                    document_name="Customs Declaration for Foreign Arrival",
                    document_code="CBP-1300",
                    is_mandatory=True,
                    lead_time_hours=24,
                    authority="CBP",
                    description="Required for all arrivals from foreign ports.",
                )
            )
        return docs

    port_code = (port_code_input or "").strip().upper()
    vt = (vessel_type or "").strip().lower() or None
    is_foreign = not ((previous_port or "").strip().upper().startswith("US"))

    # Also consider internal code variant for port_documents if you store internal codes there
    internal_code = None
    try:
        internal_code = _resolve_port_code(db, port_code)
    except Exception:
        internal_code = None

    port_codes_to_check = [c for c in {port_code, internal_code} if c]

    # Common + specific
    sql = text(
        """
        SELECT document_name, document_code, COALESCE(is_mandatory, true),
               COALESCE(lead_time_hours, 0), COALESCE(authority, ''), description
        FROM port_documents
        WHERE (port_code = 'ALL_US' OR port_code = ANY(:pcs))
          AND (applies_to_vessel_types IS NULL OR :vt = ANY(applies_to_vessel_types))
          AND (COALESCE(applies_if_foreign, false) = false OR :is_foreign = true)
        ORDER BY document_name
        """
    )
    rows = db.execute(
        sql,
        {
            "pcs": port_codes_to_check,
            "vt": vt,
            "is_foreign": is_foreign,
        },
    ).fetchall()

    # Deduplicate by (document_code, document_name)
    seen: set[tuple[str, str]] = set()
    for r in rows:
        key = ((r[1] or "").upper(), (r[0] or "").lower())
        if key in seen:
            continue
        seen.add(key)
        docs.append(
            DocumentRequirement(
                document_name=r[0],
                document_code=r[1] or "",
                is_mandatory=bool(r[2]),
                lead_time_hours=int(r[3] or 0),
                authority=r[4] or "",
                description=r[5],
            )
        )

    # Ensure CBP-1300 for foreign arrivals
    if is_foreign:
        docs.append(
            DocumentRequirement(
                document_name="Customs Declaration for Foreign Arrival",
                document_code="CBP-1300",
                is_mandatory=True,
                lead_time_hours=24,
                authority="CBP",
                description="Required for all arrivals from foreign ports.",
            )
        )

    return docs


@router.get("/documents/requirements", response_model=List[DocumentRequirement])
async def get_document_requirements(
    port_code: str = Query(..., description="UN/LOCODE or internal code"),
    vessel_type: Optional[str] = Query(None, description="Vessel type (e.g., container, tanker)"),
    previous_port: Optional[str] = Query(None, description="Previous port code (UN/LOCODE)"),
    db: Session = Depends(get_db),
):
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
        arrival_port_input = request.ports[i + 1].strip().upper()
        next_port = request.ports[i + 2].strip().upper() if (i + 2) < len(request.ports) else None

        # Resolve arrival to internal code
        internal_arrival = _resolve_port_code(db, arrival_port_input)

        eta = datetime.combine(current_date, datetime.min.time())
        etd = datetime.combine(current_date + timedelta(days=request.days_in_port), datetime.min.time())

        voyage = VoyageContext(
            previous_port_code=prev_port,
            arrival_port_code=internal_arrival,
            next_port_code=next_port,
            eta=eta,
            etd=etd,
            days_alongside=max(1, int(request.days_in_port or 1)),
        )

        vessel_specs = VesselSpecs(
            name=vessel.name,
            imo_number=vessel.imo_number,
            vessel_type=vtype,
            gross_tonnage=_dec(vessel.gross_tonnage),
            net_tonnage=_dec(vessel.net_tonnage),
            loa_meters=_dec(vessel.loa_meters),
            beam_meters=_dec(vessel.beam_meters),
            draft_meters=_dec(vessel.draft_meters),
        )

        engine = FeeEngine(db)
        leg_estimate = engine.calculate_comprehensive(vessel_specs, voyage)

        arr_type = _arrival_type(prev_port)
        weekend_arrival = eta.weekday() >= 5  # Sat/Sun
        docs = _document_requirements_core(db, arrival_port_input, vessel.vessel_type, prev_port)

        fees_totals = leg_estimate.get("totals", {})
        voyage_legs.append(
            {
                "leg": i + 1,
                "from_port": prev_port,
                "to_port": arrival_port_input,  # echo original UN/LOCODE if provided
                "internal_port_code": internal_arrival,
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

        current_date += timedelta(days=request.days_in_port + request.days_between_ports)

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
    suggestions: List[str] = []

    weekend_count = sum(1 for leg in legs if leg.get("weekend_arrival"))
    if weekend_count:
        suggestions.append(
            f"Avoid {weekend_count} weekend arrivals to reduce pilotage/port overtime charges."
        )

    high_fee_ports = [leg for leg in legs if _dec(leg["fees"]["mandatory"]) > Decimal("15000")]
    if high_fee_ports:
        suggestions.append(
            f"Consider alternatives or scheduling changes for {len(high_fee_ports)} high-fee legs."
        )

    foreign_count = sum(1 for leg in legs if leg.get("arrival_type") == "FOREIGN")
    if foreign_count > 1:
        suggestions.append("Consider inserting a qualifying U.S. stop to utilize coastwise rates.")

    return suggestions


