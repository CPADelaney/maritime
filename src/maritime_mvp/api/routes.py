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
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Body, Depends
from pydantic import BaseModel, Field
from sqlalchemy import text, select, func
from sqlalchemy.orm import Session, joinedload, selectinload

from ..db import SessionLocal
from ..rules.fee_engine import (
    FeeEngine,
    VesselSpecs,
    VoyageContext,
    VesselType,
)
from ..models import Port, PortZone, Terminal, ContractAdjustment

logger = logging.getLogger("maritime-api")

router = APIRouter(prefix="/api/v2", tags=["Enhanced Maritime API"])

# ============ Pydantic Models ============

class PortInfo(BaseModel):
    locode: str = Field(..., example="USOAK")
    port_name: str = Field(..., example="Oakland, CA")
    country_code: str = Field(..., example="US")
    region: Optional[str] = None
    zone_code: Optional[str] = Field(None, description="Parent zone code when known")


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
    contract_profile: Optional[str] = Field(
        None, description="Optional contract profile key for fee adjustments"
    )


class DocumentRequirement(BaseModel):
    document_name: str
    document_code: str
    is_mandatory: bool
    lead_time_hours: int
    authority: str
    description: Optional[str] = None
    expiry_date: Optional[date] = None
    notes: Optional[str] = None


class VoyageStop(BaseModel):
    zone_code: Optional[str] = Field(None, description="Parent zone code")
    port_code: Optional[str] = Field(None, description="Internal port code")
    terminal_code: Optional[str] = Field(None, description="Terminal identifier")
    days_alongside: Optional[int] = Field(None, description="Days at this stop")


class ContractAdjustmentIn(BaseModel):
    fee_code: str = Field(..., example="PILOTAGE")
    port_code: Optional[str] = Field(
        None, example="SFBAY", description="Internal port code, or null for all ports"
    )
    multiplier: Decimal = Field(
        Decimal("1.0"),
        example="0.90",
        description="Multiply the fee by this factor (e.g., 0.9 for 10% discount)",
    )
    offset: Optional[Decimal] = Field(
        None, example="-500.00", description="Fixed USD offset to add (can be negative)"
    )
    effective_start: date = Field(..., example="2025-01-01")
    effective_end: Optional[date] = Field(None, example="2026-01-01")
    notes: Optional[str] = Field(None, example="10% pilotage discount for Customer A")


class ContractAdjustmentOut(ContractAdjustmentIn):
    id: int
    profile: str


# Basic static fallback rules for common U.S. arrival documents when
# the structured port_documents table is not available.
#
# These are deliberately high-level and meant as planning aids, not a
# full implementation of CBP/USCG requirements.
def _static_fallback_documents(
    port_code: str,
    vessel_type: Optional[str],
    previous_port: Optional[str],
) -> List[DocumentRequirement]:
    docs: List[DocumentRequirement] = []
    prev = (previous_port or "").strip().upper()
    is_foreign = bool(prev) and not prev.startswith("US")

    if is_foreign:
        # Customs declaration for foreign arrivals.
        docs.append(
            DocumentRequirement(
                document_name="Customs Declaration for Foreign Arrival",
                document_code="CBP-1300",
                is_mandatory=True,
                lead_time_hours=24,
                authority="CBP",
                description="Standard customs declaration for arrivals from foreign ports.",
                expiry_date=None,
                notes="Capture the same data historically collected via CBP Form 1300.",
            )
        )

        # Notice of Arrival/Departure (NOA/NOAD) – typical 96-hour window for foreign voyages.
        docs.append(
            DocumentRequirement(
                document_name="Notice of Arrival/Departure (NOA/NOAD)",
                document_code="NOA/NOAD",
                is_mandatory=True,
                lead_time_hours=96,
                authority="USCG",
                description="Advance notice of arrival/departure; commonly 96 hours for foreign voyages.",
                expiry_date=None,
                notes="Aligns with data historically captured on CBP Form 3171.",
            )
        )

        # Ballast water management/reporting – broadly required for seagoing vessels.
        docs.append(
            DocumentRequirement(
                document_name="Ballast Water Management Report",
                document_code="BWMR",
                is_mandatory=True,
                lead_time_hours=24,
                authority="USCG",
                description="Ballast water management/reporting in accordance with U.S. regulations.",
                expiry_date=None,
                notes="Maintain latest submission guidance for CBP review.",
            )
        )

    compliance_docs = [
        DocumentRequirement(
            document_name="Certificate of Financial Responsibility (COFR)",
            document_code="COFR",
            is_mandatory=True,
            lead_time_hours=0,
            authority="USCG/EPA",
            description="Pollution financial responsibility evidence for U.S. waters.",
            expiry_date=None,
            notes="Renewal typically every 2-3 years; keep proof of current coverage.",
        ),
        DocumentRequirement(
            document_name="International Tonnage Certificate",
            document_code="ITC",
            is_mandatory=True,
            lead_time_hours=0,
            authority="Flag State",
            description="Flag-state tonnage certificate kept on board for clearance.",
            expiry_date=None,
            notes="Confirm validity and reissue after major modifications.",
        ),
        DocumentRequirement(
            document_name="Certificate of Documentation or Registry",
            document_code="COD/Registry",
            is_mandatory=True,
            lead_time_hours=0,
            authority="Flag State",
            description="Evidence of vessel registry for entry and clearance.",
            expiry_date=None,
            notes="Verify registry renewals per flag requirements.",
        ),
    ]

    docs.extend(compliance_docs)

    return docs

class PortSequenceRequest(BaseModel):
    vessel_name: str
    ports: List[str] = Field(..., example=["CNSHA", "USOAK", "USSEA", "USLAX"])
    start_date: date = Field(..., example="2025-09-01")
    days_in_port: int = Field(2, example=2)
    stops: Optional[List[VoyageStop]] = Field(
        None,
        description="Optional ordered list of within-zone stops (metadata only)",
    )


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


@dataclass
class ResolvedPort:
    zone_code: str
    zone_name: Optional[str]
    port_code: str
    port_name: Optional[str]


def _primary_port_for_zone(zone: PortZone) -> Optional[Port]:
    ports = list(zone.ports or [])
    if not ports:
        return None
    for port in ports:
        if (port.code or "").upper() == (zone.code or "").upper():
            return port
    return sorted(ports, key=lambda p: (p.name or "", p.code or ""))[0]


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


def _resolved_from_port(port: Port) -> ResolvedPort:
    zone = port.zone
    zone_code = zone.code if zone else port.code
    zone_name = zone.name if zone else port.name
    return ResolvedPort(zone_code=zone_code, zone_name=zone_name, port_code=port.code, port_name=port.name)


def _resolve_port_code(db: Session, locode_or_internal: str) -> ResolvedPort:
    """Resolve a caller-supplied identifier to a Port + its parent zone."""
    raw = (locode_or_internal or "").strip()
    if not raw:
        raise HTTPException(status_code=422, detail="Missing port code")

    code = raw.upper()

    # Zone direct hit
    zone = (
        db.execute(
            select(PortZone)
            .where(func.upper(PortZone.code) == code)
            .options(selectinload(PortZone.ports))
        )
        .scalars()
        .first()
    )
    if zone:
        primary = _primary_port_for_zone(zone)
        if not primary:
            raise HTTPException(status_code=422, detail=f"Zone '{zone.code}' has no associated ports")
        return ResolvedPort(
            zone_code=zone.code,
            zone_name=zone.name,
            port_code=primary.code,
            port_name=primary.name,
        )

    # Internal port code match
    port = (
        db.execute(
            select(Port)
            .where(func.upper(Port.code) == code)
            .options(joinedload(Port.zone))
        )
        .scalars()
        .first()
    )
    if port:
        return _resolved_from_port(port)

    # UN/LOCODE static mapping support
    mapped = _UNLOCODE_MAP.get(code)
    if mapped:
        return _resolve_port_code(db, mapped)

    raw_term = raw

    # Exact port name match (case-insensitive)
    port = (
        db.execute(
            select(Port)
            .where(func.lower(Port.name) == func.lower(raw_term))
            .options(joinedload(Port.zone))
        )
        .scalars()
        .first()
    )
    if port:
        return _resolved_from_port(port)

    # Fuzzy port name match
    port = (
        db.execute(
            select(Port)
            .where(Port.name.ilike(f"%{raw_term}%"))
            .options(joinedload(Port.zone))
            .order_by(func.length(Port.name), Port.name)
            .limit(1)
        )
        .scalars()
        .first()
    )
    if port:
        return _resolved_from_port(port)

    # Terminal lookup by name
    terminal = (
        db.execute(
            select(Terminal)
            .where(func.lower(Terminal.name) == func.lower(raw_term))
            .options(joinedload(Terminal.port).joinedload(Port.zone))
        )
        .scalars()
        .first()
    )
    if not terminal:
        terminal = (
            db.execute(
                select(Terminal)
                .where(Terminal.name.ilike(f"%{raw_term}%"))
                .options(joinedload(Terminal.port).joinedload(Port.zone))
                .order_by(func.length(Terminal.name), Terminal.name)
                .limit(1)
            )
            .scalars()
            .first()
        )
    if terminal:
        port = terminal.port
        if not port:
            raise HTTPException(status_code=422, detail=f"Terminal '{raw_term}' is missing a parent port mapping")
        return _resolved_from_port(port)

    raise HTTPException(
        status_code=422,
        detail=f"Unsupported port or terminal '{raw}'. Add a mapping or use an internal code.",
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
            SELECT locode, port_name, country_code, region, port_code
            FROM imo_ports
            WHERE (port_name ILIKE :q OR locode ILIKE :q OR port_code ILIKE :q)
              AND (:country IS NULL OR country_code = :country)
            ORDER BY CASE WHEN locode = UPPER(:rawq) THEN 0 ELSE 1 END, port_name
            LIMIT :limit
            """
        )
        rows = db.execute(sql, {"q": f"%{q}%", "rawq": q, "country": country, "limit": limit}).fetchall()
        results: List[PortInfo] = []
        for r in rows:
            zone_code = None
            port_code = (r[4] or "").strip()
            if port_code:
                try:
                    resolved = _resolve_port_code(db, port_code)
                    zone_code = resolved.zone_code
                except HTTPException:
                    zone_code = None
            results.append(PortInfo(locode=r[0], port_name=r[1], country_code=r[2], region=r[3], zone_code=zone_code))
        return results

    # Fallback to ports table (code, name, country, region)
    sql = text(
        """
        SELECT p.code, p.name, p.country, p.region, z.code AS zone_code
        FROM ports p
        LEFT JOIN port_zones z ON z.id = p.zone_id
        WHERE (name ILIKE :q OR code ILIKE :q)
          AND (:country IS NULL OR country = :country)
        ORDER BY CASE WHEN code = UPPER(:rawq) THEN 0 ELSE 1 END, name
        LIMIT :limit
        """
    )
    rows = db.execute(sql, {"q": f"%{q}%", "rawq": q, "country": country, "limit": limit}).fetchall()
    return [PortInfo(locode=r[0], port_name=r[1], country_code=r[2], region=r[3], zone_code=r[4]) for r in rows]


@router.get("/ports/{locode}", response_model=PortInfo)
async def get_port_details(locode: str, db: Session = Depends(get_db)):
    """
    Get detailed information for a specific UN/LOCODE. Uses imo_ports if present; otherwise ports.
    """
    code = locode.upper()
    if _use_imo_ports(db):
        sql = text(
            """
            SELECT locode, port_name, country_code, region, port_code
            FROM imo_ports
            WHERE locode = :loc
            """
        )
        row = db.execute(sql, {"loc": code}).fetchone()
        if row:
            zone_code = None
            port_code = (row[4] or "").strip()
            if port_code:
                try:
                    resolved = _resolve_port_code(db, port_code)
                    zone_code = resolved.zone_code
                except HTTPException:
                    zone_code = None
            return PortInfo(locode=row[0], port_name=row[1], country_code=row[2], region=row[3], zone_code=zone_code)

    # Fallback to ports (treat given code as internal)
    sql2 = text(
        """
        SELECT p.code, p.name, p.country, p.region, z.code AS zone_code
        FROM ports p
        LEFT JOIN port_zones z ON z.id = p.zone_id
        WHERE code = :loc
        """
    )
    row2 = db.execute(sql2, {"loc": code}).fetchone()
    if row2:
        return PortInfo(locode=row2[0], port_name=row2[1], country_code=row2[2], region=row2[3], zone_code=row2[4])

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
    arr_input_raw = (request.voyage.arrival_port_code or "").strip()
    arr_locode_or_internal = arr_input_raw.upper()
    next_code = (request.voyage.next_port_code or None)
    next_code = next_code.strip().upper() if next_code else None

    # Resolve arrival port to internal code for FeeEngine
    resolved_port = _resolve_port_code(db, arr_input_raw)
    internal_port_code = resolved_port.port_code

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
    engine.contract_profile = request.contract_profile

    result = engine.calculate_comprehensive(vessel, voyage)

    # Honor include_optional_services by stripping optional calcs and recomputing totals
    if not request.include_optional_services:
        calcs = result.get("calculations", [])
        keep = [c for c in calcs if not c.get("is_optional")]
        mand = sum(_dec(c.get("final_amount", "0")) for c in keep)
        result["calculations"] = keep
        result["totals"] = {
            "mandatory": str(mand),
            "best_case_optional": "0.00",
            "best_case_total": str(mand),
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
    meta = result.setdefault("meta", {})
    meta["arrival_type"] = _arrival_type(prev_code)
    meta["arrival_port_internal_code"] = internal_port_code
    meta["arrival_port_zone_code"] = resolved_port.zone_code
    meta["arrival_port_zone_name"] = resolved_port.zone_name
    meta["arrival_port_name"] = resolved_port.port_name
    return result


# ============ Document Requirements ============

def _document_requirements_core(
    db: Session,
    port_code_input: str,
    vessel_type: Optional[str],
    previous_port: Optional[str],
    vessel_imo: Optional[str] = None,
    vessel_name: Optional[str] = None,
) -> List[DocumentRequirement]:
    """
    Build document requirements from port_documents:
    - Includes ALL_US and specific port_code matches.
    - Honors applies_to_vessel_types (array) and applies_if_foreign (bool).
    - Always adds CBP-1300 for foreign arrivals.
    """
    docs: List[DocumentRequirement] = []
    if not _use_port_documents(db):
        # If the structured port_documents table is missing, fall back to a
        # small set of common U.S. arrival docs derived from public guidance.
        return _static_fallback_documents(port_code_input, vessel_type, previous_port)

    port_code = (port_code_input or "").strip().upper()
    vt = (vessel_type or "").strip().lower() or None
    is_foreign = not ((previous_port or "").strip().upper().startswith("US"))

    # Also consider internal code variant for port_documents if you store internal codes there
    resolved = None
    try:
        resolved = _resolve_port_code(db, port_code)
    except HTTPException:
        resolved = None

    code_candidates = {port_code.strip().upper()}
    if resolved:
        if resolved.port_code:
            code_candidates.add(resolved.port_code.upper())
        if resolved.zone_code:
            code_candidates.add(resolved.zone_code.upper())

    port_codes_to_check = [c for c in code_candidates if c]

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
                expiry_date=None,
                notes=None,
            )
        )

    fallback_docs = _static_fallback_documents(port_code_input, vessel_type, previous_port)
    for doc in fallback_docs:
        key = ((doc.document_code or "").upper(), (doc.document_name or "").lower())
        if key in seen:
            continue
        seen.add(key)
        docs.append(doc)

    # Ensure CBP-1300 for foreign arrivals
    if is_foreign:
        cbp_key = ("CBP-1300", "customs declaration for foreign arrival")
        if cbp_key not in seen:
            docs.append(
                DocumentRequirement(
                    document_name="Customs Declaration for Foreign Arrival",
                    document_code="CBP-1300",
                    is_mandatory=True,
                    lead_time_hours=24,
                    authority="CBP",
                    description="Required for all arrivals from foreign ports.",
                    expiry_date=None,
                    notes="Collects arrival particulars in line with CBP processes.",
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


# ============ Contract Adjustments (Profiles) ============


@router.get("/contracts/{profile}", response_model=List[ContractAdjustmentOut], tags=["Contracts"])
async def list_contract_adjustments(
    profile: str,
    db: Session = Depends(get_db),
):
    """List all contract adjustments for a given profile."""

    rows = (
        db.execute(
            select(ContractAdjustment)
            .where(ContractAdjustment.profile == profile)
            .order_by(
                ContractAdjustment.fee_code,
                ContractAdjustment.port_code.nullsfirst(),
                ContractAdjustment.effective_start.desc(),
            )
        )
        .scalars()
        .all()
    )
    return [
        ContractAdjustmentOut(
            id=r.id,
            profile=r.profile,
            fee_code=r.fee_code,
            port_code=r.port_code,
            multiplier=r.multiplier,
            offset=r.offset,
            effective_start=r.effective_start,
            effective_end=r.effective_end,
            notes=r.notes,
        )
        for r in rows
    ]


@router.post("/contracts/{profile}/upsert", response_model=ContractAdjustmentOut, tags=["Contracts"])
async def upsert_contract_adjustment(
    profile: str,
    body: ContractAdjustmentIn,
    db: Session = Depends(get_db),
):
    """
    Create or update a contract adjustment for a given profile + fee_code + port_code.

    If a current row exists (effective_end is null and same fee_code/port_code),
    it is updated in-place. Otherwise, a new row is inserted.
    """

    port_clause = (
        ContractAdjustment.port_code.is_(None)
        if body.port_code is None
        else ContractAdjustment.port_code == body.port_code
    )

    existing = (
        db.execute(
            select(ContractAdjustment).where(
                ContractAdjustment.profile == profile,
                ContractAdjustment.fee_code == body.fee_code,
                port_clause,
                ContractAdjustment.effective_end.is_(None),
            )
        )
        .scalars()
        .first()
    )

    if existing:
        existing.multiplier = body.multiplier
        existing.offset = body.offset
        existing.effective_start = body.effective_start
        existing.effective_end = body.effective_end
        existing.notes = body.notes
        row = existing
    else:
        row = ContractAdjustment(
            profile=profile,
            fee_code=body.fee_code,
            port_code=body.port_code,
            multiplier=body.multiplier,
            offset=body.offset,
            effective_start=body.effective_start,
            effective_end=body.effective_end,
            notes=body.notes,
        )
        db.add(row)

    db.commit()
    db.refresh(row)

    return ContractAdjustmentOut(
        id=row.id,
        profile=row.profile,
        fee_code=row.fee_code,
        port_code=row.port_code,
        multiplier=row.multiplier,
        offset=row.offset,
        effective_start=row.effective_start,
        effective_end=row.effective_end,
        notes=row.notes,
    )


@router.delete("/contracts/{profile}/{fee_code}", tags=["Contracts"])
async def delete_contract_adjustments(
    profile: str,
    fee_code: str,
    port_code: Optional[str] = Query(None, description="Optional internal port code filter"),
    db: Session = Depends(get_db),
):
    """
    Delete contract adjustments for a profile + fee_code, optionally scoped to a specific port_code.
    """

    q = select(ContractAdjustment).where(
        ContractAdjustment.profile == profile, ContractAdjustment.fee_code == fee_code
    )
    if port_code:
        q = q.where(ContractAdjustment.port_code == port_code)

    rows = db.execute(q).scalars().all()
    for r in rows:
        db.delete(r)
    db.commit()
    return {"deleted": len(rows)}


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
        arrival_port_raw = request.ports[i + 1].strip()
        arrival_port_input = arrival_port_raw.upper()
        next_port = request.ports[i + 2].strip().upper() if (i + 2) < len(request.ports) else None

        # Resolve arrival to internal code
        resolved_arrival = _resolve_port_code(db, arrival_port_raw)
        internal_arrival = resolved_arrival.port_code

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
        docs = _document_requirements_core(db, arrival_port_raw, vessel.vessel_type, prev_port)

        fees_totals = leg_estimate.get("totals", {}) or {}
        best_optional = _dec(fees_totals.get("best_case_optional", fees_totals.get("optional_low", "0")))
        best_total = _dec(fees_totals.get("best_case_total", fees_totals.get("total_low", "0")))

        # Slimmed per-fee breakdown for this leg
        fee_breakdown = []
        for c in leg_estimate.get("calculations", []) or []:
            try:
                fee_breakdown.append(
                    {
                        "code": c.get("code"),
                        "name": c.get("name"),
                        "final_amount": str(_dec(c.get("final_amount", "0"))),
                        "base_amount": str(_dec(c.get("base_amount", "0"))),
                        "is_optional": bool(c.get("is_optional")),
                    }
                )
            except Exception:
                # If anything is weird, don't blow up the whole leg
                continue

        voyage_legs.append(
            {
                "leg": i + 1,
                "from_port": prev_port,
                "to_port": arrival_port_input,  # echo original UN/LOCODE if provided
                "internal_port_code": internal_arrival,
                "zone_code": resolved_arrival.zone_code,
                "eta": eta.isoformat(),
                "etd": etd.isoformat(),
                "fees": {
                    "mandatory": str(_dec(fees_totals.get("mandatory", "0"))),
                    "best_case_optional": str(best_optional),
                    "best_case_total": str(best_total),
                    "optional_low": str(_dec(fees_totals.get("optional_low", "0"))),
                    "optional_high": str(_dec(fees_totals.get("optional_high", "0"))),
                },
                "totals": {
                    "mandatory": str(_dec(fees_totals.get("mandatory", "0"))),
                    "best_case_optional": str(best_optional),
                    "best_case_total": str(best_total),
                    "optional_low": str(_dec(fees_totals.get("optional_low", "0"))),
                    "optional_high": str(_dec(fees_totals.get("optional_high", "0"))),
                    "total_low": str(best_total),
                    "total_high": str(_dec(fees_totals.get("total_high", "0"))),
                },
                "fee_breakdown": fee_breakdown,
                "arrival_type": arr_type,
                "weekend_arrival": weekend_arrival,
                "documents_required": len(docs),
            }
        )

        current_date += timedelta(days=request.days_in_port)

    total_mandatory = sum(_dec(leg["fees"]["mandatory"]) for leg in voyage_legs)
    total_best_case = sum(_dec(leg["fees"].get("best_case_optional", "0")) for leg in voyage_legs)

    return {
        "vessel_name": request.vessel_name,
        "voyage_summary": {
            "total_ports": len(request.ports),
            "total_legs": len(voyage_legs),
            "total_days_in_port": (current_date - request.start_date).days,
            "start_date": request.start_date.isoformat(),
            "end_date": current_date.isoformat(),
        },
        "rotation": [stop.model_dump() for stop in request.stops] if request.stops else [],
        "legs": voyage_legs,
        "total_voyage_cost": {
            "mandatory": str(total_mandatory),
            "best_case_total": str(total_mandatory + total_best_case),
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


