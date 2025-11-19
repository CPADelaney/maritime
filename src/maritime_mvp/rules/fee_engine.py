# src/maritime_mvp/rules/fee_engine.py
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Optional, List, Dict, Tuple, Any, Iterable, cast, Union

from sqlalchemy import select, or_
from sqlalchemy.orm import Session

from .dockage import DockageEngine
from .tonnage_schedule import LOWER_RATE_PER_TON, LOWER_CAP_PER_TON_PER_YEAR
from ..models import Fee, Port, VesselTypeConfig, PilotageRate, ContractAdjustment
from .rates_loader import (
    MISSING_RATE_FIELD,
    load_pilotage_rates,
)

logger = logging.getLogger(__name__)

# Optional US holidays (federal + state); fall back gracefully if unavailable
try:  # pragma: no cover
    import holidays as _holidays
    _HOLIDAYS_AVAILABLE = True
except Exception:  # pragma: no cover
    _HOLIDAYS_AVAILABLE = False


# -------------------------------
# Helpers & common data models
# -------------------------------

def _money(x: Decimal | int | float | str) -> Decimal:
    if not isinstance(x, Decimal):
        x = Decimal(str(x))
    return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


@dataclass
class LineItem:
    code: str
    name: str
    amount: Decimal
    details: dict


@dataclass
class MovementLeg:
    """Representation of a single pilotage leg.

    This mirrors the attributes delivered by the movement event stream so that
    the fee engine can perform per-leg classification and pricing.
    """

    sequence: int
    leg_type: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    from_location: Optional[str] = None
    to_location: Optional[str] = None
    draft_feet: Optional[Decimal] = None
    notes: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def normalised_type(self) -> str:
        txt = (self.leg_type or "").strip().lower()
        return "_".join(part for part in txt.replace("-", " ").replace("/", " ").split() if part)

    def to_metadata(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if self.from_location:
            payload["from_location"] = self.from_location
        if self.to_location:
            payload["to_location"] = self.to_location
        if self.start_time:
            payload["start_time"] = self.start_time.isoformat()
        if self.end_time:
            payload["end_time"] = self.end_time.isoformat()
        if self.draft_feet is not None:
            payload["draft_feet"] = str(_money(self.draft_feet))
        if self.notes:
            payload["notes"] = self.notes
        if self.metadata:
            payload.update(self.metadata)
        return payload


# ---------- Back-compat context (existing callers use this) ----------
@dataclass
class EstimateContext:
    port_code: str
    arrival_date: date
    arrival_type: Optional[str] = None  # "FOREIGN" | "COASTWISE"
    previous_port_code: Optional[str] = None
    net_tonnage: Optional[Decimal] = None
    ytd_cbp_paid: Decimal = Decimal("0.00")
    tonnage_year_paid: Decimal = Decimal("0.00")
    is_ballasted: bool = True


# ---------- Enhanced context & specs ----------
class VesselType(Enum):
    CONTAINER = "container"
    TANKER = "tanker"
    BULK_CARRIER = "bulk_carrier"
    CRUISE = "cruise"
    RORO = "roro"
    GENERAL_CARGO = "general_cargo"
    LNG = "lng"
    VEHICLE_CARRIER = "vehicle_carrier"


@dataclass
class VesselSpecs:
    """Complete vessel specifications for accurate fee calculation."""
    name: str
    imo_number: Optional[str] = None
    vessel_type: VesselType = VesselType.GENERAL_CARGO
    gross_tonnage: Decimal = Decimal("0")
    net_tonnage: Decimal = Decimal("0")
    loa_meters: Decimal = Decimal("0")   # Length Overall in meters
    beam_meters: Decimal = Decimal("0")
    draft_meters: Decimal = Decimal("0")

    @property
    def loa_feet(self) -> Decimal:
        return self.loa_meters * Decimal("3.28084")

    @property
    def draft_feet(self) -> Decimal:
        return self.draft_meters * Decimal("3.28084")


@dataclass
class VoyageContext:
    """Complete voyage context including port sequence."""
    previous_port_code: str  # UN/LOCODE like "CNSHA" or "USLAX"
    arrival_port_code: str   # Internal like "LALB" / "SFBAY" or UN/LOCODE depending on your schema
    next_port_code: Optional[str] = None

    eta: datetime = field(default_factory=datetime.now)
    etd: Optional[datetime] = None
    days_alongside: int = 2

    @property
    def arrival_type(self) -> str:
        if self.previous_port_code and self.previous_port_code.upper().startswith("US"):
            return "COASTWISE"
        return "FOREIGN"

    @property
    def is_weekend_arrival(self) -> bool:
        return self.eta.weekday() >= 5  # Sat=5, Sun=6

    @property
    def is_holiday(self) -> bool:
        # Simple local fallback; comprehensive engine overrides this with state-aware detection.
        holidays = {(1, 1), (7, 4), (12, 25)}
        return (self.eta.month, self.eta.day) in holidays


@dataclass
class FeeCalculation:
    """Detailed calculation with confidence and breakdown."""
    code: str
    name: str
    base_amount: Decimal
    multipliers: Dict[str, Decimal] = field(default_factory=dict)
    final_amount: Decimal = Decimal("0")
    confidence: Decimal = Decimal("1.0")  # 0..1
    calculation_details: str = ""
    is_optional: bool = False
    estimated_range: Optional[Tuple[Decimal, Decimal]] = None
    manual_entry: bool = False


# -------------------------------
# Unified Fee Engine
# -------------------------------

class FeeEngine:
    """
    Unified engine:
      - Keeps the original simple API: compute(EstimateContext) -> list[LineItem]
      - Adds a comprehensive API: calculate_comprehensive(VesselSpecs, VoyageContext) -> dict
      - Always prefers DB-configured fees via _active_fee(); falls back to robust formulas.
    """

    # --- Formula defaults (used when DB fee not present) ---

    # Tonnage tax per-net-ton by vessel type (fallback)
    TONNAGE_RATES = {
        VesselType.CONTAINER: Decimal("0.06"),
        VesselType.TANKER: Decimal("0.08"),
        VesselType.BULK_CARRIER: Decimal("0.05"),
        VesselType.CRUISE: Decimal("0.10"),
        VesselType.RORO: Decimal("0.07"),
        VesselType.GENERAL_CARGO: Decimal("0.06"),
        VesselType.LNG: Decimal("0.09"),
        VesselType.VEHICLE_CARRIER: Decimal("0.07"),
    }

    # APHIS fallback by risk bucket
    APHIS_RISK_RATES = {
        "high_risk": Decimal("2903.73"),
        "medium_risk": Decimal("2000.00"),
        "low_risk": Decimal("1500.00"),
        "cascadia": Decimal("837.51"),
        "domestic": Decimal("500.00"),
    }

    HIGH_RISK_COUNTRIES = {"CN", "VN", "TH", "ID", "MY", "PH", "IN", "KR"}

    # Marine Exchange fallback by port
    MX_FALLBACK = {
        "LALB": Decimal("350"),
        "USOAK": Decimal("325"),
        "USSFO": Decimal("375"),
        "USSEA": Decimal("300"),
        "USPDX": Decimal("275"),
        # internal codes too:
        "SFBAY": Decimal("325"),
        "PUGET": Decimal("300"),
        "COLRIV": Decimal("275"),
        "STKN": Decimal("275"),
    }

    # Pilotage formula fallbacks used when registry lookups fail.
    _LEGACY_PILOTAGE_PORT_RATES = {
        "LALB": {"base": 3500, "per_foot": 8.50, "draft_mult": 1.15},
        "USOAK": {"base": 3200, "per_foot": 7.75, "draft_mult": 1.12},
        "USSFO": {"base": 4200, "per_foot": 9.50, "draft_mult": 1.16},
        "USSEA": {"base": 4000, "per_foot": 9.25, "draft_mult": 1.18},
        "USPDX": {"base": 3800, "per_foot": 8.00, "draft_mult": 1.20},
        # internal codes mirror regional baselines
        "SFBAY": {"base": 3500, "per_foot": 8.75, "draft_mult": 1.14},
        "PUGET": {"base": 4000, "per_foot": 9.25, "draft_mult": 1.18},
        "COLRIV": {"base": 3800, "per_foot": 8.00, "draft_mult": 1.20},
    }

    def __init__(self, db: Session, *, show_legacy_optional: bool = False):
        self.db = db
        # Rolling caps for comprehensive API; the simple API takes caps from ctx
        self.ytd_cbp_paid = Decimal("0.00")
        self.tonnage_year_paid = Decimal("0.00")
        # Small in-memory caches so we don't hit the DB repeatedly for static config
        self._vessel_type_cache: Dict[str, Optional[VesselTypeConfig]] = {}
        # key: (port_code, date)
        self._pilotage_rate_cache: Dict[Tuple[str, date], Optional[PilotageRate]] = {}
        # Optional contract profile for this calculation run
        self.contract_profile: Optional[str] = None
        # cache for contract adjustments keyed by (profile, port_code)
        self._contract_adj_cache: Dict[
            Tuple[str, str], Dict[str, Tuple[Decimal, Optional[Decimal]]]
        ] = {}
        # Legacy optional services (launch, garbage, fresh water) are hidden by default.
        self.show_legacy_optional = show_legacy_optional

    # ------------- Holiday helper -------------

    def _is_us_holiday(self, on: date, state: Optional[str]) -> bool:
        """
        True if 'on' is a US holiday (federal or state, when provided).
        Falls back to a minimal fixed-date set if 'holidays' is unavailable.
        """
        if _HOLIDAYS_AVAILABLE:
            try:
                fed = _holidays.UnitedStates()
                if on in fed:
                    return True
                if state:
                    try:
                        st = _holidays.US(state=state.upper())
                        return on in st
                    except Exception:
                        return False
            except Exception:
                logger.debug("holidays lookup failed; falling back", exc_info=True)
        # Fallback: minimal fixed-date set
        return (on.month, on.day) in {(1, 1), (7, 4), (12, 25)}

    # ------------- DB utilities -------------

    def _get_port(self, code: str) -> Port:
        return self.db.execute(select(Port).where(Port.code == code)).scalar_one()

    def _active_fee(self, code: str, on: date, port: Optional[Port] = None) -> Optional[Fee]:
        """
        Pull the most recent effective Fee row (<= date), respecting optional scoping:
        - applies_port_code
        - applies_state
        - applies_cascadia
        """
        rows = (
            self.db.execute(
                select(Fee)
                .where(Fee.code == code, Fee.effective_start <= on)
                .order_by(Fee.effective_start.desc())
            )
            .scalars()
            .all()
        )
        for f in rows:
            if f.effective_end and f.effective_end < on:
                continue
            if f.applies_port_code and port and f.applies_port_code != port.code:
                continue
            if f.applies_state and port and f.applies_state != (port.state or ""):
                continue
            if f.applies_cascadia is not None and port and bool(f.applies_cascadia) != bool(port.is_cascadia):
                continue
            return f
        return None

    # ------------- DB-backed config helpers -------------

    def _get_vessel_type_config(
        self, vessel_or_type: Union[VesselSpecs, str]
    ) -> Optional[VesselTypeConfig]:
        """Look up vessel-type configuration for a VesselSpecs or type-code."""

        if isinstance(vessel_or_type, VesselSpecs):
            code = vessel_or_type.vessel_type.value
        else:
            code = str(vessel_or_type)
        if code in self._vessel_type_cache:
            return self._vessel_type_cache[code]

        cfg = (
            self.db.execute(
                select(VesselTypeConfig).where(VesselTypeConfig.type_code == code)
            )
            .scalars()
            .first()
        )

        self._vessel_type_cache[code] = cfg
        return cfg

    def _get_pilotage_rate_for_port(self, port: Port, on: date) -> Optional[PilotageRate]:
        """
        Return the most recent PilotageRate row for a given port and date, or None.
        Used by the pilotage fallback when JSON registry isn't available.
        """

        key = (port.code, on)
        if key in self._pilotage_rate_cache:
            return self._pilotage_rate_cache[key]

        row = (
            self.db.execute(
                select(PilotageRate)
                .where(
                    PilotageRate.port_code == port.code,
                    PilotageRate.effective_date <= on,
                )
                .order_by(PilotageRate.effective_date.desc())
            )
            .scalars()
            .first()
        )

        self._pilotage_rate_cache[key] = row
        return row

    def _get_contract_adjustments(
        self, profile: str, port: Port, on: date
    ) -> Dict[str, Tuple[Decimal, Optional[Decimal]]]:
        """Return mapping fee_code -> (multiplier, offset) for contract adjustments."""

        cache_key = (profile, port.code)
        if cache_key in self._contract_adj_cache:
            return self._contract_adj_cache[cache_key]

        rows = (
            self.db.execute(
                select(ContractAdjustment).where(
                    ContractAdjustment.profile == profile,
                    ContractAdjustment.effective_start <= on,
                    or_(
                        ContractAdjustment.effective_end.is_(None),
                        ContractAdjustment.effective_end >= on,
                    ),
                    or_(
                        ContractAdjustment.port_code.is_(None),
                        ContractAdjustment.port_code == port.code,
                    ),
                )
            )
            .scalars()
            .all()
        )

        mapping: Dict[str, Tuple[Decimal, Optional[Decimal]]] = {}
        for row in rows:
            multiplier = row.multiplier or Decimal("1")
            mapping[row.fee_code] = (multiplier, row.offset)

        self._contract_adj_cache[cache_key] = mapping
        return mapping

    # ------------- Back-compat: simple API -------------

    def compute(self, ctx: EstimateContext) -> List[LineItem]:
        """
        Original lightweight estimator, now with formula fallbacks.
        Still returns List[LineItem].
        """
        items: List[LineItem] = []
        port = self._get_port(ctx.port_code)
        arrival_type = self._infer_arrival_type(ctx.previous_port_code, ctx.arrival_type)

        # ---- 1) CBP User Fee (calendar-year cap) ----
        db_cbp = self._active_fee("CBP_COMMERCIAL_VESSEL_ARRIVAL_FEE", ctx.arrival_date, port)
        if db_cbp:
            base = _money(db_cbp.rate)
            cap = _money(db_cbp.cap_amount or 0)
            if (db_cbp.cap_period or "").lower() == "calendar_year" and cap > 0:
                remaining = max(Decimal("0.00"), cap - _money(ctx.ytd_cbp_paid))
                charge = _money(min(base, remaining))
            else:
                charge = base
            items.append(
                LineItem(
                    code=db_cbp.code,
                    name=db_cbp.name,
                    amount=charge,
                    details={"rate": str(base), "cap": str(cap), "cap_period": db_cbp.cap_period},
                )
            )
        else:
            base, cap = self._cbp_rate_and_cap_by_date(ctx.arrival_date)
            remaining = max(Decimal("0.00"), cap - _money(ctx.ytd_cbp_paid))
            charge = _money(min(base, remaining))
            items.append(
                LineItem(
                    code="CBP_USER_FEE",
                    name="CBP Commercial Vessel Arrival Fee",
                    amount=charge,
                    details={"rate": str(base), "cap": str(cap), "cap_period": "calendar_year"},
                )
            )

        # ---- 2) APHIS AQI ----
        db_aphis = self._active_fee("APHIS_COMMERCIAL_VESSEL", ctx.arrival_date, port)
        if db_aphis:
            items.append(
                LineItem(
                    code=db_aphis.code,
                    name=db_aphis.name,
                    amount=_money(db_aphis.rate),
                    details={"unit": db_aphis.unit},
                )
            )
        else:
            # Cascadia gets Cascadia rate; domestic coastwise gets domestic; else medium.
            if port.is_cascadia:
                risk = "cascadia"
            elif arrival_type == "COASTWISE":
                risk = "domestic"
            else:
                risk = "medium_risk"
            rate = self.APHIS_RISK_RATES.get(risk, self.APHIS_RISK_RATES["medium_risk"])
            items.append(
                LineItem(
                    code="APHIS_AQI",
                    name="APHIS Agricultural Quarantine Inspection",
                    amount=_money(rate),
                    details={"risk": risk},
                )
            )

        # ---- 3) CA MISP (if in CA) ----
        if port.is_california:
            db_misp = self._active_fee("CA_MISP_PER_VOYAGE", ctx.arrival_date, port)
            if db_misp:
                items.append(
                    LineItem(
                        code=db_misp.code,
                        name=db_misp.name,
                        amount=_money(db_misp.rate),
                        details={"unit": db_misp.unit},
                    )
                )
            else:
                items.append(
                    LineItem(
                        code="CA_MISP",
                        name="California Marine Invasive Species Program",
                        amount=_money(1000),
                        details={"note": "fallback fixed per voyage"},
                    )
                )

        # ---- 4) Tonnage Tax ----
        db_ton = self._active_fee("TONNAGE_TAX_PER_TON", ctx.arrival_date, port)
        if db_ton and ctx.net_tonnage:
            per_ton = _money(db_ton.rate)
            amt = _money(Decimal(ctx.net_tonnage) * per_ton)
            if db_ton.cap_amount and db_ton.cap_period and ctx.tonnage_year_paid is not None:
                cap = _money(db_ton.cap_amount)
                remaining = max(Decimal("0.00"), cap - _money(ctx.tonnage_year_paid))
                amt = _money(min(amt, remaining))
            items.append(
                LineItem(
                    code=db_ton.code,
                    name=db_ton.name,
                    amount=amt,
                    details={"rate_per_ton": str(per_ton), "net_tonnage": str(ctx.net_tonnage), "cap_period": db_ton.cap_period},
                )
            )
        elif ctx.net_tonnage:
            vt_cfg = self._get_vessel_type_config("general_cargo")
            if vt_cfg and vt_cfg.tonnage_rate is not None:
                per_ton = _money(vt_cfg.tonnage_rate)
            else:
                per_ton = self.TONNAGE_RATES[VesselType.GENERAL_CARGO]
            base = _money(Decimal(ctx.net_tonnage) * per_ton)
            remaining = max(
                Decimal("0.00"), Decimal("19100.00") - _money(ctx.tonnage_year_paid)
            )
            amt = _money(min(base, remaining))
            items.append(
                LineItem(
                    code="TONNAGE_TAX",
                    name="Tonnage Tax",
                    amount=amt,
                    details={"rate_per_ton": str(per_ton), "net_tonnage": str(ctx.net_tonnage), "cap_period": "tonnage_year"},
                )
            )

        # ---- 5) Marine Exchange / VTS ----
        db_mx = self._active_fee("MX_VTS_PER_CALL", ctx.arrival_date, port)
        if db_mx:
            items.append(
                LineItem(
                    code=db_mx.code,
                    name=db_mx.name,
                    amount=_money(db_mx.rate),
                    details={"unit": db_mx.unit},
                )
            )
        else:
            base = self.MX_FALLBACK.get(port.code, Decimal("250"))
            items.append(
                LineItem(
                    code="MARINE_EXCHANGE",
                    name="Marine Exchange/VTS Services",
                    amount=_money(base),
                    details={"note": "fallback fixed port fee"},
                )
            )

        return items

    @staticmethod
    def _infer_arrival_type(previous_port_code: Optional[str], fallback: Optional[str]) -> str:
        prev = (previous_port_code or "").strip().upper()
        if prev.startswith("US"):
            return "COASTWISE"
        if prev:
            return "FOREIGN"

        fallback_norm = (fallback or "").strip().upper()
        if fallback_norm in {"COASTWISE", "FOREIGN"}:
            return fallback_norm
        if fallback_norm == "DOMESTIC":
            return "COASTWISE"
        return "FOREIGN"

    # ------------- Comprehensive API (full breakdown) -------------

    def calculate_comprehensive(self, vessel: VesselSpecs, voyage: VoyageContext) -> Dict[str, Any]:
        """Full enhanced breakdown with DB overrides + formula fallbacks."""
        port = self._get_port(voyage.arrival_port_code)
        calcs: List[FeeCalculation] = []

        # 1) CBP
        calcs.append(self._calc_cbp(voyage, port))

        # 2) APHIS
        calcs.append(self._calc_aphis(vessel, voyage, port))

        # 3) Tonnage Tax
        calcs.append(self._calc_tonnage_tax(vessel, voyage, port))

        # 4) CA MISP
        if port.is_california:
            calcs.append(self._calc_ca_misp(voyage, port))

        # 5) Pilotage (weekend/holiday multipliers, state-aware holiday detection)
        calcs.append(self._calc_pilotage(vessel, voyage, port))

        # 6) Dockage / Berth Hire
        calcs.append(self._calc_dockage(vessel, voyage, port))

        # 7) Tugboats (algorithmic estimate)
        calcs.append(self._calc_tugboats(vessel, voyage))

        # 8) Marine Exchange / VTS
        calcs.append(self._calc_mx(voyage, port))

        # 9) Optional services (water, garbage, launch, lines, etc.)
        optional_calcs = self._optional_services(
            voyage, include_legacy=self.show_legacy_optional
        )
        calcs.extend(optional_calcs)

        # Apply contract adjustments before computing totals
        if self.contract_profile:
            adj_map = self._get_contract_adjustments(
                self.contract_profile, port, voyage.eta.date()
            )
            if adj_map:
                for calc in calcs:
                    adj = adj_map.get(calc.code)
                    if not adj:
                        continue
                    mult, offset = adj
                    if mult is not None and mult != Decimal("1"):
                        existing = calc.multipliers.get("contract", Decimal("1"))
                        calc.multipliers["contract"] = _money(existing * mult)
                        calc.final_amount = _money(calc.final_amount * mult)
                    if offset:
                        calc.final_amount = _money(calc.final_amount + offset)
                    if (mult is not None and mult != Decimal("1")) or offset:
                        tag = []
                        if mult is not None and mult != Decimal("1"):
                            tag.append(f"x{mult}")
                        if offset:
                            tag.append(f"+{offset}")
                        detail = "[contract " + " ".join(tag) + "]"
                        calc.calculation_details = (calc.calculation_details or "").rstrip()
                        calc.calculation_details = (calc.calculation_details + " " + detail).strip()

        # Totals (recalculate from the filtered lists to ensure deprecated options stay excluded)
        mandatory_calcs = [c for c in calcs if not c.is_optional]
        optional_calcs = [c for c in calcs if c.is_optional]
        optional_priced = [c for c in optional_calcs if not c.manual_entry]
        mandatory_total = _money(sum(c.final_amount for c in mandatory_calcs))
        opt_low = _money(
            sum(
                (
                    c.estimated_range[0]
                    if c.estimated_range
                    else c.final_amount
                )
                for c in optional_priced
            )
        )
        opt_high = _money(
            sum(
                (
                    c.estimated_range[1]
                    if c.estimated_range
                    else c.final_amount
                )
                for c in optional_priced
            )
        )

        confidences = [c.confidence for c in calcs if not c.is_optional]
        overall_conf = (sum(confidences) / Decimal(len(confidences))) if confidences else Decimal("0.85")

        return {
            "vessel": {
                "name": vessel.name,
                "type": vessel.vessel_type.value,
                "gross_tonnage": str(_money(vessel.gross_tonnage)),
                "net_tonnage": str(_money(vessel.net_tonnage)),
                "loa_meters": str(_money(vessel.loa_meters)),
                "beam_meters": str(_money(vessel.beam_meters)),
                "draft_meters": str(_money(vessel.draft_meters)),
            },
            "voyage": {
                "previous_port": voyage.previous_port_code,
                "arrival_port": voyage.arrival_port_code,
                "next_port": voyage.next_port_code,
                "arrival_type": voyage.arrival_type,
                "eta": voyage.eta.isoformat(),
                "etd": voyage.etd.isoformat() if voyage.etd else None,
                "is_weekend": voyage.is_weekend_arrival,
                # Robust US holiday detection (federal + state where available)
                "is_holiday": self._is_us_holiday(
                    voyage.eta.date(),
                    getattr(port, "state", None),
                ),
            },
            "calculations": [
                {
                    "code": c.code,
                    "name": c.name,
                    "base_amount": str(_money(c.base_amount)),
                    "multipliers": {k: str(_money(v)) for k, v in c.multipliers.items()},
                    "final_amount": str(_money(c.final_amount)),
                    "confidence": str(c.confidence),
                    "details": c.calculation_details,
                    "is_optional": c.is_optional,
                    "manual_entry": c.manual_entry,
                    "estimated_range": (
                        [str(_money(c.estimated_range[0])), str(_money(c.estimated_range[1]))]
                        if c.estimated_range else None
                    ),
                }
                for c in calcs
            ],
            "totals": {
                "mandatory": str(mandatory_total),
                "optional_low": str(opt_low),
                "optional_high": str(opt_high),
                "total_low": str(_money(mandatory_total + opt_low)),
                "total_high": str(_money(mandatory_total + opt_high)),
            },
            "confidence": str(overall_conf),
            "accuracy_statement": f"Estimate accuracy: ±{((Decimal('1') - overall_conf) * Decimal('100')):.1f}%",
            "disclaimer": "Estimate based on standard rates/tariffs. Actual fees may vary due to negotiations, special circumstances, or regulatory changes.",
        }

    # ----- Pieces for comprehensive path (with DB overrides where applicable) -----

    def _cbp_rate_and_cap_by_date(self, on: date) -> Tuple[Decimal, Decimal]:
        # FY26 (>= Oct 1, 2025)
        if on >= date(2025, 10, 1):
            return _money("587.03"), _money("7999.40")
        # FY25
        return _money("571.81"), _money("7792.05")

    def _calc_cbp(self, voyage: VoyageContext, port: Port) -> FeeCalculation:
        on = voyage.eta.date()

        # DB override first
        db = self._active_fee("CBP_COMMERCIAL_VESSEL_ARRIVAL_FEE", on, port)
        if db:
            base = _money(db.rate)
            if (db.cap_period or "").lower() == "calendar_year" and db.cap_amount:
                cap = _money(db.cap_amount)
                remaining = max(Decimal("0"), cap - _money(self.ytd_cbp_paid))
                final_amt = _money(min(base, remaining))
            else:
                cap = _money(db.cap_amount or 0)
                final_amt = base
            return FeeCalculation(
                code=db.code,
                name=db.name,
                base_amount=base,
                final_amount=final_amt,
                confidence=Decimal("1"),
                calculation_details=f"DB rate ${base}, cap ${cap}, YTD ${_money(self.ytd_cbp_paid)}",
            )

        # Fallback to schedule
        base, cap = self._cbp_rate_and_cap_by_date(on)
        remaining = max(Decimal("0"), cap - _money(self.ytd_cbp_paid))
        final_amt = _money(min(base, remaining))
        return FeeCalculation(
            code="CBP_USER_FEE",
            name="CBP Commercial Vessel Arrival Fee",
            base_amount=base,
            final_amount=final_amt,
            confidence=Decimal("1"),
            calculation_details=f"Schedule rate ${base}, cap ${cap}, YTD ${_money(self.ytd_cbp_paid)}",
        )

    def _calc_aphis(self, vessel: VesselSpecs, voyage: VoyageContext, port: Port) -> FeeCalculation:
        """
        APHIS AQI fee:
          1) Prefer DB-configured fee rows (handles Cascadia/Great Lakes overrides via applies_cascadia).
          2) Fallback to risk buckets:
             - COASTWISE (prev UN/LOCODE starts with 'US') => 'domestic'
             - Arrival port flagged Cascadia => 'cascadia'
             - High-risk origin countries => 'high_risk'
             - Otherwise => 'medium_risk'
        """
        on = voyage.eta.date()

        # ---- DB override first (covers Cascadia/Great Lakes via applies_cascadia) ----
        db = self._active_fee("APHIS_COMMERCIAL_VESSEL", on, port)
        if db:
            base = _money(db.rate)
            details_bits = ["DB configured APHIS rate"]
            if db.applies_cascadia is not None:
                details_bits.append(f"applies_cascadia={bool(db.applies_cascadia)}")
            if db.applies_state:
                details_bits.append(f"state={db.applies_state}")
            if db.applies_port_code:
                details_bits.append(f"port={db.applies_port_code}")
            details = "; ".join(details_bits)

            return FeeCalculation(
                code=db.code,
                name=db.name,
                base_amount=base,
                final_amount=base,
                confidence=Decimal("0.95"),
                calculation_details=details,
            )

        # ---- Fallback risk logic ----
        prev = (voyage.previous_port_code or "").strip().upper()
        prev_cc = prev[:2] if len(prev) >= 2 else ""

        # 1) Coastwise moves (prev US*) are domestic
        if voyage.arrival_type == "COASTWISE" or prev_cc == "US":
            risk = "domestic"
        # 2) Cascadia/Great Lakes discounted region
        elif bool(getattr(port, "is_cascadia", False)):
            risk = "cascadia"
        # 3) High-risk origins
        elif prev_cc in self.HIGH_RISK_COUNTRIES:
            risk = "high_risk"
        # 4) Default medium
        else:
            risk = "medium_risk"

        # Attempt to use live APHIS AQI vessel fees from USDA.
        dyn_base: Optional[Decimal] = None
        try:
            from ..connectors.live_sources import fetch_aphis_vessel_fees

            fees = fetch_aphis_vessel_fees()
            std = fees.get("standard_fee")
            cas = fees.get("cascadia_fee")

            is_cascadia = bool(getattr(port, "is_cascadia", False))

            if is_cascadia and isinstance(cas, Decimal):
                dyn_base = _money(cas)
            elif isinstance(std, Decimal):
                dyn_base = _money(std)
        except Exception:
            logger.debug("APHIS live vessel fee lookup failed; using static fallback", exc_info=True)

        if dyn_base is None:
            base = self.APHIS_RISK_RATES.get(risk, self.APHIS_RISK_RATES["medium_risk"])
            details = (
                f"Fallback risk='{risk}' from prev='{prev}' "
                f"(port.is_cascadia={bool(getattr(port, 'is_cascadia', False))})"
            )
        else:
            base = dyn_base
            details = (
                f"Derived from APHIS AQI commercial vessel fee schedule "
                f"(risk='{risk}', prev='{prev}', is_cascadia={bool(getattr(port, 'is_cascadia', False))})"
            )

        return FeeCalculation(
            code="APHIS_AQI",
            name="APHIS Agricultural Quarantine Inspection",
            base_amount=_money(base),
            final_amount=_money(base),
            confidence=Decimal("0.95"),
            calculation_details=details,
        )

    def _calc_tonnage_tax(self, vessel: VesselSpecs, voyage: VoyageContext, port: Port) -> FeeCalculation:
        on = voyage.eta.date()
        db = self._active_fee("TONNAGE_TAX_PER_TON", on, port)
        if db:
            rate = _money(db.rate)
            base = _money(vessel.net_tonnage * rate)
            final_amt = base
            if db.cap_amount and db.cap_period:
                # Assume tonnage-year cap semantics for this code
                cap = _money(db.cap_amount)
                remaining = max(Decimal("0"), cap - _money(self.tonnage_year_paid))
                final_amt = _money(min(base, remaining))
                details = f"Net {vessel.net_tonnage} × ${rate}/NT, cap ${cap}, TY paid ${_money(self.tonnage_year_paid)}"
            else:
                details = f"Net {vessel.net_tonnage} × ${rate}/NT (no cap)"
            return FeeCalculation(
                code=db.code,
                name=db.name,
                base_amount=base,
                final_amount=final_amt,
                confidence=Decimal("0.98"),
                calculation_details=details,
            )

        # Fallback: apply the statutory lower regular tonnage tax schedule
        # (2¢/NT per entry, 10¢/NT per year) to derive a per-vessel annual cap.
        # See 46 U.S.C. 60301 and 19 CFR 4.20 for the underlying structure.
        if vessel.net_tonnage <= 0:
            base = Decimal("0")
            cap = Decimal("0")
            final_amt = Decimal("0")
            details = "Net tonnage missing; unable to compute statutory tonnage tax, treating as zero."
        else:
            rate = LOWER_RATE_PER_TON
            base = _money(vessel.net_tonnage * rate)
            cap_total = _money(vessel.net_tonnage * LOWER_CAP_PER_TON_PER_YEAR)
            remaining = max(Decimal("0"), cap_total - _money(self.tonnage_year_paid))
            final_amt = _money(min(base, remaining))
            details = (
                f"Net {vessel.net_tonnage} × ${rate}/NT, annual cap ${cap_total} "
                f"(2¢/NT per entry, 10¢/NT per year), TY paid ${_money(self.tonnage_year_paid)}"
            )

        return FeeCalculation(
            code="TONNAGE_TAX",
            name="Tonnage Tax",
            base_amount=base,
            final_amount=final_amt,
            confidence=Decimal("0.98"),
            calculation_details=details,
        )

    def _calc_ca_misp(self, voyage: VoyageContext, port: Port) -> FeeCalculation:
        on = voyage.eta.date()
        db = self._active_fee("CA_MISP_PER_VOYAGE", on, port)
        if db:
            base = _money(db.rate)
            return FeeCalculation(
                code=db.code,
                name=db.name,
                base_amount=base,
                final_amount=base,
                confidence=Decimal("1"),
                calculation_details="DB configured MISP per voyage",
            )

        # Fallback: try to infer current fee from CA public sources via live snapshot
        amount = Decimal("1000")
        details = "Fallback fixed per voyage"
        try:
            # Lazy import to avoid hard dependency at module import time
            from ..connectors.live_sources import fetch_misp_snapshot

            snap = fetch_misp_snapshot()
            candidates = cast(List[str], snap.get("possible_amounts_seen") or [])
            parsed: Optional[Decimal] = None

            for s in candidates:
                cleaned = "".join(ch for ch in str(s) if (ch.isdigit() or ch == "."))
                if cleaned:
                    parsed = Decimal(cleaned)
                    break

            if parsed is None:
                cf = snap.get("current_fee")
                if cf:
                    cleaned = "".join(ch for ch in str(cf) if (ch.isdigit() or ch == "."))
                    if cleaned:
                        parsed = Decimal(cleaned)

            if parsed is not None:
                amount = parsed
                details = "Derived from CA MISP public sources"
        except Exception:
            logger.debug("MISP live snapshot lookup failed; using static fallback", exc_info=True)

        base = _money(amount)
        return FeeCalculation(
            code="CA_MISP",
            name="California Marine Invasive Species Program",
            base_amount=base,
            final_amount=base,
            confidence=Decimal("1"),
            calculation_details=details,
        )

    def _calc_dockage(self, vessel: VesselSpecs, voyage: VoyageContext, port: Port) -> FeeCalculation:
        """
        Approximate dockage / berth hire for the alongside period.
        Uses a port/zone-specific table from DockageEngine.
        """
        days = voyage.days_alongside or 1
        res = DockageEngine.calculate(port.code, vessel.loa_meters, days)

        base = res["base_daily_rate"]
        total = res["total_amount"]
        loa_display = f"{float(vessel.loa_meters or Decimal('0')):.1f}"
        return FeeCalculation(
            code="DOCKAGE",
            name="Dockage / Berth Hire",
            base_amount=base,
            final_amount=total,
            confidence=Decimal("0.90"),
            calculation_details=(
                f"{res['tariff_ref']}: LOA {loa_display}m @ ${base}/day × {res['billable_periods']} days"
            ),
            is_optional=False,
        )

    @staticmethod
    def _resolve_port_zone(port: Port) -> str:
        zone = None
        if hasattr(port, "zone") and getattr(port, "zone") is not None:
            zone = getattr(port.zone, "code", None)
        if not zone:
            zone = getattr(port, "zone_code", None)
        if not zone:
            zone = getattr(port, "region", None)
        if not zone:
            zone = port.code
        return str(zone)

    def _default_legs_for_zone(self, zone: str) -> List[MovementLeg]:
        zone_up = (zone or "").upper()
        if zone_up in {"NORCAL", "SFBAY"}:
            names = ["bar", "bay", "river"]
        elif zone_up == "PUGET":
            names = ["harbor", "inter_harbor"]
        elif zone_up in {"COLUMBIA", "OREGON"}:
            names = ["bar", "river"]
        else:
            names = ["bar", "bay", "river"]
        return [MovementLeg(sequence=i + 1, leg_type=name) for i, name in enumerate(names)]

    @staticmethod
    def _classify_leg(zone: str, leg: MovementLeg) -> Tuple[Optional[str], str]:
        z = (zone or "").upper()
        key = leg.normalised_type()
        mapping: Dict[str, str]
        if z in {"NORCAL", "SFBAY"}:
            mapping = {
                "bar": "bar",
                "bar_crossing": "bar",
                "bar_transit": "bar",
                "sea_buoy": "bar",
                "golden_gate": "bar",
                "bay": "bay",
                "bay_transit": "bay",
                "harbor": "bay",
                "river": "river",
                "river_transit": "river",
                "delta": "river",
            }
        elif z == "PUGET":
            mapping = {
                "harbor": "bar",
                "harbor_move": "bar",
                "harbor_shift": "bar",
                "inter_harbor": "bay",
                "interharbor": "bay",
                "inter_harbor_transfer": "bay",
                "canal": "river",
                "river": "river",
                "river_transit": "river",
            }
        elif z in {"COLUMBIA", "OREGON"}:
            mapping = {
                "bar": "bar",
                "bar_crossing": "bar",
                "bar_transit": "bar",
                "river": "river",
                "river_transit": "river",
                "willamette": "river",
                "columbia": "river",
            }
        else:
            mapping = {
                "bar": "bar",
                "bay": "bay",
                "river": "river",
            }
        component = mapping.get(key)
        if component is None:
            if "bar" in key:
                component = "bar"
            elif "bay" in key or "harbor" in key:
                component = "bay"
            elif "river" in key or "delta" in key or "canal" in key:
                component = "river"
        return component, key

    def _pilotage_component_amounts(
        self, vessel: VesselSpecs, registry: Dict[str, Any]
    ) -> Dict[str, Decimal]:
        loa_feet = vessel.loa_feet
        bar = registry["bar"]
        bay = registry["bay"]
        river = registry["river"]

        bar_component = (bar["base_fee"] + (loa_feet * bar["per_foot_rate"])) * bar["draft_multiplier"]
        bay_component = max(bay["minimum"], loa_feet * bay["per_foot_rate"])
        river_component = max(river["minimum"], loa_feet * river["per_foot_rate"])

        return {
            "bar": _money(bar_component),
            "bay": _money(bay_component),
            "river": _money(river_component),
        }

    def _build_pilotage_breakdown(
        self,
        zone: str,
        registry: Dict[str, Any],
        vessel: VesselSpecs,
        voyage: VoyageContext,
        port: Port,
        legs: Iterable[MovementLeg],
    ) -> Dict[str, Any]:
        supplied_legs = list(legs)
        if not supplied_legs:
            supplied_legs = self._default_legs_for_zone(zone)

        zone_up = (zone or "").upper()

        # --- Special case: San Francisco Bar Pilots (NORCAL) using mill rates ---
        extras = dict(registry.get("extras", {}))
        if zone_up == "NORCAL" and "mill_rate_per_grt" in extras:

            grt = vessel.gross_tonnage
            draft_ft = vessel.draft_feet

            # Mill rates per GRT
            mill_rate = extras.get("mill_rate_per_grt", Decimal("0.09243"))
            pension = extras.get("pension_mill_rate", Decimal("0.04468"))
            boat = extras.get("pilot_boat_surcharge", Decimal("0.00200"))
            total_mill = mill_rate + pension + boat

            tonnage_charge = _money(grt * total_mill)

            # Draft-based component
            draft_rate = registry["bar"]["per_foot_rate"]
            draft_charge = _money(draft_ft * draft_rate)

            bar_subtotal = _money(tonnage_charge + draft_charge)

            # Board ops percent
            board_ops_pct = extras.get("board_ops_percent", Decimal("0.065"))
            board_ops = _money(bar_subtotal * board_ops_pct)

            # Flat continuing education + trainee fees
            flats = _money(
                extras.get("continuing_ed_flat", Decimal("45"))
                + extras.get("trainee_flat", Decimal("20"))
            )

            total = _money(max(bar_subtotal + board_ops + flats, Decimal("3000")))

            legs_out: List[Dict[str, Any]] = [
                {
                    "sequence": 1,
                    "leg_type": "Bar Pilotage",
                    "classification": "bar",
                    "base_charge": str(bar_subtotal),
                    "surcharges": [],
                    "extras": [],
                    "total": str(bar_subtotal),
                    "metadata": {},
                },
                {
                    "sequence": 2,
                    "leg_type": "Board Ops Surcharge",
                    "classification": "surcharge",
                    "base_charge": str(board_ops),
                    "surcharges": [],
                    "extras": [],
                    "total": str(board_ops),
                    "metadata": {},
                },
                {
                    "sequence": 3,
                    "leg_type": "Flat Surcharges",
                    "classification": "flat",
                    "base_charge": str(flats),
                    "surcharges": [],
                    "extras": [],
                    "total": str(flats),
                    "metadata": {},
                },
            ]

            return {
                "port_zone": zone,
                "effective_date": registry["effective"].isoformat(),
                "legs": legs_out,
                "job_total": str(total),
                "audit": {
                    "loa_feet": str(_money(vessel.loa_feet)),
                    "draft_feet": str(_money(draft_ft)),
                    "mill_rate_applied": str(_money(total_mill)),
                    "applied_multiplier": str(_money(Decimal("1"))),
                    "applied_multiplier_code": None,
                    "extras_applied": [
                        "board_ops_percent",
                        "continuing_ed_flat",
                        "trainee_flat",
                    ],
                },
            }

        components = self._pilotage_component_amounts(vessel, registry)

        surcharges = registry["surcharges"]
        weekend_mult = surcharges["weekend_multiplier"] if voyage.is_weekend_arrival else Decimal("1")
        holiday = self._is_us_holiday(voyage.eta.date(), getattr(port, "state", None))
        holiday_mult = surcharges["holiday_multiplier"] if holiday else Decimal("1")

        applied_code = None
        applied_multiplier = Decimal("1")
        if weekend_mult > applied_multiplier:
            applied_multiplier = weekend_mult
            applied_code = "weekend"
        if holiday_mult > applied_multiplier:
            applied_multiplier = holiday_mult
            applied_code = "holiday"

        night_charge = Decimal("0")
        if surcharges.get("night_flat") and (voyage.eta.hour < 6 or voyage.eta.hour >= 18):
            night_charge = _money(surcharges["night_flat"])

        extras_registry: Dict[str, Decimal] = dict(registry.get("extras", {}))

        legs_out: List[Dict[str, Any]] = []
        total = Decimal("0")
        extras_applied: List[str] = []
        first_component_assigned = False

        for leg in sorted(supplied_legs, key=lambda l: l.sequence):
            component, normalised = self._classify_leg(zone, leg)
            base_charge = components.get(component or "", Decimal("0"))
            leg_total = base_charge

            sur_list: List[Dict[str, Any]] = []
            if base_charge > 0 and applied_multiplier > 1:
                surcharge_amt = _money(base_charge * (applied_multiplier - Decimal("1")))
                leg_total += surcharge_amt
                sur_list.append(
                    {
                        "code": applied_code,
                        "multiplier": str(_money(applied_multiplier)),
                        "amount": str(surcharge_amt),
                    }
                )

            extras_list: List[Dict[str, Any]] = []
            if base_charge > 0 and not first_component_assigned and extras_registry:
                for code, amount in list(extras_registry.items()):
                    extras_list.append(
                        {
                            "code": str(code),
                            "amount": str(_money(amount)),
                        }
                    )
                    leg_total += _money(amount)
                    extras_applied.append(str(code))
                    extras_registry.pop(code, None)
                first_component_assigned = True

            if base_charge > 0 and night_charge > 0:
                extras_list.append(
                    {
                        "code": "night",
                        "amount": str(night_charge),
                    }
                )
                leg_total += night_charge
                night_charge = Decimal("0")

            leg_entry = {
                "sequence": leg.sequence,
                "leg_type": leg.leg_type,
                "classification": component,
                "base_charge": str(_money(base_charge)),
                "surcharges": sur_list,
                "extras": extras_list,
                "total": str(_money(leg_total)),
                "metadata": leg.to_metadata(),
            }
            legs_out.append(leg_entry)
            total += _money(leg_total)

        return {
            "port_zone": zone,
            "effective_date": registry["effective"].isoformat(),
            "legs": legs_out,
            "job_total": str(_money(total)),
            "audit": {
                "loa_feet": str(_money(vessel.loa_feet)),
                "draft_feet": str(_money(vessel.draft_feet)),
                "applied_multiplier": str(_money(applied_multiplier)),
                "applied_multiplier_code": applied_code,
                "extras_applied": extras_applied,
            },
        }

    def calculate_pilotage_breakdown(
        self,
        vessel: VesselSpecs,
        voyage: VoyageContext,
        legs: Optional[Iterable[MovementLeg]] = None,
        *,
        port: Optional[Port] = None,
    ) -> Dict[str, Any]:
        port_obj = port or self._get_port(voyage.arrival_port_code)
        zone = self._resolve_port_zone(port_obj)
        on = voyage.eta.date()

        try:
            registry = load_pilotage_rates(zone, on)
        except (MISSING_RATE_FIELD, KeyError, ValueError) as exc:
            logger.warning(
                "pilotage registry lookup failed for zone %s: %s; falling back",
                zone,
                exc,
            )
            fallback = self._calc_pilotage_fallback(vessel, voyage, port_obj)
            return {
                "port_zone": zone,
                "effective_date": None,
                "legs": [
                    {
                        "sequence": 1,
                        "leg_type": "fallback",
                        "classification": None,
                        "base_charge": str(_money(fallback.base_amount)),
                        "surcharges": [],
                        "extras": [],
                        "total": str(_money(fallback.final_amount)),
                        "metadata": {"details": fallback.calculation_details},
                    }
                ],
                "job_total": str(_money(fallback.final_amount)),
                "audit": {
                    "fallback": True,
                    "reason": str(exc),
                    "confidence": str(fallback.confidence),
                },
            }

        return self._build_pilotage_breakdown(zone, registry, vessel, voyage, port_obj, legs or [])

    def _calc_pilotage_fallback(
        self, vessel: VesselSpecs, voyage: VoyageContext, port: Port
    ) -> FeeCalculation:
        on = voyage.eta.date()

        # Prefer DB-configured pilotage_rates row for this port, if available
        pr = self._get_pilotage_rate_for_port(port, on)
        if pr:
            base = _money(pr.base_rate or 0)
            per_foot = Decimal(str(pr.per_foot_rate or 0))
            draft_mult = Decimal(str(pr.draft_multiplier or 1))
            loa_charge = _money(vessel.loa_feet * per_foot)
            base_amt = _money(base + (loa_charge * draft_mult))
            min_charge = _money(pr.minimum_charge) if pr.minimum_charge is not None else None
            max_charge = _money(pr.maximum_charge) if pr.maximum_charge is not None else None
        else:
            # Fall back to legacy in-code approximations
            rates = self._LEGACY_PILOTAGE_PORT_RATES.get(
                voyage.arrival_port_code,
                {"base": 3500, "per_foot": 8.00, "draft_mult": 1.15},
            )
            base = _money(rates["base"])
            loa_charge = _money(vessel.loa_feet * Decimal(str(rates["per_foot"])))
            draft_mult = Decimal(str(rates["draft_mult"]))
            base_amt = _money(base + (loa_charge * draft_mult))
            min_charge = _money(5000)
            max_charge = _money(30000)

        is_holiday = self._is_us_holiday(voyage.eta.date(), getattr(port, "state", None))

        multipliers: Dict[str, Decimal] = {}
        if voyage.is_weekend_arrival:
            multipliers["weekend"] = Decimal("1.5")
        if is_holiday:
            multipliers["holiday"] = Decimal("2.0")

        final_mult = max(multipliers.values()) if multipliers else Decimal("1.0")
        final_amt = _money(base_amt * final_mult)

        # Apply min/max from DB where available (or legacy defaults if not)
        if min_charge is not None:
            final_amt = max(final_amt, min_charge)
        if max_charge is not None:
            final_amt = min(final_amt, max_charge)

        return FeeCalculation(
            code="PILOTAGE",
            name="Harbor Pilotage",
            base_amount=base_amt,
            multipliers=multipliers,
            final_amount=final_amt,
            confidence=Decimal("0.75"),
            calculation_details=(
                "Registry fallback – using DB / legacy pilotage rates "
                f"for {port.code} at LOA {vessel.loa_feet:.0f}ft"
            ),
            is_optional=False,
        )

    def _calc_pilotage(
        self,
        vessel: VesselSpecs,
        voyage: VoyageContext,
        port: Port,
        legs: Optional[Iterable[MovementLeg]] = None,
    ) -> FeeCalculation:
        breakdown = self.calculate_pilotage_breakdown(vessel, voyage, legs, port=port)

        total = _money(Decimal(str(breakdown["job_total"])))
        base_total = _money(
            sum(Decimal(str(entry["base_charge"])) for entry in breakdown["legs"])
        )

        multipliers: Dict[str, Decimal] = {}
        audit = breakdown.get("audit", {})
        code = audit.get("applied_multiplier_code")
        try:
            mult_value = Decimal(str(audit.get("applied_multiplier", "1")))
        except Exception:
            mult_value = Decimal("1")
        if code and mult_value > 1:
            multipliers[code] = _money(mult_value)

        details = []
        for leg in breakdown["legs"]:
            details.append(
                f"Leg {leg['sequence']} {leg.get('classification') or leg['leg_type']}: ${leg['total']}"
            )

        calc_details = "; ".join(details)

        return FeeCalculation(
            code="PILOTAGE",
            name="Harbor Pilotage",
            base_amount=base_total,
            multipliers=multipliers,
            final_amount=total,
            confidence=Decimal("0.95"),
            calculation_details=calc_details,
            is_optional=False,
        )

    def _calc_tugboats(self, vessel: VesselSpecs, voyage: VoyageContext) -> FeeCalculation:
        """
        Algorithmic tugboat cost estimate.

        Assumptions:
          - 2 moves (arrival + departure)
          - ~2.5h per tug job including transit
          - Hourly rate and tug count scale with GRT
          - Fuel surcharge applied as a percentage uplift
        """
        try:
            gt = float(vessel.gross_tonnage)
        except Exception:
            gt = 0.0

        if gt <= 0:
            # No meaningful GRT: keep as a soft placeholder
            return FeeCalculation(
                code="TUGBOAT",
                name="Tugboat Services (Estimate)",
                base_amount=Decimal("0"),
                final_amount=Decimal("0"),
                confidence=Decimal("0.40"),
                calculation_details="Insufficient GRT data – manual tug quote required.",
                is_optional=True,
                manual_entry=True,
            )

        # Try to use vessel_types config for tug count if available
        vt_cfg = self._get_vessel_type_config(vessel)
        base_tugs: Optional[float] = None
        if vt_cfg and vt_cfg.typical_tug_count is not None:
            try:
                base_tugs = float(vt_cfg.typical_tug_count)
            except Exception:
                base_tugs = None

        if base_tugs is not None and base_tugs > 0:
            tugs_per_move = base_tugs
        else:
            # Fallback to GT-based heuristic
            if gt < 20000:
                tugs_per_move = 1.5
            elif gt < 60000:
                tugs_per_move = 2.0
            else:
                tugs_per_move = 2.5

        # Hourly rate remains GT-based; we can refine by type later if desired.
        if gt < 20000:
            hourly_rate = 1800.0
        elif gt < 60000:
            hourly_rate = 2400.0
        else:
            hourly_rate = 3200.0

        moves = 2
        hours_per_tug_job = 2.5
        total_tug_hours = moves * tugs_per_move * hours_per_tug_job

        fuel_surcharge_pct = 0.18  # ~18% FSC common in 2024/25

        base_cost = Decimal(str(total_tug_hours * hourly_rate))
        total_cost = base_cost * Decimal(str(1 + fuel_surcharge_pct))

        return FeeCalculation(
            code="TOWAGE",
            name="Tugboat Services (Estimate)",
            base_amount=_money(hourly_rate),
            final_amount=_money(total_cost),
            confidence=Decimal("0.75"),
            calculation_details=(
                f"Est. {int(tugs_per_move)} tugs in/out for {vessel.vessel_type.value} "
                f"@ ${int(hourly_rate)}/hr/tug "
                f"for {total_tug_hours:.1f} tug-hours + {int(fuel_surcharge_pct*100)}% FSC."
            ),
            is_optional=False,
            manual_entry=False,
        )

    def _calc_mx(self, voyage: VoyageContext, port: Port) -> FeeCalculation:
        on = voyage.eta.date()
        db = self._active_fee("MX_VTS_PER_CALL", on, port)
        if db:
            base = _money(db.rate)
            return FeeCalculation(
                code=db.code,
                name=db.name,
                base_amount=base,
                final_amount=base,
                confidence=Decimal("0.95"),
                calculation_details="DB configured MX/VTS fee",
            )
        base = self.MX_FALLBACK.get(port.code, Decimal("250"))
        base = _money(base)
        details = "Fallback fixed port fee"
        try:
            # Enrich with live MX provider/URL so users can verify against the tariff.
            from ..connectors.live_sources import choose_region, mx_snapshot_for_region

            region = choose_region(
                getattr(port, "code", None),
                getattr(port, "name", None),
                getattr(port, "state", None),
                getattr(port, "is_cascadia", None),
            )
            mx = mx_snapshot_for_region(region) or {}
            primary = mx.get("primary") or {}
            provider = primary.get("provider") or ""
            url = primary.get("url") or ""
            details = (
                f"Approximate MX/VTS fee (no DB override). "
                f"Region={region}, provider={provider or 'unknown'}, "
                f"tariff_url={url or 'n/a'}"
            )
        except Exception:
            logger.debug("MX live snapshot lookup failed; using static fallback", exc_info=True)

        return FeeCalculation(
            code="MARINE_EXCHANGE",
            name="Marine Exchange/VTS Services",
            base_amount=base,
            final_amount=base,
            confidence=Decimal("0.95"),
            calculation_details=details,
        )

    def _optional_services(
        self, voyage: VoyageContext, *, include_legacy: bool = False
    ) -> List[FeeCalculation]:
        out: List[FeeCalculation] = []

        out.append(
            FeeCalculation(
                code="LINE_HANDLING",
                name="Line Handling (Mooring/Unmooring)",
                base_amount=_money(1500),
                final_amount=_money(1500),
                confidence=Decimal("0.80"),
                calculation_details="Dockworkers for mooring",
                is_optional=True,
                estimated_range=(Decimal("1000.00"), Decimal("2500.00")),
            )
        )
        if include_legacy:
            out.append(
                FeeCalculation(
                    code="LAUNCH_SERVICE",
                    name="Launch/Water Taxi Service",
                    base_amount=_money(800),
                    final_amount=_money(800),
                    confidence=Decimal("0.75"),
                    calculation_details="Crew transportation",
                    is_optional=True,
                    estimated_range=(Decimal("500.00"), Decimal("1500.00")),
                )
            )
            out.append(
                FeeCalculation(
                    code="GARBAGE",
                    name="Garbage Disposal",
                    base_amount=_money(600),
                    final_amount=_money(600),
                    confidence=Decimal("0.90"),
                    calculation_details="Waste removal",
                    is_optional=True,
                    estimated_range=(Decimal("400.00"), Decimal("1000.00")),
                )
            )
            if voyage.days_alongside > 1:
                amt = _money(200) * Decimal(str(voyage.days_alongside))
                out.append(
                    FeeCalculation(
                        code="FRESH_WATER",
                        name="Fresh Water Supply",
                        base_amount=amt,
                        final_amount=amt,
                        confidence=Decimal("0.85"),
                        calculation_details=f"$200/day × {voyage.days_alongside} days",
                        is_optional=True,
                        estimated_range=(
                            _money(amt * Decimal("0.8")),
                            _money(amt * Decimal("1.2")),
                        ),
                    )
                )
        return out
