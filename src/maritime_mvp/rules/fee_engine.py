# src/maritime_mvp/rules/fee_engine.py
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Optional, List, Dict, Tuple, Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import Fee, Port


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


# ---------- Back-compat context (existing callers use this) ----------
@dataclass
class EstimateContext:
    port_code: str
    arrival_date: date
    arrival_type: str  # "FOREIGN" | "COASTWISE"
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
    loa_meters: Decimal = Decimal("0")  # Length Overall in meters
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
    arrival_port_code: str   # Like "USOAK"
    next_port_code: Optional[str] = None

    eta: datetime = field(default_factory=datetime.now)
    etd: Optional[datetime] = None
    days_alongside: int = 2

    @property
    def arrival_type(self) -> str:
        if self.previous_port_code and self.previous_port_code.startswith("US"):
            return "COASTWISE"
        return "FOREIGN"

    @property
    def is_weekend_arrival(self) -> bool:
        return self.eta.weekday() >= 5  # Sat=5, Sun=6

    @property
    def is_holiday(self) -> bool:
        # Simple, replace with python-holidays if needed
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

    # Pilotage formula fallbacks
    PILOTAGE_PORT_RATES = {
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


    def __init__(self, db: Session):
        self.db = db
        # Rolling caps for comprehensive API; the simple API takes caps from ctx
        self.ytd_cbp_paid = Decimal("0.00")
        self.tonnage_year_paid = Decimal("0.00")

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

    # ------------- Back-compat: simple API -------------

    def compute(self, ctx: EstimateContext) -> List[LineItem]:
        """
        Original lightweight estimator, now with formula fallbacks.
        Still returns List[LineItem].
        """
        items: List[LineItem] = []
        port = self._get_port(ctx.port_code)

        # ---- 1) CBP User Fee (calendar-year cap) ----
        # Prefer DB fee code; fallback to FY schedule
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
            # Fallback to formula (FY25/26 step)
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
            # Cheap heuristic without full voyage context:
            # Cascadia gets Cascadia rate; domestic coastwise gets domestic; else medium.
            if port.is_cascadia:
                risk = "cascadia"
            elif ctx.arrival_type == "COASTWISE":
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
            # If DB configured a cap+period, apply it (commonly tonnage-year)
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
            # Fallback to generic vessel-type rate (general cargo)
            per_ton = self.TONNAGE_RATES[VesselType.GENERAL_CARGO]
            base = _money(Decimal(ctx.net_tonnage) * per_ton)
            # Apply simplified federal cap ($19,100) against ctx-tonnage-year
            remaining = max(Decimal("0.00"), Decimal("19100.00") - _money(ctx.tonnage_year_paid))
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

    # ------------- Comprehensive API (full breakdown) -------------

    def calculate_comprehensive(self, vessel: VesselSpecs, voyage: VoyageContext) -> Dict[str, Any]:
        """Full enhanced breakdown with DB overrides + formula fallbacks."""
        port = self._get_port(voyage.arrival_port_code)
        on_date = voyage.eta.date()
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

        # 5) Pilotage
        calcs.append(self._calc_pilotage(vessel, voyage))

        # 6) Tugboats (optional estimate)
        calcs.append(self._estimate_tugboats(vessel, voyage))

        # 7) Marine Exchange / VTS
        calcs.append(self._calc_mx(voyage, port))

        # 8) Optional services (water, garbage, launch, lines, etc.)
        calcs.extend(self._optional_services(voyage))

        # Totals
        mandatory_total = _money(sum(c.final_amount for c in calcs if not c.is_optional))
        opt_low = _money(sum((c.estimated_range[0] if c.estimated_range else c.final_amount) for c in calcs if c.is_optional))
        opt_high = _money(sum((c.estimated_range[1] if c.estimated_range else c.final_amount) for c in calcs if c.is_optional))

        confidences = [c.confidence for c in calcs if not c.is_optional]
        overall_conf = (sum(confidences) / Decimal(len(confidences))) if confidences else Decimal("0.85")

        return {
            "vessel": {
                "name": vessel.name,
                "type": vessel.vessel_type.value,
                "gross_tonnage": str(_money(vessel.gross_tonnage)),
                "net_tonnage": str(_money(vessel.net_tonnage)),
                "loa_meters": str(_money(vessel.loa_meters)),
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
                "is_holiday": voyage.is_holiday,
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
            # Helpful context in details
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
    
        base = self.APHIS_RISK_RATES.get(risk, self.APHIS_RISK_RATES["medium_risk"])
    
        return FeeCalculation(
            code="APHIS_AQI",
            name="APHIS Agricultural Quarantine Inspection",
            base_amount=_money(base),
            final_amount=_money(base),
            confidence=Decimal("0.95"),
            calculation_details=f"Fallback risk='{risk}' from prev='{prev}' (port.is_cascadia={bool(getattr(port, 'is_cascadia', False))})",
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

        # Fallback: vessel-type rate and federal cap $19,100
        rate = self.TONNAGE_RATES.get(vessel.vessel_type, self.TONNAGE_RATES[VesselType.GENERAL_CARGO])
        base = _money(vessel.net_tonnage * rate)
        cap = _money("19100.00")
        remaining = max(Decimal("0"), cap - _money(self.tonnage_year_paid))
        final_amt = _money(min(base, remaining))
        return FeeCalculation(
            code="TONNAGE_TAX",
            name="Tonnage Tax",
            base_amount=base,
            final_amount=final_amt,
            confidence=Decimal("0.98"),
            calculation_details=f"Net {vessel.net_tonnage} × ${rate}/NT, cap ${cap}, TY paid ${_money(self.tonnage_year_paid)}",
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
        base = _money(1000)
        return FeeCalculation(
            code="CA_MISP",
            name="California Marine Invasive Species Program",
            base_amount=base,
            final_amount=base,
            confidence=Decimal("1"),
            calculation_details="Fallback fixed per voyage",
        )

    def _calc_pilotage(self, vessel: VesselSpecs, voyage: VoyageContext) -> FeeCalculation:
        rates = self.PILOTAGE_PORT_RATES.get(voyage.arrival_port_code, {"base": 3500, "per_foot": 8.00, "draft_mult": 1.15})
        base = _money(rates["base"])
        loa_charge = _money(vessel.loa_feet * Decimal(str(rates["per_foot"])))
        draft_mult = Decimal(str(rates["draft_mult"]))
        base_amt = _money(base + (loa_charge * draft_mult))

        multipliers: Dict[str, Decimal] = {}
        if voyage.is_weekend_arrival:
            multipliers["weekend"] = Decimal("1.5")
        if voyage.is_holiday:
            multipliers["holiday"] = Decimal("2.0")

        final_mult = max(multipliers.values()) if multipliers else Decimal("1.0")
        final_amt = _money(base_amt * final_mult)
        final_amt = max(final_amt, _money(5000))
        final_amt = min(final_amt, _money(30000))

        return FeeCalculation(
            code="PILOTAGE",
            name="Harbor Pilotage",
            base_amount=base_amt,
            multipliers=multipliers,
            final_amount=final_amt,
            confidence=Decimal("0.85"),
            calculation_details=f"LOA {vessel.loa_feet:.0f}ft × ${rates['per_foot']}/ft × draft factor {draft_mult}",
            is_optional=False,
        )

    def _estimate_tugboats(self, vessel: VesselSpecs, voyage: VoyageContext) -> FeeCalculation:
        tugs_by_type = {
            VesselType.CONTAINER: 2,
            VesselType.TANKER: 3,
            VesselType.LNG: 4,
            VesselType.CRUISE: 3,
            VesselType.BULK_CARRIER: 2,
        }
        num = tugs_by_type.get(vessel.vessel_type, 2)
        hr_rate = _money(2500)
        hours = Decimal("2")
        base = _money(num * hr_rate * hours)
        low = _money(base * Decimal("0.8"))
        high = _money(base * Decimal("1.3"))
        return FeeCalculation(
            code="TUGBOAT",
            name="Tugboat Assist Services",
            base_amount=base,
            final_amount=base,
            confidence=Decimal("0.70"),
            calculation_details=f"{num} tugs × {hours}h × ${hr_rate}/h",
            is_optional=True,
            estimated_range=(low, high),
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
        return FeeCalculation(
            code="MARINE_EXCHANGE",
            name="Marine Exchange/VTS Services",
            base_amount=base,
            final_amount=base,
            confidence=Decimal("0.95"),
            calculation_details="Fallback fixed port fee",
        )

    def _optional_services(self, voyage: VoyageContext) -> List[FeeCalculation]:
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
                    estimated_range=(_money(amt * Decimal("0.8")), _money(amt * Decimal("1.2"))),
                )
            )
        return out
