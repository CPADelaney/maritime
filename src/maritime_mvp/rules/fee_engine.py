from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from sqlalchemy import select
from sqlalchemy.orm import Session
from ..models import Fee, Port

@dataclass
class EstimateContext:
    port_code: str
    arrival_date: date
    arrival_type: str  # "FOREIGN" | "COASTWISE"
    net_tonnage: Decimal | None = None
    ytd_cbp_paid: Decimal = Decimal("0.00")  # calendar-year paid for user fee cap math
    tonnage_year_paid: Decimal = Decimal("0.00")  # running total for tonnage tax cap
    is_ballasted: bool = True

@dataclass
class LineItem:
    code: str
    name: str
    amount: Decimal
    details: dict

class FeeEngine:
    def __init__(self, db: Session):
        self.db = db

    def _active_fee(self, code: str, on: date, port: Port | None = None) -> Fee | None:
        q = select(Fee).where(Fee.code == code, Fee.effective_start <= on).order_by(Fee.effective_start.desc())
        fees = self.db.execute(q).scalars().all()
        for f in fees:
            if f.effective_end and f.effective_end < on:
                continue
            # match-by-port/state/cascadia if present
            if f.applies_port_code and port and f.applies_port_code != port.code:
                continue
            if f.applies_state and port and f.applies_state != (port.state or ""):
                continue
            if f.applies_cascadia is not None and port and bool(f.applies_cascadia) != bool(port.is_cascadia):
                continue
            return f
        return None

    def compute(self, ctx: EstimateContext) -> list[LineItem]:
        items: list[LineItem] = []
        port = self.db.execute(select(Port).where(Port.code == ctx.port_code)).scalar_one()

        # 1) CBP Commercial Vessel Arrival User Fee (calendar-year cap)
        uf = self._active_fee("CBP_COMMERCIAL_VESSEL_ARRIVAL_FEE", ctx.arrival_date, port)
        if uf:
            base = Decimal(uf.rate)
            cap = Decimal(uf.cap_amount or 0)
            if uf.cap_period == "calendar_year" and cap > 0:
                remaining = max(Decimal("0.00"), cap - ctx.ytd_cbp_paid)
                charge = min(base, remaining)
            else:
                charge = base
            items.append(LineItem(code=uf.code, name=uf.name, amount=charge, details={"rate": str(base), "cap": str(cap), "cap_period": uf.cap_period}))

        # 2) APHIS AQI Commercial Vessel Fee (Cascadia/Great Lakes reduced rate option)
        aphis_code = "APHIS_COMMERCIAL_VESSEL"
        aphis = self._active_fee(aphis_code, ctx.arrival_date, port)
        if aphis:
            items.append(LineItem(code=aphis.code, name=aphis.name, amount=Decimal(aphis.rate), details={"unit": aphis.unit}))

        # 3) CA MISP ballast-program fee (per qualifying voyage to CA from outside CA)
        if port.is_california:
            misp = self._active_fee("CA_MISP_PER_VOYAGE", ctx.arrival_date, port)
            if misp:
                items.append(LineItem(code=misp.code, name=misp.name, amount=Decimal(misp.rate), details={"unit": misp.unit}))

        # 4) Tonnage tax — leave as a stub driven by fee rows (regular/special/light money) selected by ops at estimate time
        #    You can wire a selector UI for 2c/6c or 9c/27c regimes; this engine simply looks for a port-scoped row if present.
        ton = self._active_fee("TONNAGE_TAX_PER_TON", ctx.arrival_date, port)
        if ton and ctx.net_tonnage:
            per_ton = Decimal(ton.rate)
            amount = (ctx.net_tonnage * per_ton).quantize(Decimal("0.01"))
            items.append(LineItem(code=ton.code, name=ton.name, amount=amount, details={"rate_per_ton": str(per_ton), "net_tonnage": str(ctx.net_tonnage), "cap_period": ton.cap_period}))

        # 5) Marine Exchange / VTS or local port fees (if you encode them as flat per-call lines in fees)
        mx = self._active_fee("MX_VTS_PER_CALL", ctx.arrival_date, port)
        if mx:
            items.append(LineItem(code=mx.code, name=mx.name, amount=Decimal(mx.rate), details={"unit": mx.unit}))

        return items
