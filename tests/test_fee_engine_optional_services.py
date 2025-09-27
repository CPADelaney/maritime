from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from maritime_mvp.rules.fee_engine import (
    FeeCalculation,
    FeeEngine,
    VesselSpecs,
    VoyageContext,
)


def _make_fee(code: str, amount: str, *, optional: bool = False) -> FeeCalculation:
    value = Decimal(amount)
    return FeeCalculation(
        code=code,
        name=code,
        base_amount=value,
        final_amount=value,
        confidence=Decimal("1"),
        calculation_details="stub",
        is_optional=optional,
    )


def test_comprehensive_excludes_legacy_optional_services_by_default():
    engine = FeeEngine(MagicMock())
    engine._get_port = MagicMock(
        return_value=SimpleNamespace(
            code="LALB", is_california=False, is_cascadia=False, state="CA"
        )
    )
    engine._calc_cbp = MagicMock(return_value=_make_fee("CBP", "500"))
    engine._calc_aphis = MagicMock(return_value=_make_fee("APHIS", "750"))
    engine._calc_tonnage_tax = MagicMock(return_value=_make_fee("TONNAGE", "900"))
    engine._calc_pilotage = MagicMock(return_value=_make_fee("PILOTAGE", "1200"))
    engine._calc_mx = MagicMock(return_value=_make_fee("MARINE_EXCHANGE", "300"))
    engine._estimate_tugboats = MagicMock(
        return_value=FeeCalculation(
            code="TUGBOAT",
            name="Tugboat Assist Services",
            base_amount=Decimal("10000"),
            final_amount=Decimal("10000"),
            confidence=Decimal("0.70"),
            calculation_details="stub",
            is_optional=True,
            estimated_range=(Decimal("8000"), Decimal("13000")),
        )
    )

    vessel = VesselSpecs(name="MV Test")
    voyage = VoyageContext(
        previous_port_code="CNSHA",
        arrival_port_code="LALB",
        next_port_code=None,
        days_alongside=3,
    )

    result = engine.calculate_comprehensive(vessel, voyage)

    optional_codes = {
        calc["code"]
        for calc in result["calculations"]
        if calc["is_optional"]
    }

    assert optional_codes == {"LINE_HANDLING", "TUGBOAT"}
    assert result["totals"]["optional_low"] == str(Decimal("9000.00"))
    assert result["totals"]["optional_high"] == str(Decimal("15500.00"))
