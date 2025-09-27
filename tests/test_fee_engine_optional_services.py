from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from fastapi.testclient import TestClient

from maritime_mvp.rules.fee_engine import (
    FeeCalculation,
    FeeEngine,
    VesselSpecs,
    VoyageContext,
)

os.environ.setdefault("DATABASE_URL", "sqlite://")


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


def test_estimate_endpoint_excludes_legacy_launch_service(monkeypatch):
    from maritime_mvp.api import main as api_main

    class DummySession:
        def execute(self, stmt):  # noqa: D401 - simple stub
            class Result:
                def scalar_one_or_none(self_inner):
                    return SimpleNamespace(code="LALB", name="Port of Los Angeles")

            return Result()

        def close(self):
            pass

    monkeypatch.setattr(api_main, "SessionLocal", lambda: DummySession())

    class StubFeeEngine:
        _infer_arrival_type = staticmethod(lambda previous, declared: declared or "FOREIGN")

        def __init__(self, db):  # noqa: D401 - simple stub
            pass

        def compute(self, ctx):
            return [
                SimpleNamespace(
                    code="CBP",
                    name="CBP User Fee",
                    amount=Decimal("100.00"),
                    details="stub",
                )
            ]

    monkeypatch.setattr(api_main, "FeeEngine", StubFeeEngine)

    client = TestClient(api_main.app)
    response = client.get(
        "/estimate",
        params={
            "port_code": "LALB",
            "eta": "2024-05-01",
            "arrival_type": "FOREIGN",
            "include_optional": "true",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    services = payload.get("optional_services", [])
    assert services, "expected optional services list to be populated"
    assert all("Launch" not in svc["service"] for svc in services)
    tug = next((svc for svc in services if svc["service"] == "Tugboat Assist"), None)
    assert tug is not None, "expected tug service entry"
    assert tug.get("manual_entry") is True
    assert "estimated_low" not in tug and "estimated_high" not in tug
    assert payload["total_with_optional_low"] == "6100.00"
    assert payload["total_with_optional_high"] == "17600.00"
