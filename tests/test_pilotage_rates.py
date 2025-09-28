from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from maritime_mvp.rules.fee_engine import (
    FeeEngine,
    MovementLeg,
    VesselSpecs,
    VoyageContext,
)
from maritime_mvp.rules.rates_loader import (
    MISSING_RATE_FIELD,
    load_pilotage_rates,
)


def test_load_pilotage_rates_picks_latest_effective_version(tmp_path):
    registry = {
        "SOCAL": [
            {
                "effective": "2023-01-01",
                "bar": {
                    "base_fee": 1000,
                    "per_foot_rate": 5,
                    "draft_multiplier": 1.1,
                    "min_total": 3000,
                    "max_total": 15000,
                },
                "bay": {"per_foot_rate": 2, "minimum": 500},
                "river": {"per_foot_rate": 0, "minimum": 0},
                "surcharges": {
                    "weekend_multiplier": 1.5,
                    "holiday_multiplier": 2.0,
                    "night_flat": 400,
                },
                "extras": {"transportation": 200},
            },
            {
                "effective": "2024-01-01",
                "bar": {
                    "base_fee": 1200,
                    "per_foot_rate": 6,
                    "draft_multiplier": 1.2,
                    "min_total": 3500,
                    "max_total": 18000,
                },
                "bay": {"per_foot_rate": 3, "minimum": 600},
                "river": {"per_foot_rate": 0, "minimum": 0},
                "surcharges": {
                    "weekend_multiplier": 1.6,
                    "holiday_multiplier": 2.1,
                    "night_flat": 450,
                },
                "extras": {"transportation": 250},
            },
        ]
    }
    path = tmp_path / "rates.json"
    path.write_text(__import__("json").dumps(registry))

    rates = load_pilotage_rates("socal", date(2024, 5, 1), registry_path=path)

    assert rates["effective"].isoformat() == "2024-01-01"
    assert rates["bar"]["base_fee"] == Decimal("1200")
    assert rates["extras"]["transportation"] == Decimal("250")


def test_load_pilotage_rates_missing_field(tmp_path):
    registry = {
        "SOCAL": [
            {
                "effective": "2024-01-01",
                "bar": {
                    "per_foot_rate": 6,
                    "draft_multiplier": 1.2,
                    "min_total": 3500,
                    "max_total": 18000,
                },
                "bay": {"per_foot_rate": 3, "minimum": 600},
                "river": {"per_foot_rate": 0, "minimum": 0},
                "surcharges": {
                    "weekend_multiplier": 1.6,
                    "holiday_multiplier": 2.1,
                    "night_flat": 450,
                },
                "extras": {"transportation": 250},
            }
        ]
    }
    path = tmp_path / "rates.json"
    path.write_text(__import__("json").dumps(registry))

    with pytest.raises(MISSING_RATE_FIELD):
        load_pilotage_rates("SOCAL", date(2024, 1, 2), registry_path=path)


def test_calc_pilotage_uses_registry(monkeypatch):
    engine = FeeEngine(MagicMock())
    port = SimpleNamespace(
        code="USOAK",
        zone=SimpleNamespace(code="NORCAL"),
        zone_code="NORCAL",
        state="CA",
    )
    engine._get_port = MagicMock(return_value=port)

    vessel = VesselSpecs(name="Test Vessel", loa_meters=Decimal("300"), draft_meters=Decimal("12"))
    voyage = VoyageContext(
        previous_port_code="CNSHA",
        arrival_port_code="USOAK",
        eta=datetime(2024, 7, 4, 21, 0, 0),
    )

    legs = [
        MovementLeg(sequence=1, leg_type="Bar Transit", from_location="Sea Buoy"),
        MovementLeg(sequence=2, leg_type="Bay Transit", from_location="Golden Gate"),
        MovementLeg(sequence=3, leg_type="River Transit", from_location="Carquinez Strait"),
    ]

    breakdown = engine.calculate_pilotage_breakdown(vessel, voyage, legs, port=port)

    assert breakdown["port_zone"] == "NORCAL"
    assert breakdown["effective_date"] == "2024-01-01"
    assert breakdown["job_total"] == "48032.58"
    classifications = [leg["classification"] for leg in breakdown["legs"]]
    assert classifications == ["bar", "bay", "river"]
    assert any(extra["code"] == "transportation" for extra in breakdown["legs"][0]["extras"])


def test_pilotage_breakdown_classifies_puget_sound(monkeypatch):
    engine = FeeEngine(MagicMock())
    port = SimpleNamespace(
        code="USSEA",
        zone=SimpleNamespace(code="PUGET"),
        zone_code="PUGET",
        state="WA",
    )
    engine._get_port = MagicMock(return_value=port)

    vessel = VesselSpecs(name="Puget Vessel", loa_meters=Decimal("300"), draft_meters=Decimal("12"))
    voyage = VoyageContext(
        previous_port_code="CNSHA",
        arrival_port_code="USSEA",
        eta=datetime(2024, 7, 4, 5, 0, 0),
    )

    legs = [
        MovementLeg(sequence=1, leg_type="Harbor Shift"),
        MovementLeg(sequence=2, leg_type="Inter-harbor Transfer"),
    ]

    breakdown = engine.calculate_pilotage_breakdown(vessel, voyage, legs, port=port)

    assert breakdown["port_zone"] == "PUGET"
    assert [leg["classification"] for leg in breakdown["legs"]] == ["bar", "bay"]
    assert breakdown["job_total"] == "34995.61"


def test_pilotage_breakdown_classifies_columbia_river(monkeypatch):
    engine = FeeEngine(MagicMock())
    port = SimpleNamespace(
        code="USPDX",
        zone=SimpleNamespace(code="COLUMBIA"),
        zone_code="COLUMBIA",
        state="OR",
    )
    engine._get_port = MagicMock(return_value=port)

    vessel = VesselSpecs(name="Columbia Vessel", loa_meters=Decimal("300"), draft_meters=Decimal("12"))
    voyage = VoyageContext(
        previous_port_code="CNSHA",
        arrival_port_code="USPDX",
        eta=datetime(2024, 7, 4, 19, 0, 0),
    )

    legs = [
        MovementLeg(sequence=1, leg_type="Bar Crossing"),
        MovementLeg(sequence=2, leg_type="River Transit"),
    ]

    breakdown = engine.calculate_pilotage_breakdown(vessel, voyage, legs, port=port)

    assert breakdown["port_zone"] == "COLUMBIA"
    assert [leg["classification"] for leg in breakdown["legs"]] == ["bar", "river"]
    assert breakdown["job_total"] == "30787.57"
