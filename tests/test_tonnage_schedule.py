from decimal import Decimal

import pytest

from maritime_mvp.rules.tonnage_schedule import lower_annual_cap, lower_entry_fee


@pytest.mark.parametrize(
    "net_tonnage, expected",
    [
        (Decimal("0"), Decimal("0.00")),
        (-1, Decimal("0.00")),
        (Decimal("1000"), Decimal("20.00")),
        (1250.5, Decimal("25.01")),
    ],
)
def test_lower_entry_fee(net_tonnage: Decimal, expected: Decimal) -> None:
    assert lower_entry_fee(net_tonnage) == expected


@pytest.mark.parametrize(
    "net_tonnage, expected",
    [
        (Decimal("0"), Decimal("0.00")),
        (-1, Decimal("0.00")),
        (Decimal("1000"), Decimal("100.00")),
        (1250.5, Decimal("125.05")),
    ],
)
def test_lower_annual_cap(net_tonnage: Decimal, expected: Decimal) -> None:
    assert lower_annual_cap(net_tonnage) == expected
