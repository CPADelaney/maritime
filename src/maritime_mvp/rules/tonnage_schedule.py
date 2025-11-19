"""Statutory tonnage tax schedule helpers.

This module encodes the *lower* regular tonnage tax rate from
46 U.S.C. § 60301 and 19 CFR 4.20:

  - 2 cents per net ton at each entry, and
  - a cap of 10 cents per net ton in any 1 year.

The FeeEngine fallback uses these values to derive a per-vessel
annual cap in dollars: net_tonnage × 0.10.

This is intentionally conservative and applies the lower rate for
all vessels when no DB-configured tonnage fee is present. If you
need the higher rate or more nuanced geography-based logic, you
should configure explicit Fee rows in the database instead.
"""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP

# 2 cents per net ton per entry
LOWER_RATE_PER_TON: Decimal = Decimal("0.02")

# 10 cents per net ton per fiscal year (cap)
LOWER_CAP_PER_TON_PER_YEAR: Decimal = Decimal("0.10")


def _money(value: Decimal | int | float | str) -> Decimal:
    """Quantize a numeric value to the nearest cent using standard half-up rounding."""

    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def lower_entry_fee(net_tonnage: Decimal | int | float) -> Decimal:
    """Compute the lower statutory per-entry fee for a vessel's net tonnage."""

    net = Decimal(str(net_tonnage))
    if net <= 0:
        return Decimal("0.00")
    return _money(net * LOWER_RATE_PER_TON)


def lower_annual_cap(net_tonnage: Decimal | int | float) -> Decimal:
    """Compute the per-vessel annual cap under the lower statutory schedule."""

    net = Decimal(str(net_tonnage))
    if net <= 0:
        return Decimal("0.00")
    return _money(net * LOWER_CAP_PER_TON_PER_YEAR)

