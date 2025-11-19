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

from decimal import Decimal

# 2 cents per net ton per entry
LOWER_RATE_PER_TON: Decimal = Decimal("0.02")

# 10 cents per net ton per fiscal year (cap)
LOWER_CAP_PER_TON_PER_YEAR: Decimal = Decimal("0.10")

