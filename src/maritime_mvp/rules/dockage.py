"""Dockage calculation rules based on West Coast port tariffs (POLA/POLB/Oakland/NWSA)."""
from __future__ import annotations

import math
from decimal import Decimal
from typing import Dict, List, Tuple


def _money(val: Decimal | float | int | str) -> Decimal:
    if not isinstance(val, Decimal):
        val = Decimal(str(val))
    return val.quantize(Decimal("0.01"))


class DockageEngine:
    """
    Approximate length-based dockage engine.

    These tables are *approximations* of the published tariffs, tuned to be
    within a reasonable accuracy band for an MVP estimator. They are expressed
    as daily dockage charges by LOA.
    """

    # LA/LB Tariff 4 Tier Approximation (per 24h day, USD)
    POLA_RATES: List[Tuple[float, float]] = [
        (100.0, 500.0),
        (150.0, 1500.0),
        (200.0, 3500.0),
        (250.0, 7500.0),
        (300.0, 14000.0),
        (350.0, 21000.0),
        (400.0, 35000.0),
    ]

    # Oakland Tariff 2-A (slightly softer than LA/LB)
    OAK_RATES: List[Tuple[float, float]] = [
        (100.0, 450.0),
        (150.0, 1200.0),
        (200.0, 3000.0),
        (250.0, 6800.0),
        (300.0, 12500.0),
        (350.0, 19000.0),
        (400.0, 31000.0),
    ]

    # NWSA (Seattle/Tacoma) Tariff 300
    NWSA_RATES: List[Tuple[float, float]] = [
        (100.0, 600.0),
        (150.0, 1800.0),
        (200.0, 4200.0),
        (250.0, 9000.0),
        (300.0, 15500.0),
        (350.0, 23000.0),
        (400.0, 38000.0),
    ]

    @classmethod
    def calculate(cls, port_code: str, loa_meters: Decimal, days: float) -> Dict[str, object]:
        """
        Estimate dockage/berth hire for a given port, LOA and days alongside.

        Returns a dict suitable for feeding into FeeCalculation.
        """
        port = (port_code or "").upper()

        if port in {"LALB", "USLAX", "USLGB"}:
            table = cls.POLA_RATES
            tariff_name = "Port of LA/LB Tariff No. 4 (approx.)"
        elif port in {"OAK", "USOAK", "SFBAY"}:
            table = cls.OAK_RATES
            tariff_name = "Port of Oakland Tariff 2-A (approx.)"
        elif port in {"USSEA", "USTAC", "PUGET", "NWSA"}:
            table = cls.NWSA_RATES
            tariff_name = "NWSA Tariff No. 300 (approx.)"
        else:
            table = cls.POLA_RATES
            tariff_name = "Generic West Coast Dockage (approx.)"

        loa_val = float(loa_meters or Decimal("0"))
        daily_rate = cls._interpolate_rate(loa_val, table)

        billable_periods = max(1, math.ceil(float(days) if days is not None else 1.0))
        total = Decimal(daily_rate) * Decimal(billable_periods)

        return {
            "base_daily_rate": _money(daily_rate),
            "billable_periods": billable_periods,
            "period_unit": "Day (24h)",
            "total_amount": _money(total),
            "tariff_ref": tariff_name,
        }

    @staticmethod
    def _interpolate_rate(loa: float, table: List[Tuple[float, float]]) -> float:
        """Simple linear interpolation between LOA tiers, with linear extrapolation beyond the last tier."""
        if loa <= table[0][0]:
            return table[0][1]
        if loa >= table[-1][0]:
            last_len, last_rate = table[-1]
            prev_len, prev_rate = table[-2]
            slope = (last_rate - prev_rate) / (last_len - prev_len)
            return last_rate + slope * (loa - last_len)

        for (l1, r1), (l2, r2) in zip(table, table[1:]):
            if l1 <= loa <= l2:
                fraction = (loa - l1) / (l2 - l1)
                return r1 + fraction * (r2 - r1)
        return table[-1][1]
