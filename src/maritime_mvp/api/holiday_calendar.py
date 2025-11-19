"""Static labor holiday calendar keyed by internal port zones.

This is an advisory list of *fixed-date* labor holidays that are known to
affect West Coast port costs under the ILWU/PMA contracts and common
terminal tariffs.

Floating holidays (Memorial Day, Labor Day, Thanksgiving, etc.) are not
modeled here; those are handled by the main holiday logic via the
`holidays` package for surcharge calculations.
"""
from __future__ import annotations

from datetime import date
from typing import List, Dict

# Canonical fixed-date ILWU/port holidays that materially impact port costs.
# Sources: ILWU/PMA contracts and published holiday calendars. 
_BASE_LABOR_HOLIDAYS: List[Dict[str, object]] = [
    {
        "name": "New Year's Day",
        "month": 1,
        "day": 1,
        "note": "ILWU/PMA paid holiday; most terminals closed or at premium rates.",
    },
    {
        "name": "Cesar Chavez Day",
        "month": 3,
        "day": 31,
        "note": "ILWU paid holiday; California ports often run reduced gangs or overtime.",
    },
    {
        "name": "Juneteenth",
        "month": 6,
        "day": 19,
        "note": "Recognized ILWU/PMA holiday; many terminals operate at holiday rates.",
    },
    {
        "name": "Independence Day",
        "month": 7,
        "day": 4,
        "note": "US federal holiday; longshore work typically at premium or shut down.",
    },
    {
        "name": "Bloody Thursday",
        "month": 7,
        "day": 5,
        "note": "ILWU no-work holiday; West Coast ports routinely shut down for 24 hours.",
    },
    {
        "name": "Harry Bridges' Birthday",
        "month": 7,
        "day": 28,
        "note": "ILWU paid holiday; work usually at overtime rates where performed.",
    },
    {
        "name": "Veterans Day",
        "month": 11,
        "day": 11,
        "note": "ILWU paid holiday; many terminals treat as overtime/limited operations.",
    },
    {
        "name": "Christmas Eve",
        "month": 12,
        "day": 24,
        "note": "Work restrictions and shortened shifts; evening work typically at premium.",
    },
    {
        "name": "Christmas Day",
        "month": 12,
        "day": 25,
        "note": "ILWU no-work holiday; terminals effectively closed except emergencies.",
    },
    {
        "name": "New Year's Eve",
        "month": 12,
        "day": 31,
        "note": "Work restrictions from afternoon onward; premium rates for night work.",
    },
]

# All West Coast ILWU ports share essentially the same holiday structure; we
# key by your internal zones for convenience.
_HOLIDAY_TEMPLATES: Dict[str, List[Dict[str, object]]] = {
    "SOCAL": list(_BASE_LABOR_HOLIDAYS),
    "NORCAL": list(_BASE_LABOR_HOLIDAYS),
    "PUGET": list(_BASE_LABOR_HOLIDAYS),
    "COLUMBIA": list(_BASE_LABOR_HOLIDAYS),
    "INLAND": list(_BASE_LABOR_HOLIDAYS),
}

_DEFAULT_CODES = {"SOCAL", "NORCAL", "PUGET", "COLUMBIA", "INLAND"}


def get_upcoming_holidays(zone_code: str, *, limit: int = 4) -> List[Dict[str, str]]:
    """Return upcoming fixed-date labor holidays for the given zone.

    This is intentionally conservative and only includes known ILWU/port
    holidays that are fixed in the calendar. Floating holidays (Memorial Day,
    Labor Day, Thanksgiving, etc.) are handled elsewhere in the cost engine.
    """
    if not zone_code:
        return []

    today = date.today()
    templates = _HOLIDAY_TEMPLATES.get(zone_code.upper())
    if templates is None and zone_code.upper() not in _DEFAULT_CODES:
        # Fallback: treat unknown zones like SoCal for advisory purposes
        templates = _HOLIDAY_TEMPLATES.get("SOCAL", [])
    elif templates is None:
        templates = []

    entries: List[Dict[str, str]] = []
    for template in templates:
        for year in (today.year, today.year + 1):
            try:
                observed = date(
                    year,
                    int(template["month"]),
                    int(template["day"]),
                )
            except (ValueError, TypeError):
                continue
            if observed < today:
                continue
            entries.append(
                {
                    "name": str(template["name"]),
                    "date": observed.isoformat(),
                    "note": str(template.get("note", "")),
                }
            )
    entries.sort(key=lambda item: item["date"])
    return entries[:limit]
