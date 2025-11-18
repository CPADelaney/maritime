"""Static labor holiday calendar keyed by internal port zones."""
from __future__ import annotations

from datetime import date
from typing import List, Dict

_HOLIDAY_TEMPLATES: Dict[str, List[Dict[str, object]]] = {
    "SOCAL": [
        {"name": "Cesar Chavez Day", "month": 3, "day": 31, "note": "Overtime likely across ILWU locals."},
        {"name": "Bloody Thursday", "month": 7, "day": 5, "note": "ILWU work stoppage and premium tug/line rates."},
    ],
    "NORCAL": [
        {"name": "Cesar Chavez Day", "month": 3, "day": 31, "note": "State holiday — most gangs on overtime."},
        {"name": "King Tide Safety Stand-down", "month": 1, "day": 11, "note": "Harbor craft overtime for daylight windows."},
    ],
    "PUGET": [
        {"name": "Juneteenth", "month": 6, "day": 19, "note": "Contract holiday for ILWU/PMA locals."},
        {"name": "Bloody Thursday", "month": 7, "day": 5, "note": "Expect stop work meetings & tug surcharges."},
    ],
    "COLUMBIA": [
        {"name": "Cesar Chavez Day", "month": 3, "day": 31, "note": "Grain elevators on skeleton crews."},
        {"name": "Harvest Kickoff", "month": 8, "day": 15, "note": "Pilot and tug standby premiums common."},
    ],
    "INLAND": [
        {"name": "Cesar Chavez Day", "month": 3, "day": 31, "note": "Overtime for line handlers likely."},
        {"name": "Labor Day", "month": 9, "day": 1, "note": "1.5x tug and longshore rates."},
    ],
}

_DEFAULT_CODES = {"SOCAL", "NORCAL", "PUGET", "COLUMBIA", "INLAND"}


def get_upcoming_holidays(zone_code: str, *, limit: int = 4) -> List[Dict[str, str]]:
    """Return upcoming holiday entries for the given zone, limited to the next two seasons."""
    if not zone_code:
        return []

    today = date.today()
    templates = _HOLIDAY_TEMPLATES.get(zone_code.upper())
    if templates is None and zone_code.upper() not in _DEFAULT_CODES:
        templates = _HOLIDAY_TEMPLATES.get("SOCAL", [])
    elif templates is None:
        templates = []

    entries: List[Dict[str, str]] = []
    for template in templates:
        for year in (today.year, today.year + 1):
            try:
                observed = date(year, int(template["month"]), int(template["day"]))
            except ValueError:
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
