"""Pilotage rate registry loader."""
from __future__ import annotations

import json
import os
from datetime import date, datetime
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Mapping

__all__ = [
    "MISSING_RATE_FIELD",
    "MissingRateField",
    "load_pilotage_rates",
]


class MissingRateField(KeyError):
    """Raised when an expected field is missing from the rate registry."""

    def __init__(self, field_path: str):
        super().__init__(field_path)
        self.field_path = field_path

    def __str__(self) -> str:  # pragma: no cover - inherited KeyError repr is noisy
        return f"missing required rate field: {self.field_path}"


# Backwards-compatible alias for callers expecting a constant name.
MISSING_RATE_FIELD = MissingRateField

_DEFAULT_REGISTRY_PATH = Path(__file__).with_name("rates_registry.json")

_REQUIRED_VERSION_KEYS = {"effective", "bar", "bay", "river", "surcharges", "extras"}
_REQUIRED_BAR_KEYS = {"base_fee", "per_foot_rate", "draft_multiplier", "min_total", "max_total"}
_REQUIRED_BAY_KEYS = {"per_foot_rate", "minimum"}
_REQUIRED_RIVER_KEYS = {"per_foot_rate", "minimum"}
_REQUIRED_SURCHARGE_KEYS = {"weekend_multiplier", "holiday_multiplier", "night_flat"}


def _resolve_registry_path(path: str | os.PathLike[str] | None) -> Path:
    if path is not None:
        return Path(path)
    override = os.getenv("PILOTAGE_RATES_PATH")
    if override:
        return Path(override)
    return _DEFAULT_REGISTRY_PATH


@lru_cache(maxsize=None)
def _load_registry(path_str: str) -> Dict[str, Any]:
    path = Path(path_str)
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, Mapping):
        raise ValueError("rates registry must be a mapping of port zone to versions")
    return dict(data)


def _ensure_keys(zone: str, record: Mapping[str, Any]) -> None:
    for key in _REQUIRED_VERSION_KEYS:
        if key not in record:
            raise MISSING_RATE_FIELD(f"{zone}.{key}")

    for key in _REQUIRED_BAR_KEYS:
        if key not in record["bar"]:
            raise MISSING_RATE_FIELD(f"{zone}.bar.{key}")

    for key in _REQUIRED_BAY_KEYS:
        if key not in record["bay"]:
            raise MISSING_RATE_FIELD(f"{zone}.bay.{key}")

    for key in _REQUIRED_RIVER_KEYS:
        if key not in record["river"]:
            raise MISSING_RATE_FIELD(f"{zone}.river.{key}")

    for key in _REQUIRED_SURCHARGE_KEYS:
        if key not in record["surcharges"]:
            raise MISSING_RATE_FIELD(f"{zone}.surcharges.{key}")

    if not isinstance(record["extras"], Mapping):
        raise MISSING_RATE_FIELD(f"{zone}.extras")


def _to_decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def _normalise_version(record: Mapping[str, Any]) -> Dict[str, Any]:
    effective = record["effective"]
    if isinstance(effective, date):
        effective_date = effective
    else:
        effective_date = datetime.strptime(str(effective), "%Y-%m-%d").date()

    bar = {k: _to_decimal(record["bar"][k]) for k in _REQUIRED_BAR_KEYS}
    bay = {k: _to_decimal(record["bay"][k]) for k in _REQUIRED_BAY_KEYS}
    river = {k: _to_decimal(record["river"][k]) for k in _REQUIRED_RIVER_KEYS}
    surcharges = {k: _to_decimal(record["surcharges"][k]) for k in _REQUIRED_SURCHARGE_KEYS}
    extras = {str(k): _to_decimal(v) for k, v in record["extras"].items()}

    return {
        "effective": effective_date,
        "bar": bar,
        "bay": bay,
        "river": river,
        "surcharges": surcharges,
        "extras": extras,
    }


def load_pilotage_rates(
    port_zone: str,
    job_date: date,
    *,
    registry_path: str | os.PathLike[str] | None = None,
) -> Dict[str, Any]:
    """Return the most recent pilotage rate definition for ``port_zone``."""

    if not port_zone:
        raise ValueError("port_zone is required")

    zone = port_zone.upper()
    path = _resolve_registry_path(registry_path)
    registry = _load_registry(str(path))

    zone_versions = registry.get(zone)
    if not zone_versions:
        raise KeyError(f"no pilotage rates configured for zone {zone}")

    selected: Dict[str, Any] | None = None
    selected_date: date | None = None
    for entry in zone_versions:
        if not isinstance(entry, Mapping):
            raise ValueError(f"invalid registry entry for zone {zone}")
        _ensure_keys(zone, entry)
        version = _normalise_version(entry)
        effective = version["effective"]
        if effective <= job_date and (selected_date is None or effective > selected_date):
            selected = version
            selected_date = effective

    if selected is None:
        raise ValueError(
            f"no pilotage rates effective on {job_date.isoformat()} for zone {zone}"
        )

    return {
        "effective": selected["effective"],
        "bar": dict(selected["bar"]),
        "bay": dict(selected["bay"]),
        "river": dict(selected["river"]),
        "surcharges": dict(selected["surcharges"]),
        "extras": dict(selected["extras"]),
    }

