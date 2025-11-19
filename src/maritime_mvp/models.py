from __future__ import annotations
from typing import Optional
import datetime
from decimal import Decimal

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import String, Boolean, Numeric, Date, Integer, Text, ForeignKey
from sqlalchemy.dialects.postgresql import ARRAY

class Base(DeclarativeBase):
    pass

class PortZone(Base):
    __tablename__ = "port_zones"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(12), unique=True)
    name: Mapped[str] = mapped_column(String(120))
    region: Mapped[Optional[str]] = mapped_column(String(48))
    primary_state: Mapped[Optional[str]] = mapped_column(String(2))
    country: Mapped[str] = mapped_column(String(2), default="US")
    description: Mapped[Optional[str]] = mapped_column(Text)

    ports: Mapped[list["Port"]] = relationship(back_populates="zone", cascade="all, delete-orphan")


class Port(Base):
    __tablename__ = "ports"

    id: Mapped[int] = mapped_column(primary_key=True)
    zone_id: Mapped[Optional[int]] = mapped_column(ForeignKey("port_zones.id", onupdate="CASCADE", ondelete="SET NULL"))
    code: Mapped[str] = mapped_column(String(12), unique=True)
    name: Mapped[str] = mapped_column(String(120))
    state: Mapped[Optional[str]] = mapped_column(String(2))
    country: Mapped[str] = mapped_column(String(2), default="US")
    region: Mapped[Optional[str]] = mapped_column(String(24))
    is_california: Mapped[bool] = mapped_column(Boolean, default=False)
    is_cascadia: Mapped[bool] = mapped_column(Boolean, default=False)
    is_eca: Mapped[bool] = mapped_column(Boolean, default=True)
    latitude: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 6))
    longitude: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 6))
    pilotage_url: Mapped[Optional[str]] = mapped_column(String(512))
    mx_url: Mapped[Optional[str]] = mapped_column(String(512))
    tariff_url: Mapped[Optional[str]] = mapped_column(String(512))

    zone: Mapped[Optional[PortZone]] = relationship(back_populates="ports")
    terminals: Mapped[list["Terminal"]] = relationship(back_populates="port", cascade="all, delete-orphan")


class Terminal(Base):
    __tablename__ = "terminals"

    id: Mapped[int] = mapped_column(primary_key=True)
    port_id: Mapped[int] = mapped_column(ForeignKey("ports.id", ondelete="CASCADE"))
    code: Mapped[str] = mapped_column(String(24), unique=True)
    name: Mapped[str] = mapped_column(String(200))
    operator_name: Mapped[Optional[str]] = mapped_column(String(200))
    is_public: Mapped[bool] = mapped_column(Boolean, default=False)
    notes: Mapped[Optional[str]] = mapped_column(Text)

    port: Mapped[Port] = relationship(back_populates="terminals")

class Fee(Base):
    __tablename__ = "fees"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(64))  # not unique (time/region versions allowed)
    name: Mapped[str] = mapped_column(String(200))
    scope: Mapped[str] = mapped_column(String(24))           # federal/state/port
    unit: Mapped[str] = mapped_column(String(24))            # per_call/per_net_ton/...
    rate: Mapped[Decimal] = mapped_column(Numeric(12, 4))
    currency: Mapped[str] = mapped_column(String(3), default="USD")
    cap_amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4))
    cap_period: Mapped[Optional[str]] = mapped_column(String(24))
    applies_state: Mapped[Optional[str]] = mapped_column(String(2))
    applies_port_code: Mapped[Optional[str]] = mapped_column(String(12))
    applies_cascadia: Mapped[Optional[bool]] = mapped_column(Boolean)
    effective_start: Mapped[datetime.date] = mapped_column(Date)
    effective_end: Mapped[Optional[datetime.date]] = mapped_column(Date)
    source_url: Mapped[Optional[str]] = mapped_column(String(512))
    authority: Mapped[Optional[str]] = mapped_column(String(512))

class Source(Base):
    __tablename__ = "sources"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(200))
    url: Mapped[str] = mapped_column(String(512))
    type: Mapped[str] = mapped_column(String(24))  # pilotage/tariff/law/program/api
    effective_date: Mapped[Optional[datetime.date]] = mapped_column(Date)


class PortDocument(Base):
    __tablename__ = "port_documents"

    id: Mapped[int] = mapped_column(primary_key=True)
    port_code: Mapped[str] = mapped_column(String(12))
    document_name: Mapped[str] = mapped_column(String(200))
    document_code: Mapped[Optional[str]] = mapped_column(String(64))
    is_mandatory: Mapped[bool] = mapped_column(Boolean, default=True)
    lead_time_hours: Mapped[int] = mapped_column(Integer, default=0)
    authority: Mapped[Optional[str]] = mapped_column(String(200))
    description: Mapped[Optional[str]] = mapped_column(Text)
    applies_to_vessel_types: Mapped[Optional[list[str]]] = mapped_column(ARRAY(String))
    applies_if_foreign: Mapped[bool] = mapped_column(Boolean, default=False)


class ContractAdjustment(Base):
    """Per-profile fee adjustment layer."""

    __tablename__ = "contract_adjustments"

    id: Mapped[int] = mapped_column(primary_key=True)

    profile: Mapped[str] = mapped_column(String(64))
    fee_code: Mapped[str] = mapped_column(String(64))
    port_code: Mapped[Optional[str]] = mapped_column(String(12))

    multiplier: Mapped[Decimal] = mapped_column(Numeric(8, 4), default=Decimal("1.0"))
    offset: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2))

    effective_start: Mapped[datetime.date] = mapped_column(Date)
    effective_end: Mapped[Optional[datetime.date]] = mapped_column(Date)

    notes: Mapped[Optional[str]] = mapped_column(Text)


class VesselTypeConfig(Base):
    """
    DB-backed configuration for vessel-type specific tuning:
      - tonnage_rate: optional per-net-ton heuristic (used in legacy/simple paths)
      - pilotage_multiplier: factor to adjust pilotage cost for this type
      - typical_tug_count: typical tugs per move for this type
    """

    __tablename__ = "vessel_types"

    id: Mapped[int] = mapped_column(primary_key=True)
    type_code: Mapped[str] = mapped_column(String, unique=True)
    type_name: Mapped[str] = mapped_column(String)
    tonnage_rate: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4))
    pilotage_multiplier: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), default=Decimal("1.0"))
    typical_tug_count: Mapped[Optional[int]] = mapped_column(Integer, default=2)


class PilotageRate(Base):
    """
    DB pilotage configuration per port and effective date.
    Used as a structured fallback when the JSON registry is unavailable.
    """

    __tablename__ = "pilotage_rates"

    id: Mapped[int] = mapped_column(primary_key=True)
    port_code: Mapped[str] = mapped_column(String(12))
    effective_date: Mapped[datetime.date] = mapped_column(Date)

    base_rate: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2))
    per_foot_rate: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2))
    draft_multiplier: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4))

    overtime_multiplier: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), default=Decimal("1.5"))
    holiday_multiplier: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), default=Decimal("2.0"))

    minimum_charge: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2))
    maximum_charge: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2))

    formula: Mapped[Optional[str]] = mapped_column(Text)
    notes: Mapped[Optional[str]] = mapped_column(Text)
