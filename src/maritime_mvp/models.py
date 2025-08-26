from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Integer, BigInteger, Numeric, Date, DateTime, Boolean, Text, ForeignKey
from typing import Optional

class Base(DeclarativeBase):
    pass

class Port(Base):
    __tablename__ = "ports"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(12), unique=True)
    name: Mapped[str] = mapped_column(String(120))
    state: Mapped[Optional[str]] = mapped_column(String(2))
    country: Mapped[str] = mapped_column(String(2), default="US")
    region: Mapped[Optional[str]] = mapped_column(String(24))
    is_california: Mapped[bool] = mapped_column(Boolean, default=False)
    is_cascadia: Mapped[bool] = mapped_column(Boolean, default=False)
    pilotage_url: Mapped[Optional[str]] = mapped_column(String(512))
    mx_url: Mapped[Optional[str]] = mapped_column(String(512))
    tariff_url: Mapped[Optional[str]] = mapped_column(String(512))

class Fee(Base):
    __tablename__ = "fees"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(64), unique=True)
    name: Mapped[str] = mapped_column(String(200))
    scope: Mapped[str] = mapped_column(String(24))  # federal/state/port
    unit: Mapped[str] = mapped_column(String(24))   # per_call/per_net_ton/per_passenger/flat
    rate: Mapped[Numeric] = mapped_column(Numeric(12, 4))
    currency: Mapped[str] = mapped_column(String(3), default="USD")
    cap_amount: Mapped[Optional[Numeric]] = mapped_column(Numeric(12, 4))
    cap_period: Mapped[Optional[str]] = mapped_column(String(24))  # calendar_year/tonnage_year/none
    applies_state: Mapped[Optional[str]] = mapped_column(String(2))
    applies_port_code: Mapped[Optional[str]] = mapped_column(String(12))
    applies_cascadia: Mapped[Optional[bool]] = mapped_column(Boolean)
    effective_start: Mapped[Date]
    effective_end: Mapped[Optional[Date]]
    source_url: Mapped[Optional[str]] = mapped_column(String(512))
    authority: Mapped[Optional[str]] = mapped_column(String(512))

class Source(Base):
    __tablename__ = "sources"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(200))
    url: Mapped[str] = mapped_column(String(512))
    type: Mapped[str] = mapped_column(String(24))  # pilotage/tariff/law/program/api
    effective_date: Mapped[Optional[Date]]

