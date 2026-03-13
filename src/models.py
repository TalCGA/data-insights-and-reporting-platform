from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Optional

from sqlalchemy import Date, ForeignKey, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Customer(Base):
    __tablename__ = "customers"

    id: Mapped[str] = mapped_column("CustomerID", String(32), primary_key=True)
    name: Mapped[str] = mapped_column("CustomerName", String(255))
    join_date: Mapped[dt.date] = mapped_column("JoinDate", Date)
    tenure_years: Mapped[Decimal] = mapped_column("TenureYears", Numeric(10, 4))
    segment: Mapped[str] = mapped_column("Segment", String(32))

    account_map: Mapped[Optional[AccountMap]] = relationship(
        back_populates="customer",
        uselist=False,
        cascade="all, delete-orphan",
    )
    trades: Mapped[list[Trade]] = relationship(back_populates="customer")
    holdings: Mapped[list[HoldingSnapshot]] = relationship(back_populates="customer")


class AccountMap(Base):
    __tablename__ = "account_map"
    __table_args__ = (
        UniqueConstraint("cust_id", "external_account", name="uq_account_map_customer_account"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    customer_id: Mapped[str] = mapped_column(
        "cust_id",
        ForeignKey("customers.CustomerID", ondelete="CASCADE"),
        index=True,
    )
    external_account: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    segment_tag: Mapped[Optional[str]] = mapped_column("SegmentTag", String(32), nullable=True)

    customer: Mapped[Customer] = relationship(back_populates="account_map")


class StockMaster(Base):
    __tablename__ = "stocks_master"

    ticker: Mapped[str] = mapped_column("Ticker", String(16), primary_key=True)
    company_name: Mapped[Optional[str]] = mapped_column("CompanyName", String(255), nullable=True)
    exchange: Mapped[str] = mapped_column("Exchange", String(32))
    currency: Mapped[str] = mapped_column("Currency", String(8))
    sector: Mapped[Optional[str]] = mapped_column("Sector", String(64), nullable=True)
    country: Mapped[Optional[str]] = mapped_column("Country", String(8), nullable=True)

    trades: Mapped[list[Trade]] = relationship(back_populates="asset")
    prices: Mapped[list[PriceHistory]] = relationship(back_populates="asset")
    holdings: Mapped[list[HoldingSnapshot]] = relationship(back_populates="asset")


class Trade(Base):
    __tablename__ = "trades_source_a"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    customer_id: Mapped[str] = mapped_column(
        "Customer_ID",
        ForeignKey("customers.CustomerID", ondelete="RESTRICT"),
        index=True,
    )
    trade_date: Mapped[dt.date] = mapped_column("tradeDate", Date, index=True)
    ticker: Mapped[str] = mapped_column(
        "ticker",
        ForeignKey("stocks_master.Ticker", ondelete="RESTRICT"),
        index=True,
    )
    side: Mapped[str] = mapped_column("Side", String(8))
    quantity: Mapped[int] = mapped_column("Quantity", Integer)
    price: Mapped[Decimal] = mapped_column("Px", Numeric(18, 6))
    trade_currency: Mapped[str] = mapped_column("TradeCurrency", String(8))
    fee_usd: Mapped[Optional[Decimal]] = mapped_column("FeeUSD", Numeric(18, 6), nullable=True)

    customer: Mapped[Customer] = relationship(back_populates="trades")
    asset: Mapped[StockMaster] = relationship(back_populates="trades")


class HoldingSnapshot(Base):
    __tablename__ = "holdings_snapshot"

    customer_id: Mapped[str] = mapped_column(
        "CustomerID",
        ForeignKey("customers.CustomerID", ondelete="CASCADE"),
        primary_key=True,
    )
    ticker: Mapped[str] = mapped_column(
        "Ticker",
        ForeignKey("stocks_master.Ticker", ondelete="CASCADE"),
        primary_key=True,
    )
    quantity: Mapped[int] = mapped_column("Quantity", Integer)
    as_of_date: Mapped[dt.date] = mapped_column("AsOfDate", Date, primary_key=True)

    customer: Mapped[Customer] = relationship(back_populates="holdings")
    asset: Mapped[StockMaster] = relationship(back_populates="holdings")


class PriceHistory(Base):
    __tablename__ = "price_history"

    ticker: Mapped[str] = mapped_column(
        "Ticker",
        ForeignKey("stocks_master.Ticker", ondelete="CASCADE"),
        primary_key=True,
    )
    date: Mapped[dt.date] = mapped_column("Date", Date, primary_key=True)
    close: Mapped[Decimal] = mapped_column("Close", Numeric(18, 6))
    currency: Mapped[str] = mapped_column("Currency", String(8))

    asset: Mapped[StockMaster] = relationship(back_populates="prices")


class FxRateUSD(Base):
    __tablename__ = "fx_rates_usd"

    currency: Mapped[str] = mapped_column("Currency", String(8), primary_key=True)
    date: Mapped[dt.date] = mapped_column("Date", Date, primary_key=True)
    usd_per_unit: Mapped[Decimal] = mapped_column("USD_per_unit", Numeric(18, 8))


class DiscountRule(Base):
    __tablename__ = "discount_rules"

    min_portfolio_value_from_usd: Mapped[Decimal] = mapped_column(
        "MinPortfolioValueFromUSD",
        Numeric(18, 2),
        primary_key=True,
    )
    min_tenure_years: Mapped[Decimal] = mapped_column("MinTenureYears", Numeric(10, 4), primary_key=True)
    base_discount_pct: Mapped[Decimal] = mapped_column("BaseDiscountPct", Numeric(6, 4))
    tenure_bonus_pct: Mapped[Decimal] = mapped_column("TenureBonusPct", Numeric(6, 4))

