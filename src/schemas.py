from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, field_validator


class CsvRowModel(BaseModel):
    """Base schema for CSV ingestion with whitespace cleanup and NA handling."""

    model_config = ConfigDict(str_strip_whitespace=True)

    @field_validator("*", mode="before")
    @classmethod
    def normalize_missing_values(cls, value: Any) -> Any:
        if value is None:
            return None

        if isinstance(value, str) and value.strip() == "":
            return None

        try:
            if pd.isna(value):
                return None
        except TypeError:
            pass

        return value


class AccountMapRow(CsvRowModel):
    cust_id: str
    external_account: str | None = None
    SegmentTag: str | None = None


class CustomerRow(CsvRowModel):
    CustomerID: str
    CustomerName: str
    JoinDate: date
    TenureYears: Decimal
    Segment: str


class DiscountRuleRow(CsvRowModel):
    MinPortfolioValueFromUSD: Decimal
    MinTenureYears: Decimal
    BaseDiscountPct: Decimal
    TenureBonusPct: Decimal


class FxRateUsdRow(CsvRowModel):
    Currency: str
    Date: date
    USD_per_unit: Decimal


class HoldingSnapshotRow(CsvRowModel):
    CustomerID: str
    Ticker: str
    Quantity: int
    AsOfDate: date


class PriceHistoryRow(CsvRowModel):
    Ticker: str
    Date: date
    Close: Decimal
    Currency: str

    @field_validator("Date", mode="before")
    @classmethod
    def parse_day_first_date(cls, value: Any) -> Any:
        if value is None:
            return value

        if isinstance(value, str):
            return datetime.strptime(value, "%d/%m/%Y").date()

        return value


class StockMasterRow(CsvRowModel):
    Ticker: str
    CompanyName: str | None = None
    Exchange: str
    Currency: str
    Sector: str | None = None
    Country: str | None = None


class TradeRow(CsvRowModel):
    Customer_ID: str
    tradeDate: date
    ticker: str
    Side: str
    Quantity: int
    Px: Decimal
    TradeCurrency: str
    FeeUSD: Decimal | None = Decimal("0") 

    @field_validator("FeeUSD", mode="before")
    @classmethod
    def default_fee_usd(cls, value: Any) -> Any:
        if value is None:
            return Decimal("0")
        return value
