from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, TypeVar

import pandas as pd
from pydantic import BaseModel, ValidationError
from sqlalchemy import delete
from sqlalchemy.orm import Session

from src.database import SessionLocal
from src.models import (
    AccountMap,
    Customer,
    DiscountRule,
    FxRateUSD,
    HoldingSnapshot,
    PriceHistory,
    StockMaster,
    Trade,
)
from src.schemas import (
    AccountMapRow,
    CustomerRow,
    DiscountRuleRow,
    FxRateUsdRow,
    HoldingSnapshotRow,
    PriceHistoryRow,
    StockMasterRow,
    TradeRow,
)

LOGGER = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

TModel = TypeVar("TModel")
TSchema = TypeVar("TSchema", bound=BaseModel)


@dataclass(slots=True)
class IngestionResult:
    table_name: str
    success_count: int = 0
    failure_count: int = 0
    errors: list[str] = field(default_factory=list)


def _load_csv(filename: str, dedupe_subset: list[str] | None = None) -> list[dict[str, object]]:
    """Load a CSV file, optionally drop duplicates, and normalize missing values."""
    path = DATA_DIR / filename
    df = pd.read_csv(path)

    if dedupe_subset:
        df = df.drop_duplicates(subset=dedupe_subset, keep="first")

    df = df.where(pd.notna(df), None)
    return df.to_dict(orient="records")


def _bulk_insert(session: Session, model_cls: type[TModel], objects: List[TModel]) -> None:
    """Replace all rows in the table for model_cls with the given objects."""
    session.execute(delete(model_cls))
    if objects:
        session.add_all(objects)


def _build_row_error(table_name: str, row_number: int, message: str) -> str:
    return f"{table_name}: row {row_number} - {message}"


def _ingest_file(
    *,
    table_name: str,
    filename: str,
    schema_cls: type[TSchema],
    model_cls: type[TModel],
    factory: Callable[[TSchema], TModel],
    dedupe_subset: list[str] | None = None,
) -> IngestionResult:
    result = IngestionResult(table_name=table_name)
    objects: List[TModel] = []

    LOGGER.info("Starting ingestion for %s.", table_name)

    try:
        records = _load_csv(filename, dedupe_subset=dedupe_subset)
    except Exception as exc:  # noqa: BLE001
        message = f"{table_name}: failed to read {filename} - {exc}"
        LOGGER.exception(message)
        result.errors.append(message)
        return result

    for row_number, record in enumerate(records, start=2):
        try:
            validated_row = schema_cls.model_validate(record)
            objects.append(factory(validated_row))
        except ValidationError as exc:
            message = _build_row_error(table_name, row_number, exc.json())
            LOGGER.warning(message)
            result.failure_count += 1
            result.errors.append(message)
        except Exception as exc:  # noqa: BLE001
            message = _build_row_error(table_name, row_number, str(exc))
            LOGGER.exception(message)
            result.failure_count += 1
            result.errors.append(message)

    try:
        with SessionLocal() as session:
            with session.begin():
                _bulk_insert(session, model_cls, objects)
        result.success_count = len(objects)
        LOGGER.info("Successfully ingested %d rows into %s.", result.success_count, table_name)
    except Exception as exc:  # noqa: BLE001
        message = f"{table_name}: database transaction failed - {exc}"
        LOGGER.exception(message)
        result.errors.append(message)
        result.failure_count += len(objects)
        result.success_count = 0

    return result


def ingest_customers() -> IngestionResult:
    return _ingest_file(
        table_name="customers",
        filename="customers.csv",
        schema_cls=CustomerRow,
        model_cls=Customer,
        factory=lambda row: Customer(
            id=row.CustomerID,
            name=row.CustomerName,
            join_date=row.JoinDate,
            tenure_years=row.TenureYears,
            segment=row.Segment,
        ),
    )


def ingest_account_map() -> IngestionResult:
    return _ingest_file(
        table_name="account_map",
        filename="account_map.csv",
        schema_cls=AccountMapRow,
        model_cls=AccountMap,
        factory=lambda row: AccountMap(
            customer_id=row.cust_id,
            external_account=row.external_account,
            segment_tag=row.SegmentTag,
        ),
    )


def ingest_stocks_master() -> IngestionResult:
    return _ingest_file(
        table_name="stocks_master",
        filename="stocks_master.csv",
        schema_cls=StockMasterRow,
        model_cls=StockMaster,
        dedupe_subset=["Ticker"],
        factory=lambda row: StockMaster(
            ticker=row.Ticker,
            company_name=row.CompanyName,
            exchange=row.Exchange,
            currency=row.Currency,
            sector=row.Sector,
            country=row.Country,
        ),
    )


def ingest_discount_rules() -> IngestionResult:
    return _ingest_file(
        table_name="discount_rules",
        filename="discount_rules.csv",
        schema_cls=DiscountRuleRow,
        model_cls=DiscountRule,
        factory=lambda row: DiscountRule(
            min_portfolio_value_from_usd=row.MinPortfolioValueFromUSD,
            min_tenure_years=row.MinTenureYears,
            base_discount_pct=row.BaseDiscountPct,
            tenure_bonus_pct=row.TenureBonusPct,
        ),
    )


def ingest_fx_rates() -> IngestionResult:
    return _ingest_file(
        table_name="fx_rates_usd",
        filename="fx_rates_usd.csv",
        schema_cls=FxRateUsdRow,
        model_cls=FxRateUSD,
        factory=lambda row: FxRateUSD(
            currency=row.Currency,
            date=row.Date,
            usd_per_unit=row.USD_per_unit,
        ),
    )


def ingest_holdings_snapshot() -> IngestionResult:
    return _ingest_file(
        table_name="holdings_snapshot",
        filename="holdings_snapshot.csv",
        schema_cls=HoldingSnapshotRow,
        model_cls=HoldingSnapshot,
        factory=lambda row: HoldingSnapshot(
            customer_id=row.CustomerID,
            ticker=row.Ticker,
            quantity=row.Quantity,
            as_of_date=row.AsOfDate,
        ),
    )


def ingest_price_history() -> IngestionResult:
    return _ingest_file(
        table_name="price_history",
        filename="price_history.csv",
        schema_cls=PriceHistoryRow,
        model_cls=PriceHistory,
        factory=lambda row: PriceHistory(
            ticker=row.Ticker,
            date=row.Date,
            close=row.Close,
            currency=row.Currency,
        ),
    )


def ingest_trades() -> IngestionResult:
    return _ingest_file(
        table_name="trades_source_a",
        filename="trades_source_a.csv",
        schema_cls=TradeRow,
        model_cls=Trade,
        factory=lambda row: Trade(
            customer_id=row.Customer_ID,
            trade_date=row.tradeDate,
            ticker=row.ticker,
            side=row.Side,
            quantity=row.Quantity,
            price=row.Px,
            trade_currency=row.TradeCurrency,
            fee_usd=row.FeeUSD,
        ),
    )


def _format_summary(results: list[IngestionResult]) -> str:
    header = f"{'Table':<20} {'Success':>10} {'Failed':>10}"
    separator = "-" * len(header)

    lines = [
        "",
        "Ingestion Summary",
        separator,
        header,
        separator,
    ]

    total_success = 0
    total_failure = 0

    for result in results:
        total_success += result.success_count
        total_failure += result.failure_count
        lines.append(
            f"{result.table_name:<20} {result.success_count:>10} {result.failure_count:>10}"
        )

    lines.extend(
        [
            separator,
            f"{'TOTAL':<20} {total_success:>10} {total_failure:>10}",
        ]
    )

    all_errors = [error for result in results for error in result.errors]
    if all_errors:
        lines.append("")
        lines.append("Errors")
        lines.append(separator)
        lines.extend(f"- {error}" for error in all_errors)

    return "\n".join(lines)


def ingest_all() -> list[IngestionResult]:
    """Run full ingestion pipeline in foreign-key-safe order."""
    results = [
        ingest_customers(),
        ingest_account_map(),
        ingest_stocks_master(),
        ingest_discount_rules(),
        ingest_fx_rates(),
        ingest_holdings_snapshot(),
        ingest_price_history(),
        ingest_trades(),
    ]

    print(_format_summary(results))
    return results

