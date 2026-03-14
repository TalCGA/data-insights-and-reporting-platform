"""
Ingestion layer
===============
Validates and persists data from any DataProvider into the local database.

Architecture
------------
  DataProvider  →  list[dict]  →  Pydantic schema  →  SQLAlchemy model  →  DB
  ──────────────────────────────────────────────────────────────────────────────
  CSVDataProvider     local CSV files          (structured)
  MockApiProvider     external REST / JSON     (semi-structured)
  MockDatabaseProvider  legacy CRM database    (structured, foreign schema)

Every provider is decoupled from the Pydantic and ORM layers: swapping the
source never touches validation or persistence code.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable, List, TypeVar

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
from src.providers import (
    CSVDataProvider,
    DataProvider,
    MockApiProvider,
    MockDatabaseProvider,
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

TModel = TypeVar("TModel")
TSchema = TypeVar("TSchema", bound=BaseModel)


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class IngestionResult:
    table_name: str
    source_label: str = "unknown"
    success_count: int = 0
    failure_count: int = 0
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------


def _bulk_insert(session: Session, model_cls: type[TModel], objects: List[TModel]) -> None:
    """Truncate and replace all rows for *model_cls* in a single operation."""
    session.execute(delete(model_cls))
    if objects:
        session.add_all(objects)


def _build_row_error(table_name: str, row_number: int, message: str) -> str:
    return f"{table_name}: row {row_number} - {message}"


# ---------------------------------------------------------------------------
# Generic ingestion engine
# ---------------------------------------------------------------------------


def _ingest(
    *,
    table_name: str,
    provider: DataProvider,
    schema_cls: type[TSchema],
    model_cls: type[TModel],
    factory: Callable[[TSchema], TModel],
) -> IngestionResult:
    """
    Core ingestion pipeline.

    1. Ask the provider for raw records (list[dict]).
    2. Validate / coerce each record through *schema_cls*.
    3. Convert validated rows to ORM instances via *factory*.
    4. Persist all valid instances in a single atomic transaction.

    Row-level failures are logged and counted without halting the pipeline.
    A transaction failure rolls back all rows for this table and is reported
    in the result.
    """
    result = IngestionResult(table_name=table_name, source_label=provider.source_label)
    objects: List[TModel] = []

    LOGGER.info("Ingesting '%s' from %s.", table_name, provider.source_label)

    # ── 1. Fetch raw records ────────────────────────────────────────────────
    try:
        records = provider.fetch()
    except Exception as exc:  # noqa: BLE001
        message = f"{table_name}: provider fetch failed - {exc}"
        LOGGER.exception(message)
        result.errors.append(message)
        return result

    # ── 2 & 3. Validate and build ORM objects ───────────────────────────────
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

    # ── 4. Persist ──────────────────────────────────────────────────────────
    try:
        with SessionLocal() as session:
            with session.begin():
                _bulk_insert(session, model_cls, objects)
        result.success_count = len(objects)
        LOGGER.info(
            "Persisted %d row(s) into '%s' (source: %s).",
            result.success_count,
            table_name,
            provider.source_label,
        )
    except Exception as exc:  # noqa: BLE001
        message = f"{table_name}: database transaction failed - {exc}"
        LOGGER.exception(message)
        result.errors.append(message)
        result.failure_count += len(objects)
        result.success_count = 0

    return result


# ---------------------------------------------------------------------------
# Per-table ingestion functions
# Each function is responsible for:
#   • choosing the DataProvider (the "source")
#   • mapping validated schema fields to the ORM model
# ---------------------------------------------------------------------------


def ingest_customers() -> IngestionResult:
    return _ingest(
        table_name="customers",
        provider=CSVDataProvider("customers.csv"),
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
    """Ingest account mapping data from an external (legacy CRM) database source."""
    return _ingest(
        table_name="account_map",
        provider=MockDatabaseProvider(),
        schema_cls=AccountMapRow,
        model_cls=AccountMap,
        factory=lambda row: AccountMap(
            customer_id=row.cust_id,
            external_account=row.external_account,
            segment_tag=row.SegmentTag,
        ),
    )


def ingest_stocks_master() -> IngestionResult:
    return _ingest(
        table_name="stocks_master",
        provider=CSVDataProvider("stocks_master.csv", dedupe_subset=["Ticker"]),
        schema_cls=StockMasterRow,
        model_cls=StockMaster,
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
    return _ingest(
        table_name="discount_rules",
        provider=CSVDataProvider("discount_rules.csv"),
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
    """Ingest FX rates from a mock external API (simulates a live currency feed)."""
    return _ingest(
        table_name="fx_rates_usd",
        provider=MockApiProvider(),
        schema_cls=FxRateUsdRow,
        model_cls=FxRateUSD,
        factory=lambda row: FxRateUSD(
            currency=row.Currency,
            date=row.Date,
            usd_per_unit=row.USD_per_unit,
        ),
    )


def ingest_holdings_snapshot() -> IngestionResult:
    return _ingest(
        table_name="holdings_snapshot",
        provider=CSVDataProvider("holdings_snapshot.csv"),
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
    return _ingest(
        table_name="price_history",
        provider=CSVDataProvider("price_history.csv"),
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
    """Ingest trades from CSV (source system A)."""
    return _ingest(
        table_name="trades_source_a",
        provider=CSVDataProvider("trades_source_a.csv"),
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


# ---------------------------------------------------------------------------
# Summary formatting
# ---------------------------------------------------------------------------


def _format_summary(results: list[IngestionResult]) -> str:
    col_table = 22
    col_source = 36
    col_count = 10

    header = (
        f"{'Table':<{col_table}} {'Source':<{col_source}}"
        f" {'Success':>{col_count}} {'Failed':>{col_count}}"
    )
    separator = "─" * len(header)

    lines = ["", "Ingestion Summary", separator, header, separator]

    total_success = 0
    total_failure = 0

    for result in results:
        total_success += result.success_count
        total_failure += result.failure_count
        lines.append(
            f"{result.table_name:<{col_table}} {result.source_label:<{col_source}}"
            f" {result.success_count:>{col_count}} {result.failure_count:>{col_count}}"
        )

    lines.extend(
        [
            separator,
            f"{'TOTAL':<{col_table}} {'':<{col_source}}"
            f" {total_success:>{col_count}} {total_failure:>{col_count}}",
        ]
    )

    all_errors = [error for result in results for error in result.errors]
    if all_errors:
        lines.append("")
        lines.append("Errors")
        lines.append(separator)
        lines.extend(f"  • {error}" for error in all_errors)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Unified entry point
# ---------------------------------------------------------------------------


def ingest_all() -> list[IngestionResult]:
    """Run the full ingestion pipeline, demonstrating all three source types.

    Source breakdown
    ────────────────
    CSV              customers, stocks_master, discount_rules,
                     holdings_snapshot, price_history, trades_source_a
    Mock API         fx_rates_usd   (simulates a live external currency feed)
    Mock Database    account_map    (simulates a legacy CRM database SELECT)
    """
    results = [
        # ── CSV sources ──────────────────────────────────────────────────────
        ingest_customers(),
        ingest_stocks_master(),
        ingest_discount_rules(),
        # ── External Database source ─────────────────────────────────────────
        ingest_account_map(),
        # ── CSV sources (depend on customers / stocks_master) ────────────────
        ingest_fx_rates(),          # ← Mock API
        ingest_holdings_snapshot(),
        ingest_price_history(),
        ingest_trades(),
    ]

    print(_format_summary(results))
    return results
