from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from src.models import (
    Customer,
    DiscountRule,
    FxRateUSD,
    HoldingSnapshot,
    PriceHistory,
    StockMaster,
    Trade,
)

LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result dataclasses (pure data – no SQLAlchemy deps leak outside)
# ---------------------------------------------------------------------------


@dataclass
class PlatformSummary:
    total_aum_usd: Decimal
    total_customers: int
    total_trades: int


@dataclass
class HoldingDetail:
    ticker: str
    company_name: str | None
    sector: str | None
    quantity: int
    latest_price_native: Decimal
    native_currency: str
    latest_price_usd: Decimal
    market_value_usd: Decimal
    price_date: date | None


@dataclass
class CustomerReport:
    customer_id: str
    customer_name: str
    segment: str
    join_date: date
    tenure_years: Decimal
    external_account: str | None
    portfolio_value_usd: Decimal
    discount_pct: Decimal
    holdings: list[HoldingDetail] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------


def _clean_text(value: str | None) -> str | None:
    """Collapse whitespace and title-case a sector/company name."""
    if not value:
        return None
    cleaned = re.sub(r"\s+", " ", value.strip())
    return cleaned.title()


# ---------------------------------------------------------------------------
# FX helpers
# ---------------------------------------------------------------------------


def _get_fx_rate(
    session: Session,
    currency: str,
    as_of: date,
) -> Decimal:
    """Return USD-per-unit for *currency* on *as_of*, falling back to the
    nearest earlier rate if the exact date is missing.  USD always returns 1.
    """
    if currency.upper() == "USD":
        return Decimal("1")

    row = session.scalar(
        select(FxRateUSD.usd_per_unit)
        .where(FxRateUSD.currency == currency.upper())
        .where(FxRateUSD.date <= as_of)
        .order_by(FxRateUSD.date.desc())
        .limit(1)
    )

    if row is None:
        LOGGER.warning(
            "No FX rate found for %s on or before %s – defaulting to 1.0", currency, as_of
        )
        return Decimal("1")

    return Decimal(str(row))


def _to_usd(amount: Decimal, currency: str, as_of: date, session: Session) -> Decimal:
    rate = _get_fx_rate(session, currency, as_of)
    return (amount * rate).quantize(Decimal("0.0001"))


# ---------------------------------------------------------------------------
# Latest price helper
# ---------------------------------------------------------------------------


def _latest_price(session: Session, ticker: str) -> PriceHistory | None:
    return session.scalar(
        select(PriceHistory)
        .where(PriceHistory.ticker == ticker)
        .order_by(PriceHistory.date.desc())
        .limit(1)
    )


# ---------------------------------------------------------------------------
# Discount engine
# ---------------------------------------------------------------------------


def _calculate_discount(
    session: Session,
    portfolio_value_usd: Decimal,
    tenure_years: Decimal,
) -> Decimal:
    """Select the best-matching discount rule (highest combined pct) for a
    customer based on their portfolio value and tenure.

    A rule qualifies when:
        portfolio_value_usd >= rule.min_portfolio_value_from_usd
        tenure_years        >= rule.min_tenure_years
    """
    qualifying_rules = session.scalars(
        select(DiscountRule)
        .where(DiscountRule.min_portfolio_value_from_usd <= portfolio_value_usd)
        .where(DiscountRule.min_tenure_years <= tenure_years)
    ).all()

    if not qualifying_rules:
        return Decimal("0")

    best = max(
        qualifying_rules,
        key=lambda r: r.base_discount_pct + r.tenure_bonus_pct,
    )

    return (best.base_discount_pct + best.tenure_bonus_pct).quantize(Decimal("0.0001"))


# ---------------------------------------------------------------------------
# Main aggregation entry point
# ---------------------------------------------------------------------------


def build_customer_report(session: Session, customer_id: str) -> CustomerReport | None:
    """Aggregate a full customer report from the database.

    Steps:
        1. Load customer + account map + latest holdings (snapshot).
        2. For each holding: fetch the latest price, normalise to USD.
        3. Sum holdings → total portfolio value in USD.
        4. Look up the best discount rule for that portfolio/tenure.
    """
    customer = session.scalar(
        select(Customer)
        .where(Customer.id == customer_id)
        .options(selectinload(Customer.account_map))
    )

    if customer is None:
        return None

    # Latest holdings: one row per (customer, ticker) with the most recent date.
    # We use a subquery to pick the max AsOfDate per ticker for this customer.
    latest_snapshot_subq = (
        select(
            HoldingSnapshot.ticker,
            HoldingSnapshot.as_of_date,
        )
        .where(HoldingSnapshot.customer_id == customer_id)
        .order_by(HoldingSnapshot.as_of_date.desc())
        .limit(1)
    ).subquery()

    holdings_rows = session.scalars(
        select(HoldingSnapshot)
        .where(HoldingSnapshot.customer_id == customer_id)
        .where(HoldingSnapshot.as_of_date == select(latest_snapshot_subq.c.as_of_date).scalar_subquery())
        .options(selectinload(HoldingSnapshot.asset))
    ).all()

    holding_details: list[HoldingDetail] = []
    portfolio_value_usd = Decimal("0")
    valuation_date = date.today()

    for h in holdings_rows:
        asset: StockMaster = h.asset
        price_row = _latest_price(session, h.ticker)

        if price_row is None:
            LOGGER.warning("No price found for ticker %s – skipping.", h.ticker)
            continue

        valuation_date = price_row.date
        price_usd = _to_usd(price_row.close, price_row.currency, price_row.date, session)
        market_value_usd = (price_usd * Decimal(h.quantity)).quantize(Decimal("0.0001"))
        portfolio_value_usd += market_value_usd

        holding_details.append(
            HoldingDetail(
                ticker=h.ticker,
                company_name=_clean_text(asset.company_name),
                sector=_clean_text(asset.sector),
                quantity=h.quantity,
                latest_price_native=price_row.close,
                native_currency=price_row.currency,
                latest_price_usd=price_usd,
                market_value_usd=market_value_usd,
                price_date=price_row.date,
            )
        )

    discount_pct = _calculate_discount(
        session,
        portfolio_value_usd,
        customer.tenure_years,
    )

    return CustomerReport(
        customer_id=customer.id,
        customer_name=customer.name,
        segment=customer.segment,
        join_date=customer.join_date,
        tenure_years=customer.tenure_years,
        external_account=customer.account_map.external_account if customer.account_map else None,
        portfolio_value_usd=portfolio_value_usd.quantize(Decimal("0.01")),
        discount_pct=discount_pct,
        holdings=sorted(holding_details, key=lambda h: h.market_value_usd, reverse=True),
    )


# ---------------------------------------------------------------------------
# Platform-wide summary aggregation
# ---------------------------------------------------------------------------


def build_platform_summary(session: Session) -> PlatformSummary:
    """Compute organisation-level metrics in a small number of bulk queries.

    Strategy (no per-customer iteration):
        1. COUNT customers and trades directly.
        2. Identify each customer's latest holdings snapshot date via GROUP BY.
        3. Fetch all holdings at those dates in one query.
        4. Fetch the latest price per ticker in one query.
        5. Fetch the latest FX rate per currency in one query.
        6. Multiply and sum in Python to produce total AUM.
    """
    # ── Scalar counts ────────────────────────────────────────────────────────
    total_customers: int = session.scalar(select(func.count()).select_from(Customer)) or 0
    total_trades: int = session.scalar(select(func.count()).select_from(Trade)) or 0

    # ── Latest holdings snapshot per customer ────────────────────────────────
    latest_snapshot_subq = (
        select(
            HoldingSnapshot.customer_id,
            func.max(HoldingSnapshot.as_of_date).label("max_date"),
        )
        .group_by(HoldingSnapshot.customer_id)
        .subquery()
    )

    all_holdings = session.scalars(
        select(HoldingSnapshot).join(
            latest_snapshot_subq,
            (HoldingSnapshot.customer_id == latest_snapshot_subq.c.customer_id)
            & (HoldingSnapshot.as_of_date == latest_snapshot_subq.c.max_date),
        )
    ).all()

    # ── Latest price per ticker ───────────────────────────────────────────────
    latest_price_subq = (
        select(
            PriceHistory.ticker,
            func.max(PriceHistory.date).label("max_date"),
        )
        .group_by(PriceHistory.ticker)
        .subquery()
    )

    price_rows = session.scalars(
        select(PriceHistory).join(
            latest_price_subq,
            (PriceHistory.ticker == latest_price_subq.c.ticker)
            & (PriceHistory.date == latest_price_subq.c.max_date),
        )
    ).all()

    price_map: dict[str, PriceHistory] = {p.ticker: p for p in price_rows}

    # ── Latest FX rate per currency ──────────────────────────────────────────
    latest_fx_subq = (
        select(
            FxRateUSD.currency,
            func.max(FxRateUSD.date).label("max_date"),
        )
        .group_by(FxRateUSD.currency)
        .subquery()
    )

    fx_rows = session.scalars(
        select(FxRateUSD).join(
            latest_fx_subq,
            (FxRateUSD.currency == latest_fx_subq.c.currency)
            & (FxRateUSD.date == latest_fx_subq.c.max_date),
        )
    ).all()

    fx_map: dict[str, Decimal] = {
        fx.currency.upper(): Decimal(str(fx.usd_per_unit)) for fx in fx_rows
    }

    # ── AUM summation ────────────────────────────────────────────────────────
    total_aum = Decimal("0")

    for holding in all_holdings:
        price_row = price_map.get(holding.ticker)
        if price_row is None:
            LOGGER.warning("No price for ticker %s – excluded from AUM.", holding.ticker)
            continue

        rate = fx_map.get(price_row.currency.upper(), Decimal("1"))
        price_usd = (Decimal(str(price_row.close)) * rate).quantize(Decimal("0.0001"))
        total_aum += price_usd * Decimal(holding.quantity)

    return PlatformSummary(
        total_aum_usd=total_aum.quantize(Decimal("0.01")),
        total_customers=total_customers,
        total_trades=total_trades,
    )
