from __future__ import annotations

from datetime import date
from decimal import Decimal

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict
from sqlalchemy.orm import Session

from src.analytics import build_customer_report, build_platform_summary
from src.database import SessionLocal

app = FastAPI(
    title="Finance Platform Reporting API",
    description="Aggregated portfolio analytics and customer reporting.",
    version="1.0.0",
)


# ---------------------------------------------------------------------------
# Dependency: database session per request
# ---------------------------------------------------------------------------


def get_db() -> Session:  # type: ignore[return]
    with SessionLocal() as session:
        yield session


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    status: str
    message: str


class PlatformSummaryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    total_aum_usd: Decimal
    total_customers: int
    total_trades: int


class HoldingResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    ticker: str
    company_name: str | None
    sector: str | None
    quantity: int
    latest_price_native: Decimal
    native_currency: str
    latest_price_usd: Decimal
    market_value_usd: Decimal
    price_date: date | None


class CustomerReportResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    customer_id: str
    customer_name: str
    segment: str
    join_date: date
    tenure_years: Decimal
    external_account: str | None
    portfolio_value_usd: Decimal
    discount_pct: Decimal
    holdings: list[HoldingResponse]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check",
    tags=["System"],
)
def health() -> HealthResponse:
    """Returns 200 when the service is up and the database is reachable."""
    try:
        with SessionLocal() as session:
            session.execute(__import__("sqlalchemy").text("SELECT 1"))
        db_status = "ok"
    except Exception:  # noqa: BLE001
        db_status = "unreachable"

    return HealthResponse(
        status="ok" if db_status == "ok" else "degraded",
        message=f"API is running. Database: {db_status}.",
    )


@app.get(
    "/analytics/summary",
    response_model=PlatformSummaryResponse,
    summary="Organisation-level platform summary",
    tags=["Analytics"],
)
def platform_summary(db: Session = Depends(get_db)) -> PlatformSummaryResponse:
    """Return high-level metrics across the entire platform:

    - **total_aum_usd** – sum of all customer portfolio values in USD,
      calculated from the latest holdings snapshot and most recent prices.
    - **total_customers** – number of unique customers in the database.
    - **total_trades** – total number of trade records processed.
    """
    summary = build_platform_summary(db)
    return PlatformSummaryResponse.model_validate(summary, from_attributes=True)


@app.get(
    "/reports/customer/{customer_id}",
    response_model=CustomerReportResponse,
    summary="Full customer portfolio report",
    tags=["Reports"],
)
def customer_report(
    customer_id: str,
    db: Session = Depends(get_db),
) -> CustomerReportResponse:
    """Return a full portfolio report for *customer_id*, including:

    - Customer metadata (name, segment, tenure, external account)
    - Total portfolio value in USD (latest prices × holdings)
    - Per-holding breakdown with price, currency, and USD conversion
    - Applicable discount percentage based on portfolio value and tenure
    """
    report = build_customer_report(db, customer_id)

    if report is None:
        raise HTTPException(status_code=404, detail=f"Customer '{customer_id}' not found.")

    return CustomerReportResponse.model_validate(report, from_attributes=True)
