# Data Insights & Reporting Platform — Design Document

**Author:** Tal Chausho Gur Arie
**Version:** 1.0  
**Date:** March 2026

---

## 1. Executive Summary

This platform pulls data from different sources (CSV files, external APIs, and databases), cleans and validates it, stores it in a relational database, and exposes it through a REST API.

The main goals are:

- **Ingest** data from multiple source types — not just CSV files.
- **Validate** every record before it touches the database, so bad data is caught early and logged clearly.
- **Normalise** all monetary values to USD so portfolio values are comparable across currencies.
- **Report** key metrics through a simple API that any frontend or dashboard can consume.

The project is built as a proof of concept using Python, SQLAlchemy, Pydantic, and FastAPI. The architecture is designed so that swapping SQLite for PostgreSQL, or replacing a mock API with a real one, requires minimal code changes.

---

## 2. Architecture

The platform is split into four layers. Data flows top to bottom.

```
┌────────────────────────────────────────────────────────────────────┐
│                          Data Sources                              │
│   CSV Files          External REST API        Legacy Database      │
│   (trades, prices…)  (FX rates feed)          (account map)       │
└───────────┬──────────────────┬─────────────────────┬──────────────┘
            │                  │                     │
            ▼                  ▼                     ▼
┌────────────────────────────────────────────────────────────────────┐
│                      Ingestion Layer                               │
│               providers.py  +  ingestion.py                       │
│                                                                    │
│  Each source has a Provider class. All providers return            │
│  the same thing: a list of plain Python dicts.                     │
│                                                                    │
│  Those dicts are then validated by a Pydantic schema               │
│  and converted into SQLAlchemy ORM objects ready to save.          │
└───────────────────────────┬────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────────────┐
│                       Storage Layer                                │
│                  models.py  +  database.py                         │
│                                                                    │
│  SQLite in development, swappable to PostgreSQL.                   │
│  8 tables defined using SQLAlchemy 2.0 ORM with foreign keys,      │
│  composite primary keys, and indexes on frequently queried columns.│
└───────────────────────────┬────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────────────┐
│                      Analytics Layer                               │
│                        analytics.py                                │
│                                                                    │
│  Queries the database and aggregates data into reports.            │
│  Converts all prices to USD using date-matched FX rates.           │
│  Calculates the discount a customer qualifies for.                 │
└───────────────────────────┬────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────────────┐
│                         API Layer                                  │
│                           api.py                                   │
│                                                                    │
│  GET /health                  — is the service running?            │
│  GET /analytics/summary       — total AUM, customers, trades       │
│  GET /reports/customer/{id}   — full portfolio report per customer │
└────────────────────────────────────────────────────────────────────┘
```

---

## 3. Tech Stack


| Tool               | What it does here                                     | Why this choice                                                             |
| ------------------ | ----------------------------------------------------- | --------------------------------------------------------------------------- |
| **Python 3.12**    | Everything                                            | Most practical language for data + backend work                             |
| **SQLAlchemy 2.0** | Defines tables and runs queries                       | Modern type-safe ORM; works with any SQL database without changing the code |
| **Pydantic v2**    | Validates and coerces every row before it hits the DB | Catches bad data with clear error messages; no manual type-checking needed  |
| **FastAPI**        | Serves the API                                        | Auto-generates `/docs` (Swagger UI); Pydantic works natively with it        |
| **SQLite**         | Stores data locally                                   | Zero setup for a POC; one line to switch to PostgreSQL                      |
| **pandas**         | Reads CSV files                                       | Handles edge cases in date parsing and missing values cleanly               |
| **uvicorn**        | Runs the FastAPI server                               | Production-grade ASGI server; supports multiple workers                     |


---

## 4. Ingestion Layer

### 4.1 How the Provider Pattern Works

The ingestion layer is built around one idea: **every data source must return the same thing** — a list of plain dicts.

This is enforced by the `DataProvider` abstract class in `providers.py`:

```python
class DataProvider(ABC):
    source_label: str

    @abstractmethod
    def fetch(self) -> list[dict[str, object]]: ...
```

There are three concrete implementations:


| Class                  | Source type                | Used for                                      |
| ---------------------- | -------------------------- | --------------------------------------------- |
| `CSVDataProvider`      | Local CSV file             | Customers, trades, prices, holdings, etc.     |
| `MockApiProvider`      | External REST API (JSON)   | FX rates — simulates a live currency feed     |
| `MockDatabaseProvider` | Legacy relational database | Account map — simulates a `SELECT` from a CRM |


Once a provider returns its list of dicts, the rest of the pipeline handles all three sources in exactly the same way. Swapping a mock for a real HTTP client means changing one line in `ingestion.py`.

### 4.2 The Ingestion Pipeline

Every table is loaded through the same `_ingest()` function in `ingestion.py`:

```
provider.fetch()
    → list[dict]
    → Pydantic schema validates each row
    → SQLAlchemy ORM object created
    → all valid rows saved to DB in one transaction
```

At the end of a full run, the pipeline prints a summary table showing how many rows succeeded and failed per source.

---

## 5. Data Modelling

### 5.1 Tables

The database has 8 tables. The key relationships are:

- A **Customer** can have trades, holdings, and one account mapping.
- A **Trade** and a **HoldingSnapshot** both reference a customer and a stock ticker.
- **PriceHistory** and **FxRateUSD** are reference tables looked up during reporting.
- **DiscountRule** defines the conditions for applying a discount to a customer's portfolio.

```
Customer ──< AccountMap
    │
    ├──< Trade >── StockMaster ──< PriceHistory
    │
    └──< HoldingSnapshot >── StockMaster

FxRateUSD       (currency + date → USD rate)
DiscountRule    (portfolio threshold + tenure → discount %)
```

All tables are defined in `models.py` using SQLAlchemy 2.0's `Mapped` / `mapped_column` syntax, which is the modern type-safe way to define ORM models.

### 5.2 Currency Normalisation to USD

Different stocks are priced in different currencies (USD, EUR, GBP, ILS). To calculate a total portfolio value, everything needs to be in the same currency.

The process in `analytics.py` is:

1. Look up the FX rate for the stock's currency on or before the price date.
2. Multiply the stock price by that rate to get a USD price.
3. Multiply the USD price by the quantity held to get a USD market value.
4. Sum all market values to get the total portfolio value.

The key detail is step 1 — the platform uses the FX rate from the **same date as the price**, not today's rate. This gives historically accurate valuations.

If no FX rate exists for a given date, the system falls back to the most recent available rate and logs a warning instead of crashing.

---

## 6. Data Quality & Error Handling

### 6.1 Pydantic Validation

Before any row is saved to the database, it passes through a Pydantic schema. Pydantic handles:

- **Type coercion** — a string like `"10.50"` is automatically converted to `Decimal("10.50")`.
- **Whitespace stripping** — all string fields are trimmed automatically.
- **Missing value normalisation** — pandas `NaN`, empty strings, and `None` are all converted to `None` before validation runs.
- **Edge cases** — for example, `PriceHistoryRow` has a custom validator to handle dates formatted as `DD/MM/YYYY` instead of the standard `YYYY-MM-DD`.

Each table has its own Pydantic schema defined in `schemas.py`.

### 6.2 Row-Level Error Handling

Errors on individual rows do **not** stop the pipeline. The `_ingest()` function in `ingestion.py` handles this like so:

- If a row fails Pydantic validation, the error is logged and the row is counted as a failure. The next row is processed normally.
- Once all rows are processed, all valid rows are saved in a **single database transaction**. If the transaction itself fails (e.g. a foreign key violation), the entire batch for that table is rolled back and reported.
- Tables are independent — a failure in one table does not affect others.

After every ingestion run, a summary is printed:

```
Table                  Source                               Success     Failed
──────────────────────────────────────────────────────────────────────────────
customers              CSV (customers.csv)                     1000          0
stocks_master          CSV (stocks_master.csv)                   45          0
discount_rules         CSV (discount_rules.csv)                   5          0
account_map            Mock Database (legacy CRM)                 4          1
fx_rates_usd           Mock API (external FX feed)                4          0
holdings_snapshot      CSV (holdings_snapshot.csv)             5000          0
price_history          CSV (price_history.csv)                 9800          0
trades_source_a        CSV (trades_source_a.csv)               5000          0
──────────────────────────────────────────────────────────────────────────────
TOTAL                                                          20858          1
```

---

## 7. Reporting Layer

The API is built with FastAPI and defined in `api.py`. All response shapes are defined as Pydantic models, so the structure is validated on the way out as well as the way in.

### `GET /health`

A basic liveness check. Also runs a `SELECT 1` against the database so you can tell if the DB connection is down.

### `GET /analytics/summary`

Returns three organisation-level numbers: total AUM in USD, number of customers, number of trades. The AUM calculation runs in five SQL queries total (one per data type needed), so it stays fast regardless of how many customers there are.

```json
{
  "total_aum_usd": "4821763209.38",
  "total_customers": 1000,
  "total_trades": 5000
}
```

### `GET /reports/customer/{customer_id}`

Returns a full portfolio report for one customer: their personal details, total portfolio value in USD, a breakdown of every holding with its latest price and USD conversion, and the discount percentage they qualify for.

The discount is calculated by finding all `DiscountRule` rows where the customer's portfolio value and tenure exceed the minimum thresholds, then applying the one with the highest total percentage.

---

## 8. Future Improvements

These are the changes that would be needed to move this from a POC to a production system.

### Database

- Replace SQLite with **PostgreSQL**. The SQLAlchemy layer means no application code changes are needed.
- Use **Alembic** to manage schema changes through versioned migration files instead of `create_all()`.
- Replace the full table truncation on each ingest with an **upsert** (`INSERT … ON CONFLICT DO UPDATE`) so re-runs only update changed records.

### Ingestion

- Replace `MockApiProvider` with a real HTTP client (`httpx`) and run ingestion on a schedule using **Celery** or **APScheduler**.
- Replace `MockDatabaseProvider` with a real secondary SQLAlchemy engine pointed at the CRM, with credentials loaded from environment variables.
- Add a **checksum or timestamp** check per file so unchanged files are not re-processed.

### API

- Add **authentication** using API keys or OAuth2 via FastAPI's built-in `Security` dependency.
- Cache `GET /analytics/summary` with **Redis** (e.g. 60-second TTL) since the result only changes after a new ingestion run.

### Observability

- Replace `logging.basicConfig` with **structured JSON logging** (`structlog`) so logs are queryable in tools like Datadog or CloudWatch.
- Add a **Prometheus metrics endpoint** to track ingestion row counts, failure rates, and API response times.
- Integrate **Sentry** for automatic error capture and alerting.

### Testing

- Unit-test each provider with mocked HTTP and database responses.
- Integration-test `_ingest()` against an in-memory SQLite database.
- Test all API endpoints using `pytest` + `httpx.AsyncClient`.

---

## 9. Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Ingest all data and start the API server
python main.py --serve

# Skip re-ingestion if data is already in the DB
python main.py --serve --skip-ingestion

# Run uvicorn directly with hot-reload (useful during development)
uvicorn src.api:app --reload

# View the API docs
open http://localhost:8000/docs
```

