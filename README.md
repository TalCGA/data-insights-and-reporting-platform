# Data Insights & Reporting Platform — Design Document

**Author:** Tal Chausho Gur Arie  
**Version:** 1.0  
**Date:** March 2026

---

## 1. Summary

This platform pulls data from different sources (CSV files, external APIs, and databases), cleans and validates it, stores it in a relational database, and exposes it through a REST API.

The main goals are:
- **Ingest** data from multiple source types — not just CSV files.
- **Validate** every record before it touches the database, so bad data is caught early and logged clearly.
- **Normalise** all monetary values to USD so portfolio values are comparable across currencies.
- **Report** key metrics through a simple API that any frontend or dashboard can consume.

The project is built as a proof of concept using Python, SQLAlchemy, Pydantic, and FastAPI. The architecture is designed so that swapping SQLite for PostgreSQL, or replacing a mock API with a real one, requires minimal code changes.

---

## 2. Key Assumptions

To ensure the system is both robust and practical for a POC, the following assumptions were made:

* **Data Consistency:** It is assumed that while sources vary (CSV, API, DB), they can all be mapped to a standard dictionary format during the ingestion phase.
* **Historical Accuracy:** Currency normalization must use the FX rate from the **date of the price/trade**, not the current date, to reflect true historical value.
* **Fallback Logic:** In cases where an FX rate is missing for a specific date, the system assumes the most recent available rate is a valid proxy to avoid processing failures.
* **Infrastructure:** For the POC, SQLite is assumed to be sufficient for local development, with the understanding that the ORM layer allows an immediate switch to a production-grade DB like PostgreSQL.
* **Error Tolerance:** We assume that "dirty" data should not crash the entire pipeline. Therefore, individual row failures are logged while valid data is committed.

---

## 3. Architecture

The platform is split into four layers. Data flows top to bottom.

```text
┌────────────────────────────────────────────────────────────────────┐
│                          Data Sources                              │
│   CSV Files          External REST API        Legacy Database      │
│   (trades, prices…)  (FX rates feed)          (account map)        │
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

## 4. Tech Stack


| Tool               | What it does here                                     | Why this choice                                                             |
| ------------------ | ----------------------------------------------------- | --------------------------------------------------------------------------- |
| **Python 3.12**    | Backend logic and processing                          | Most practical language for data + backend work                             |
| **SQLAlchemy 2.0** | Defines tables and runs queries                       | Modern type-safe ORM; works with any SQL database without changing the code |
| **Pydantic v2**    | Validates and coerces every row before it hits the DB | Catches bad data with clear error messages; no manual type-checking needed  |
| **FastAPI**        | Serves the API                                        | Auto-generates `/docs` (Swagger UI); Pydantic works natively with it        |
| **SQLite**         | Stores data locally                                   | Zero setup for a POC; one line to switch to PostgreSQL                      |
| **pandas**         | Reads CSV files                                       | Handles edge cases in date parsing and missing values cleanly               |
| **uvicorn**        | Runs the FastAPI server                               | Production-grade ASGI server; supports multiple workers                     |


---

## 5. Ingestion Layer

### 5.1 How the Provider Pattern Works

The ingestion layer is built around the idea that every data source must return the same thing: a list of plain Python dictionaries. This is enforced by the `DataProvider` abstract class.

There are three concrete implementations:

- `CSVDataProvider`: Reads from local CSV files.
- `MockApiProvider`: Simulates an external REST API (JSON) for FX rates.
- `MockDatabaseProvider`: Simulates a legacy CRM database for account mapping.

### 5.2 The Ingestion Pipeline

Every table is loaded through the same `_ingest()` function:  
`provider.fetch()` → `list[dict]` → `Pydantic validation` → `SQLAlchemy ORM` → `Database Transaction`.

---

## 6. Data Modelling

### 6.1 Tables

The database consists of 8 tables. Key relationships include:

- **Customer** can have trades, holdings, and account mapping.
- **Trade** & **HoldingSnapshot** reference a customer and a stock ticker.
- **PriceHistory** & **FxRateUSD** are reference tables used during reporting.
- **DiscountRule** defines conditions for applying discounts to a customer's portfolio.

### 6.2 Currency Normalisation to USD

The platform normalizes all prices to USD using a historical match:

1. Identify the stock's currency and the FX rate for the **price date**.
2. Multiply the price by that rate to get a USD price.
3. If no FX rate exists for a given date, the system falls back to the most recent available rate.

---

## 7. Data Quality & Error Handling

### 7.1 Pydantic Validation

Before data is saved, it passes through Pydantic schemas which handle:

- **Type coercion**: e.g., converting strings to Decimal.
- **Data Cleanup**: Whitespace stripping and normalizing `NaN` to `None`.
- **Custom Validators**: Handling specific date formats (e.g., DD/MM/YYYY).

### 7.2 Row-Level Error Handling

Individual row failures do not stop the ingestion. If a row fails validation, the error is logged and the pipeline continues to the next row. Valid rows are committed in a single transaction per table.

---

## 8. Reporting Layer

The API is built with FastAPI. All response shapes are defined as Pydantic models to ensure consistent structure.

### 8.1 API Interface (Swagger UI)

The system automatically generates interactive documentation for exploring and testing the endpoints.

![API Swagger Interface](screenshots/swagger_ui.png)

### 8.2 Sample Portfolio Report

Example of a generated portfolio report for customer **C100000**, calculated from the provided CSV sample data.

![Customer Portfolio Report Example](screenshots/customer_report.png)

---

## 9. Future Improvements

- **Database**: Migrate to **PostgreSQL** and use **Alembic** for schema versioning.
- **Ingestion**: Replace mock providers with real HTTP clients (`httpx`) and use **Celery** for background tasks.
- **Performance**: Implement **Redis** caching for heavy analytical endpoints and add database indexes.
- **Observability**: Integrate **Structured JSON Logging** and **Sentry** for error tracking.

---

## 10. Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Ingest all data and start the API server
python main.py --serve

# View the API docs
open http://localhost:8000/docs
```

