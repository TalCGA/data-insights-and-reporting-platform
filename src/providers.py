"""
Data providers
==============
Defines a common DataProvider interface and concrete implementations for each
supported source type:

    CSVDataProvider      – reads a local CSV file via pandas
    MockApiProvider      – simulates an HTTP JSON response from an external API
    MockDatabaseProvider – simulates a SELECT from an external/legacy database

Every provider returns ``list[dict[str, object]]`` so the ingestion engine can
pipe that output directly into Pydantic validation, regardless of origin.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd

LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------


class DataProvider(ABC):
    """Contract that every data source must fulfil.

    ``fetch()`` must return a list of raw records (plain dicts). Missing /
    NaN values should be normalised to ``None`` so Pydantic validation is
    uniform across all sources.
    """

    source_label: str  # shown in logs and the ingestion summary

    @abstractmethod
    def fetch(self) -> list[dict[str, object]]:
        """Return all records from this source as a list of dicts."""


# ---------------------------------------------------------------------------
# CSV source
# ---------------------------------------------------------------------------


class CSVDataProvider(DataProvider):
    """Reads a local CSV file from the project *data/* directory.

    Args:
        filename:      Filename (not a full path) inside the ``data/`` folder.
        dedupe_subset: Optional list of column names to deduplicate on (keep
                       first occurrence).
    """

    _DATA_DIR: Path = Path(__file__).resolve().parent.parent / "data"

    def __init__(self, filename: str, dedupe_subset: list[str] | None = None) -> None:
        self.filename = filename
        self.dedupe_subset = dedupe_subset
        self.source_label = f"CSV ({filename})"

    def fetch(self) -> list[dict[str, object]]:
        path = self._DATA_DIR / self.filename
        LOGGER.debug("CSVDataProvider: reading %s", path)

        df = pd.read_csv(path)

        if self.dedupe_subset:
            before = len(df)
            df = df.drop_duplicates(subset=self.dedupe_subset, keep="first")
            dropped = before - len(df)
            if dropped:
                LOGGER.warning(
                    "CSVDataProvider: dropped %d duplicate row(s) on %s in %s",
                    dropped,
                    self.dedupe_subset,
                    self.filename,
                )

        df = df.where(pd.notna(df), None)
        return df.to_dict(orient="records")


# ---------------------------------------------------------------------------
# Mock API source
# ---------------------------------------------------------------------------


class MockApiProvider(DataProvider):
    """Simulates fetching JSON data from an external REST API.

    In a production system this would call ``httpx.get(url)`` (or similar)
    and return ``response.json()``.  Here we return a hard-coded payload that
    mirrors the shape of the ``fx_rates_usd`` table so the rest of the
    pipeline is exercised identically to a live API call.

    Numbers arrive as strings (as a real JSON response would return them);
    Pydantic coerces them to ``Decimal`` automatically.
    """

    source_label = "Mock API (external FX feed)"

    def fetch(self) -> list[dict[str, object]]:
        LOGGER.debug("MockApiProvider: simulating GET /api/v1/fx-rates")

        records: list[dict[str, object]] = [
            {"Currency": "EUR", "Date": "2025-09-02", "USD_per_unit": "1.0812"},
            {"Currency": "GBP", "Date": "2025-09-02", "USD_per_unit": "1.2201"},
            {"Currency": "ILS", "Date": "2025-09-02", "USD_per_unit": "0.2866"},
            {"Currency": "USD", "Date": "2025-09-02", "USD_per_unit": "1.0"},
        ]

        LOGGER.debug("MockApiProvider: returning %d record(s)", len(records))
        return records


# ---------------------------------------------------------------------------
# Mock external-database source
# ---------------------------------------------------------------------------


class MockDatabaseProvider(DataProvider):
    """Simulates querying an external / legacy relational database.

    In production this would open a second SQLAlchemy engine (e.g. pointing
    at an Oracle or MSSQL instance) and execute a SELECT.  Here we return
    an in-memory list of dicts that mirrors a hypothetical
    ``SELECT cust_id, external_account, SegmentTag FROM crm.account_map``
    result set.

    The records use mixed-case / legacy column names to prove the normalisation
    layer (Pydantic) handles structural differences between source systems.
    """

    source_label = "Mock Database (legacy CRM)"

    def fetch(self) -> list[dict[str, object]]:
        LOGGER.debug("MockDatabaseProvider: simulating SELECT from crm.account_map")

        # The legacy CRM uses snake_case column names.  Our AccountMapRow
        # schema expects exactly these keys, so no mapping is needed here;
        # Pydantic will coerce types if necessary.
        rows: list[dict[str, object]] = [
            {"cust_id": "C100000", "external_account": "CRM-ACC-000000", "SegmentTag": "Retail"},
            {"cust_id": "C100001", "external_account": "CRM-ACC-000001", "SegmentTag": "Retail"},
            {"cust_id": "C100002", "external_account": None, "SegmentTag": "HNW"},
            {"cust_id": "C100003", "external_account": "CRM-ACC-000003", "SegmentTag": "Corporate"},
            # Intentionally bad row – unknown cust_id will trigger FK error at
            # DB level, which the transaction handler will capture gracefully.
            {"cust_id": "INVALID-999", "external_account": "CRM-ACC-BAD", "SegmentTag": None},
        ]

        LOGGER.debug("MockDatabaseProvider: returning %d record(s)", len(rows))
        return rows
