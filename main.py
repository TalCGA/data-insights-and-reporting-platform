from __future__ import annotations

import argparse
import logging

import uvicorn

from src.database import engine
from src.ingestion import ingest_all
from src.models import Base

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Finance Platform – ingestion and API server.")
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Start the FastAPI server after ingestion completes.",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind the API server to (default: 127.0.0.1).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to bind the API server to (default: 8000).",
    )
    parser.add_argument(
        "--skip-ingestion",
        action="store_true",
        help="Skip data ingestion and go straight to serving (useful during development).",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    logger = logging.getLogger(__name__)

    args = _parse_args()

    # ------------------------------------------------------------------
    # 1. Ensure all database tables exist
    # ------------------------------------------------------------------
    logger.info("Initialising database schema.")
    Base.metadata.create_all(bind=engine)

    # ------------------------------------------------------------------
    # 2. Run ingestion pipeline (unless explicitly skipped)
    # ------------------------------------------------------------------
    if not args.skip_ingestion:
        logger.info("Starting data ingestion pipeline.")
        ingest_all()
    else:
        logger.info("Skipping ingestion (--skip-ingestion flag set).")

    # ------------------------------------------------------------------
    # 3. Optionally start the API server
    # ------------------------------------------------------------------
    if args.serve:
        logger.info("Starting API server at http://%s:%d", args.host, args.port)
        uvicorn.run(
            "src.api:app",
            host=args.host,
            port=args.port,
            reload=False,
        )


if __name__ == "__main__":
    main()
