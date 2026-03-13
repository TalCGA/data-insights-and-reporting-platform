from __future__ import annotations

import logging

from src.database import engine
from src.ingestion import ingest_all
from src.models import Base


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    Base.metadata.create_all(bind=engine)
    ingest_all()


if __name__ == "__main__":
    main()

