from __future__ import annotations

from src.database import engine
from src.models import Base


def main() -> None:
    Base.metadata.create_all(bind=engine)


if __name__ == "__main__":
    main()

