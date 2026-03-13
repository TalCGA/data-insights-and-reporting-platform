from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

DATABASE_URL = "sqlite:///finance_platform.db"

engine: Engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, class_=Session, expire_on_commit=False)


@contextmanager
def get_session() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

