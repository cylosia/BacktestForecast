from __future__ import annotations

from collections.abc import Generator
from functools import lru_cache

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from backtestforecast.config import Settings, get_settings


def build_engine(
    settings: Settings | None = None,
    *,
    database_url: str | None = None,
) -> Engine:
    cfg = settings or get_settings()
    url = database_url or cfg.database_url
    engine_kwargs: dict[str, object] = {
        "pool_pre_ping": True,
    }
    if url.startswith("sqlite"):
        engine_kwargs["connect_args"] = {"check_same_thread": False}
    else:
        engine_kwargs["pool_size"] = cfg.db_pool_size
        engine_kwargs["pool_recycle"] = cfg.db_pool_recycle
    return create_engine(url, **engine_kwargs)


@lru_cache
def _get_engine() -> Engine:
    return build_engine(get_settings())


@lru_cache
def _get_session_factory() -> sessionmaker[Session]:
    return sessionmaker(
        bind=_get_engine(),
        autoflush=False,
        expire_on_commit=False,
    )


def SessionLocal() -> Session:
    return _get_session_factory()()


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def ping_database() -> None:
    with _get_engine().connect() as connection:
        connection.execute(text("SELECT 1"))
