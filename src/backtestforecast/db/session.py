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
        # Emit a lightweight ``SELECT 1`` before handing out a connection to
        # detect stale/broken connections after DB restarts or network blips.
        # This adds minimal latency (~1ms) but prevents SQLAlchemy from
        # handing the application a disconnected connection from the pool.
        "pool_pre_ping": True,
    }
    if url.startswith("sqlite"):
        engine_kwargs["connect_args"] = {"check_same_thread": False}
    else:
        engine_kwargs["pool_size"] = cfg.db_pool_size
        engine_kwargs["max_overflow"] = cfg.db_pool_max_overflow
        engine_kwargs["pool_recycle"] = cfg.db_pool_recycle
        engine_kwargs["pool_timeout"] = 10
    return create_engine(url, **engine_kwargs)


@lru_cache
def _get_engine() -> Engine:
    return build_engine(get_settings())


@lru_cache
def _get_session_factory() -> sessionmaker[Session]:
    return sessionmaker(
        bind=_get_engine(),
        autoflush=False,
        expire_on_commit=True,
    )


def create_session() -> Session:
    return _get_session_factory()()


# Backward-compatible alias
SessionLocal = create_session


def get_db() -> Generator[Session, None, None]:
    """Yield a SQLAlchemy session for request-scoped use.

    The session is configured with ``autoflush=False`` and does NOT
    auto-commit.  Callers must explicitly call ``db.commit()`` to
    persist changes.  On unhandled exceptions the session is rolled back
    automatically; it is always closed when the request finishes.
    """
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


def get_pool_stats() -> dict[str, int]:
    """Return connection pool statistics for monitoring."""
    pool = _get_engine().pool
    return {
        "pool_size": pool.size(),
        "checked_in": pool.checkedin(),
        "checked_out": pool.checkedout(),
        "overflow": pool.overflow(),
    }
