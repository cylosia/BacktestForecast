from __future__ import annotations

from collections.abc import Generator
from functools import lru_cache

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from backtestforecast.config import Settings, get_settings, register_invalidation_callback


def build_engine(
    settings: Settings | None = None,
    *,
    database_url: str | None = None,
    statement_timeout_ms: int = 30_000,
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
        engine_kwargs["connect_args"] = {"options": f"-c statement_timeout={statement_timeout_ms}"}
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


@lru_cache
def _get_worker_engine() -> Engine:
    return build_engine(get_settings(), statement_timeout_ms=300_000)


@lru_cache
def _get_worker_session_factory() -> sessionmaker[Session]:
    return sessionmaker(
        bind=_get_worker_engine(),
        autoflush=False,
        expire_on_commit=True,
    )


def create_worker_session() -> Session:
    """Session with a 5-minute statement_timeout for long-running worker tasks."""
    return _get_worker_session_factory()()


# Backward-compatible alias
SessionLocal = create_session


def get_db() -> Generator[Session, None, None]:
    """Yield a SQLAlchemy session for request-scoped use.

    **Commit contract:** The session is configured with ``autoflush=False``
    and does NOT auto-commit.  Callers (services, routers) MUST explicitly
    call ``db.commit()`` to persist changes.  On unhandled exceptions the
    session is rolled back automatically; it is always closed when the
    request finishes.

    Services should commit at the end of their public method after all
    mutations are applied.  Routers should not commit — that is the
    responsibility of the service layer.
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
        connection.execute(text("SET LOCAL statement_timeout = '2s'"))
        connection.execute(text("SELECT 1"))


def _invalidate_db_caches() -> None:
    """Dispose the current engine and clear cached singletons so fresh
    settings (e.g. rotated credentials) take effect on next access."""
    for engine_fn, factory_fn in [
        (_get_engine, _get_session_factory),
        (_get_worker_engine, _get_worker_session_factory),
    ]:
        if engine_fn.cache_info().currsize > 0:
            try:
                engine_fn().dispose()
            except Exception:
                pass
        engine_fn.cache_clear()
        factory_fn.cache_clear()


register_invalidation_callback(_invalidate_db_caches)


def get_pool_stats() -> dict[str, int]:
    """Return connection pool statistics for monitoring."""
    pool = _get_engine().pool
    stats = {
        "pool_size": pool.size(),
        "checked_in": pool.checkedin(),
        "checked_out": pool.checkedout(),
        "overflow": pool.overflow(),
    }
    from sqlalchemy.pool import QueuePool
    if isinstance(pool, QueuePool):
        stats["max_overflow"] = pool._max_overflow  # type: ignore[attr-defined]
    return stats
