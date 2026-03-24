from __future__ import annotations

from collections.abc import Generator
from contextlib import suppress
from functools import lru_cache

import structlog
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from backtestforecast.config import Settings, get_settings, register_invalidation_callback

# Monitor query performance via pg_stat_statements or application-level
# timing. The statement_timeout protects against runaway queries but does
# not provide visibility into normal query latency distribution.


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
        # pool_recycle should be shorter than the DB's idle connection timeout
        # (default 300s for PostgreSQL's tcp_keepalives_idle). pool_pre_ping
        # provides a safety net, but pool_recycle proactively avoids reusing
        # connections that the DB may have already closed.
        engine_kwargs["pool_recycle"] = cfg.db_pool_recycle
        engine_kwargs["pool_timeout"] = cfg.db_pool_timeout
        engine_kwargs["connect_args"] = {"options": f"-c statement_timeout={statement_timeout_ms}"}
    return create_engine(url, **engine_kwargs)


@lru_cache
def _get_engine() -> Engine:
    settings = get_settings()
    return build_engine(settings, statement_timeout_ms=settings.db_statement_timeout_ms)


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
    settings = get_settings()
    return build_engine(settings, statement_timeout_ms=settings.db_worker_statement_timeout_ms)


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


def get_worker_db() -> Generator[Session, None, None]:
    """Yield a worker session with guaranteed cleanup on exit."""
    db = create_worker_session()
    try:
        yield db
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


@lru_cache
def _get_readonly_engine() -> Engine | None:
    settings = get_settings()
    if not settings.database_read_replica_url:
        return None
    return build_engine(
        settings,
        database_url=settings.database_read_replica_url,
        statement_timeout_ms=settings.db_statement_timeout_ms,
    )


@lru_cache
def _get_readonly_session_factory() -> sessionmaker[Session] | None:
    engine = _get_readonly_engine()
    if engine is None:
        return None
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=True)




def create_readonly_session() -> Session:
    """Create a read-only session, preferring the read replica when configured."""
    factory = _get_readonly_session_factory()
    if factory is None:
        return create_session()
    return factory()

def get_readonly_db() -> Generator[Session, None, None]:
    """Yield a read-only session, preferring the read replica if configured.

    Falls back to the primary database when no replica URL is set.
    Use this for list/detail/compare endpoints that don't mutate data.
    """
    db = create_readonly_session()
    try:
        yield db
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def get_db() -> Generator[Session, None, None]:
    """Yield a SQLAlchemy session for request-scoped use.

    **Commit contract:** The session is configured with ``autoflush=False``
    and does NOT auto-commit.  Callers (services, routers) MUST explicitly
    call ``db.commit()`` to persist changes.  On unhandled exceptions the
    session is rolled back automatically; it is always closed when the
    request finishes.

    Services should commit at the end of their public method after all
    mutations are applied.  Routers should not commit - that is the
    responsibility of the service layer.
    """
    db = create_session()
    try:
        yield db
        if db.new or db.dirty or db.deleted:
            structlog.get_logger("db.session").warning(
                "session.pending_changes_on_close",
                new=len(db.new),
                dirty=len(db.dirty),
                deleted=len(db.deleted),
            )
            db.rollback()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def ping_database() -> None:
    with _get_engine().connect() as connection, connection.begin():
        connection.execute(text("SET LOCAL statement_timeout = '2s'"))
        connection.execute(text("SELECT 1"))


def _invalidate_db_caches() -> None:
    """Dispose the current engine and clear cached singletons so fresh
    settings (e.g. rotated credentials) take effect on next access."""
    for engine_fn, factory_fn in [
        (_get_engine, _get_session_factory),
        (_get_worker_engine, _get_worker_session_factory),
        (_get_readonly_engine, _get_readonly_session_factory),
    ]:
        engine_ref = None
        if engine_fn.cache_info().currsize > 0:
            with suppress(Exception):
                engine_ref = engine_fn()
        engine_fn.cache_clear()
        factory_fn.cache_clear()
        if engine_ref is not None:
            with suppress(Exception):
                engine_ref.dispose()


register_invalidation_callback(_invalidate_db_caches)


def get_pool_stats() -> dict[str, int]:
    """Return connection pool statistics for monitoring."""
    pool = _get_engine().pool
    stats: dict[str, int] = {
        "pool_size": pool.size(),
        "checked_in": pool.checkedin(),
        "checked_out": pool.checkedout(),
        "overflow": pool.overflow(),
    }
    from sqlalchemy.pool import QueuePool
    if isinstance(pool, QueuePool):
        try:
            stats["max_overflow"] = int(getattr(pool, "_max_overflow", -1))
        except (AttributeError, TypeError, ValueError):
            stats["max_overflow"] = -1
    return stats
