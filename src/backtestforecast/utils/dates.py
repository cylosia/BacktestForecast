"""Canonical date utilities for US-market-aligned operations.

Holiday data comes from two sources, merged at runtime:

1. A static fallback set covering 2025-2027 (always available).
2. A dynamic set fetched from the Massive ``/v1/marketstatus/upcoming``
   endpoint and cached in Redis (key ``bff:market_holidays``).  A Celery
   beat task refreshes this cache weekly; processes read from Redis and
   hold an in-memory copy for up to 1 hour to avoid per-call overhead.

If Redis is unavailable the static set is used alone.
"""

from __future__ import annotations

import threading
import time as _time
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import structlog

_US_EASTERN = ZoneInfo("America/New_York")

logger = structlog.get_logger("dates")

REDIS_HOLIDAYS_KEY = "bff:market_holidays"
REDIS_HOLIDAYS_TTL_SECONDS = 60 * 60 * 24 * 30  # 30 days

_MEMORY_CACHE_TTL_SECONDS = 3600.0  # re-read Redis at most once per hour

_US_MARKET_HOLIDAYS: frozenset[date] = frozenset({
    # 2025
    date(2025, 1, 1),    # New Year's Day
    date(2025, 1, 20),   # MLK Day
    date(2025, 2, 17),   # Presidents' Day
    date(2025, 4, 18),   # Good Friday
    date(2025, 5, 26),   # Memorial Day
    date(2025, 6, 19),   # Juneteenth
    date(2025, 7, 4),    # Independence Day
    date(2025, 9, 1),    # Labor Day
    date(2025, 11, 27),  # Thanksgiving
    date(2025, 12, 25),  # Christmas
    # 2026
    date(2026, 1, 1),    # New Year's Day
    date(2026, 1, 19),   # MLK Day
    date(2026, 2, 16),   # Presidents' Day
    date(2026, 4, 3),    # Good Friday
    date(2026, 5, 25),   # Memorial Day
    date(2026, 6, 19),   # Juneteenth
    date(2026, 7, 3),    # Independence Day (observed)
    date(2026, 9, 7),    # Labor Day
    date(2026, 11, 26),  # Thanksgiving
    date(2026, 12, 25),  # Christmas
    # 2027
    date(2027, 1, 1),    # New Year's Day
    date(2027, 1, 18),   # MLK Day
    date(2027, 2, 15),   # Presidents' Day
    date(2027, 3, 26),   # Good Friday
    date(2027, 5, 31),   # Memorial Day
    date(2027, 6, 18),   # Juneteenth (observed)
    date(2027, 7, 5),    # Independence Day (observed)
    date(2027, 9, 6),    # Labor Day
    date(2027, 11, 25),  # Thanksgiving
    date(2027, 12, 24),  # Christmas (observed)
})

_dynamic_holidays: frozenset[date] = frozenset()
_dynamic_holidays_loaded_at: float = 0.0
_dynamic_holidays_lock = threading.Lock()


def _load_dynamic_holidays_from_redis() -> frozenset[date]:
    """Read holiday dates from the Redis cache.  Returns an empty set on any failure."""
    try:
        from redis import Redis

        from backtestforecast.config import get_settings

        settings = get_settings()
        if not settings.redis_url:
            return frozenset()
        r = Redis.from_url(settings.redis_url, decode_responses=True, socket_timeout=3)
        try:
            raw_dates = r.smembers(REDIS_HOLIDAYS_KEY)
            return frozenset(date.fromisoformat(d) for d in raw_dates if d)
        finally:
            r.close()
    except Exception:
        return frozenset()


def _get_dynamic_holidays() -> frozenset[date]:
    global _dynamic_holidays, _dynamic_holidays_loaded_at

    now = _time.monotonic()
    if now - _dynamic_holidays_loaded_at < _MEMORY_CACHE_TTL_SECONDS and _dynamic_holidays:
        return _dynamic_holidays

    with _dynamic_holidays_lock:
        now = _time.monotonic()
        if now - _dynamic_holidays_loaded_at < _MEMORY_CACHE_TTL_SECONDS and _dynamic_holidays:
            return _dynamic_holidays

        loaded = _load_dynamic_holidays_from_redis()
        if loaded:
            _dynamic_holidays = loaded
            _dynamic_holidays_loaded_at = now
        return _dynamic_holidays


def get_all_holidays() -> frozenset[date]:
    """Return the union of static and dynamically-fetched market holidays."""
    dynamic = _get_dynamic_holidays()
    if dynamic:
        return _US_MARKET_HOLIDAYS | dynamic
    return _US_MARKET_HOLIDAYS


def store_holidays_in_redis(holidays: list[date]) -> int:
    """Write *holidays* into the Redis cache and return the count stored.

    Called by the ``maintenance.refresh_market_holidays`` Celery task.
    """
    from redis import Redis

    from backtestforecast.config import get_settings

    if not holidays:
        return 0

    settings = get_settings()
    r = Redis.from_url(settings.redis_url, decode_responses=True, socket_timeout=5)
    try:
        pipe = r.pipeline()
        pipe.delete(REDIS_HOLIDAYS_KEY)
        pipe.sadd(REDIS_HOLIDAYS_KEY, *(d.isoformat() for d in holidays))
        pipe.expire(REDIS_HOLIDAYS_KEY, REDIS_HOLIDAYS_TTL_SECONDS)
        pipe.execute()
        logger.info("market_holidays.stored_in_redis", count=len(holidays))
        return len(holidays)
    finally:
        r.close()


def invalidate_holiday_cache() -> None:
    """Force the next call to re-read Redis.  Useful after a refresh task runs."""
    global _dynamic_holidays_loaded_at
    _dynamic_holidays_loaded_at = 0.0


def is_market_holiday(d: date) -> bool:
    """Return True if *d* is a known US equity market holiday."""
    return d in get_all_holidays()


def market_date_today() -> date:
    """Return the most recent trading day in US Eastern time.

    Rolls back past weekends and known US equity market holidays so that
    pipeline runs, scans, and analysis always reference a real trading day.
    """
    holidays = get_all_holidays()
    today = datetime.now(_US_EASTERN).date()
    while today.weekday() >= 5 or today in holidays:
        today -= timedelta(days=1)
    return today
