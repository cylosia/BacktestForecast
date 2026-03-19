"""Canonical date utilities for US-market-aligned operations.

Holiday data comes from two sources, merged at runtime:

1. A static fallback set covering 2025-2035 (always available).
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
    date(2027, 12, 31),  # New Year 2028 (observed; Jan 1 2028 is Saturday)
    # 2028
    date(2028, 1, 17),   # MLK Day
    date(2028, 2, 21),   # Presidents' Day
    date(2028, 4, 14),   # Good Friday
    date(2028, 5, 29),   # Memorial Day
    date(2028, 6, 19),   # Juneteenth
    date(2028, 7, 4),    # Independence Day
    date(2028, 9, 4),    # Labor Day
    date(2028, 11, 23),  # Thanksgiving
    date(2028, 12, 25),  # Christmas
    # 2029
    date(2029, 1, 1),    # New Year's Day
    date(2029, 1, 15),   # MLK Day
    date(2029, 2, 19),   # Presidents' Day
    date(2029, 3, 30),   # Good Friday
    date(2029, 5, 28),   # Memorial Day
    date(2029, 6, 19),   # Juneteenth
    date(2029, 7, 4),    # Independence Day
    date(2029, 9, 3),    # Labor Day
    date(2029, 11, 22),  # Thanksgiving
    date(2029, 12, 25),  # Christmas
    # 2030
    date(2030, 1, 1),    # New Year's Day
    date(2030, 1, 21),   # MLK Day
    date(2030, 2, 18),   # Presidents' Day
    date(2030, 4, 19),   # Good Friday
    date(2030, 5, 27),   # Memorial Day
    date(2030, 6, 19),   # Juneteenth
    date(2030, 7, 4),    # Independence Day
    date(2030, 9, 2),    # Labor Day
    date(2030, 11, 28),  # Thanksgiving
    date(2030, 12, 25),  # Christmas
    # 2031
    date(2031, 1, 1),    # New Year's Day
    date(2031, 1, 20),   # MLK Day
    date(2031, 2, 17),   # Presidents' Day
    date(2031, 4, 11),   # Good Friday
    date(2031, 5, 26),   # Memorial Day
    date(2031, 6, 19),   # Juneteenth
    date(2031, 7, 4),    # Independence Day
    date(2031, 9, 1),    # Labor Day
    date(2031, 11, 27),  # Thanksgiving
    date(2031, 12, 25),  # Christmas
    # 2032
    date(2032, 1, 1),    # New Year's Day
    date(2032, 1, 19),   # MLK Day
    date(2032, 2, 16),   # Presidents' Day
    date(2032, 3, 26),   # Good Friday
    date(2032, 5, 31),   # Memorial Day
    date(2032, 6, 18),   # Juneteenth (observed)
    date(2032, 7, 5),    # Independence Day (observed)
    date(2032, 9, 6),    # Labor Day
    date(2032, 11, 25),  # Thanksgiving
    date(2032, 12, 24),  # Christmas (observed)
    date(2032, 12, 31),  # New Year 2033 (observed; Jan 1 2033 is Saturday)
    # 2033
    date(2033, 1, 17),   # MLK Day
    date(2033, 2, 21),   # Presidents' Day
    date(2033, 4, 15),   # Good Friday
    date(2033, 5, 30),   # Memorial Day
    date(2033, 6, 20),   # Juneteenth (observed)
    date(2033, 7, 4),    # Independence Day
    date(2033, 9, 5),    # Labor Day
    date(2033, 11, 24),  # Thanksgiving
    date(2033, 12, 26),  # Christmas (observed)
    # 2034
    date(2034, 1, 2),    # New Year's Day (observed)
    date(2034, 1, 16),   # MLK Day
    date(2034, 2, 20),   # Presidents' Day
    date(2034, 4, 7),    # Good Friday
    date(2034, 5, 29),   # Memorial Day
    date(2034, 6, 19),   # Juneteenth
    date(2034, 7, 4),    # Independence Day
    date(2034, 9, 4),    # Labor Day
    date(2034, 11, 23),  # Thanksgiving
    date(2034, 12, 25),  # Christmas
    # 2035
    date(2035, 1, 1),    # New Year's Day
    date(2035, 1, 15),   # MLK Day
    date(2035, 2, 19),   # Presidents' Day
    date(2035, 3, 23),   # Good Friday
    date(2035, 5, 28),   # Memorial Day
    date(2035, 6, 19),   # Juneteenth
    date(2035, 7, 4),    # Independence Day
    date(2035, 9, 3),    # Labor Day
    date(2035, 11, 22),  # Thanksgiving
    date(2035, 12, 25),  # Christmas
})

_LAST_STATIC_HOLIDAY_YEAR = 2035


def _check_holiday_list_freshness() -> None:
    """Log a warning if the static holiday list is nearing expiry."""
    import logging
    current_year = date.today().year
    if current_year >= _LAST_STATIC_HOLIDAY_YEAR - 1:
        logging.getLogger("backtestforecast.dates").warning(
            "Static holiday list expires in %d. Update utils/dates.py or ensure Redis holiday refresh is configured.",
            _LAST_STATIC_HOLIDAY_YEAR,
        )


_dynamic_holidays: frozenset[date] = frozenset()
_dynamic_holidays_loaded_at: float = 0.0
_dynamic_holidays_lock = threading.Lock()


_holiday_redis_client = None
_holiday_redis_lock = threading.Lock()


def _reset_holiday_redis() -> None:
    """Close the holiday Redis client so the next call reconnects."""
    global _holiday_redis_client
    with _holiday_redis_lock:
        client = _holiday_redis_client
        _holiday_redis_client = None
    if client is not None:
        try:
            client.close()
        except Exception:
            pass


def _get_redis_client():
    """Return a shared, lazily-initialised Redis client for holiday data."""
    global _holiday_redis_client
    if _holiday_redis_client is not None:
        return _holiday_redis_client
    with _holiday_redis_lock:
        if _holiday_redis_client is not None:
            return _holiday_redis_client
        from backtestforecast.config import get_settings, register_invalidation_callback
        import atexit
        import redis
        settings = get_settings()
        url = settings.redis_cache_url or settings.redis_url
        if not url:
            return None
        _holiday_redis_client = redis.Redis.from_url(
            url,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
        )
        atexit.register(_reset_holiday_redis)
        register_invalidation_callback(_reset_holiday_redis)
        return _holiday_redis_client


def _load_dynamic_holidays_from_redis() -> frozenset[date]:
    """Read holiday dates from the Redis cache.  Returns an empty set on any failure."""
    try:
        r = _get_redis_client()
        if r is None:
            return frozenset()
        raw_dates = r.smembers(REDIS_HOLIDAYS_KEY)
    except Exception:
        return frozenset()
    try:
        return frozenset(date.fromisoformat(d) for d in raw_dates if d)
    except (ValueError, TypeError):
        logger.warning("dates.holiday_parse_error", exc_info=True)
        return frozenset()


_dynamic_holidays_attempted: bool = False


def _get_dynamic_holidays() -> frozenset[date]:
    global _dynamic_holidays, _dynamic_holidays_loaded_at, _dynamic_holidays_attempted

    now = _time.monotonic()
    if _dynamic_holidays_attempted and now - _dynamic_holidays_loaded_at < _MEMORY_CACHE_TTL_SECONDS:
        return _dynamic_holidays

    with _dynamic_holidays_lock:
        now = _time.monotonic()
        if _dynamic_holidays_attempted and now - _dynamic_holidays_loaded_at < _MEMORY_CACHE_TTL_SECONDS:
            return _dynamic_holidays

        loaded = _load_dynamic_holidays_from_redis()
        _dynamic_holidays = loaded
        _dynamic_holidays_loaded_at = now
        _dynamic_holidays_attempted = True
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
    if not holidays:
        return 0

    r = _get_redis_client()
    if r is None:
        return 0
    pipe = r.pipeline(transaction=True)
    pipe.delete(REDIS_HOLIDAYS_KEY)
    pipe.sadd(REDIS_HOLIDAYS_KEY, *(d.isoformat() for d in holidays))
    pipe.expire(REDIS_HOLIDAYS_KEY, REDIS_HOLIDAYS_TTL_SECONDS)
    pipe.execute()
    logger.info("market_holidays.stored_in_redis", count=len(holidays))
    return len(holidays)


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
    max_iterations = 30
    for _ in range(max_iterations):
        if today.weekday() < 5 and today not in holidays:
            return today
        today -= timedelta(days=1)
    else:
        raise RuntimeError("Could not find a valid market date within 30 days")


_check_holiday_list_freshness()
