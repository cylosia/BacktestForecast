from __future__ import annotations

import base64
import math
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from uuid import UUID

DECIMAL_QUANT = Decimal("0.0001")


def to_decimal(value: float | Decimal | None, *, allow_infinite: bool = False) -> Decimal | None:
    """Convert a float or Decimal to a quantized Decimal.

    NaN returns ``None`` so that scan/sweep serialization does not crash
    on unexpected NaN values from the backtest engine.  Infinite values
    return ``None`` when *allow_infinite* is True (appropriate for
    metrics like profit_factor where infinity is a valid result meaning
    "no losses"), or raise ``ValueError`` when False (the default, for
    fields that must be finite).
    """
    if value is None:
        return None
    if isinstance(value, Decimal):
        if value.is_nan():
            return None
        if value.is_infinite():
            if allow_infinite:
                return None
            raise ValueError(f"Non-finite Decimal value: {value}")
        return value.quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)
    try:
        fval = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(fval):
        return None
    if math.isinf(fval):
        if allow_infinite:
            return None
        raise ValueError(f"Non-finite value: {value}")
    return Decimal(str(fval)).quantize(DECIMAL_QUANT, rounding=ROUND_HALF_UP)


def encode_cursor(dt: datetime, row_id: UUID) -> str:
    """Encode a datetime and primary key as an opaque, URL-safe pagination cursor."""
    raw = f"{dt.isoformat()}|{row_id}"
    return base64.urlsafe_b64encode(raw.encode()).decode()


def decode_cursor(cursor: str) -> tuple[datetime, UUID] | None:
    """Decode a pagination cursor back to ``(created_at, id)``. Returns None on invalid input."""
    try:
        raw = base64.urlsafe_b64decode(cursor.encode()).decode()
        created_at_raw, row_id_raw = raw.split("|", 1)
        return datetime.fromisoformat(created_at_raw), UUID(row_id_raw)
    except (ValueError, UnicodeDecodeError, Exception):
        return None


def create_cache_redis(
    *,
    decode_responses: bool = True,
    socket_timeout: float = 5.0,
    socket_connect_timeout: float = 3.0,
):
    """Create a Redis client connected to the cache/ops Redis (not the Celery broker).

    Centralises the connection parameters that were previously scattered
    across worker tasks, health checks, and utility functions.  Always
    uses ``redis_cache_url`` from settings - never ``redis_url`` (the
    broker).

    Callers are responsible for closing the returned client.
    """
    from redis import Redis

    from backtestforecast.config import get_settings

    return Redis.from_url(
        get_settings().redis_cache_url,
        decode_responses=decode_responses,
        socket_timeout=socket_timeout,
        socket_connect_timeout=socket_connect_timeout,
    )
