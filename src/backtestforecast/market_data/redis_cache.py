from __future__ import annotations

import contextlib
import json
import threading
import time
from datetime import date
from enum import Enum

import redis
import structlog

from backtestforecast.market_data.types import OptionContractRecord, OptionQuoteRecord

logger = structlog.get_logger("market_data.redis_cache")

_KEY_PREFIX = "bff:optcache"
_FRESHNESS_WARN_SECONDS = 86_400 * 3  # 3 days
_NEGATIVE_CACHE_TTL_SECONDS = 300


class _CacheMiss(Enum):
    """Sentinel distinguishing a Redis miss from a cached None (no quote available)."""
    MISS = "MISS"


CACHE_MISS = _CacheMiss.MISS


def _contract_key(symbol: str, as_of: date, contract_type: str, exp_gte: date, exp_lte: date) -> str:
    return f"{_KEY_PREFIX}:contracts:{symbol}:{as_of.isoformat()}:{contract_type}:{exp_gte.isoformat()}:{exp_lte.isoformat()}"


def _quote_key(option_ticker: str, trade_date: date) -> str:
    return f"{_KEY_PREFIX}:quote:{option_ticker}:{trade_date.isoformat()}"


def _exact_contract_key(
    symbol: str,
    as_of: date,
    contract_type: str,
    expiration_date: date,
    strike_gte: float | None,
    strike_lte: float | None,
) -> str:
    strike_floor = "any" if strike_gte is None else f"{strike_gte:.4f}"
    strike_ceiling = "any" if strike_lte is None else f"{strike_lte:.4f}"
    return (
        f"{_KEY_PREFIX}:contracts_exact:{symbol}:{as_of.isoformat()}:{contract_type}:"
        f"{expiration_date.isoformat()}:{strike_floor}:{strike_ceiling}"
    )


def _ex_dividend_key(symbol: str, start_date: date, end_date: date) -> str:
    return f"{_KEY_PREFIX}:exdiv:{symbol}:{start_date.isoformat()}:{end_date.isoformat()}"


def _serialize_contracts(contracts: list[OptionContractRecord]) -> str:
    return json.dumps([
        {
            "ticker": c.ticker,
            "contract_type": c.contract_type,
            "expiration_date": c.expiration_date.isoformat(),
            "strike_price": c.strike_price,
            "shares_per_contract": c.shares_per_contract,
            "underlying_symbol": c.underlying_symbol,
            "as_of_mid_price": c.as_of_mid_price,
        }
        for c in contracts
    ])


def _deserialize_contracts(raw: str) -> list[OptionContractRecord]:
    rows = json.loads(raw)
    return [
        OptionContractRecord(
            ticker=r["ticker"],
            contract_type=r["contract_type"],
            expiration_date=date.fromisoformat(r["expiration_date"]),
            strike_price=r["strike_price"],
            shares_per_contract=r["shares_per_contract"],
            underlying_symbol=r.get("underlying_symbol"),
            as_of_mid_price=r.get("as_of_mid_price"),
        )
        for r in rows
    ]


def _serialize_quote(quote: OptionQuoteRecord | None) -> str:
    if quote is None:
        return json.dumps({"_null": True})
    return json.dumps({
        "trade_date": quote.trade_date.isoformat(),
        "bid_price": quote.bid_price,
        "ask_price": quote.ask_price,
        "participant_timestamp": quote.participant_timestamp,
        "source_option_ticker": quote.source_option_ticker,
        "deliverable_shares_per_contract": quote.deliverable_shares_per_contract,
    })


def _deserialize_quote(raw: str) -> OptionQuoteRecord | None:
    data = json.loads(raw)
    if data.get("_null"):
        return None
    return OptionQuoteRecord(
        trade_date=date.fromisoformat(data["trade_date"]),
        bid_price=data["bid_price"],
        ask_price=data["ask_price"],
        participant_timestamp=data.get("participant_timestamp"),
        source_option_ticker=data.get("source_option_ticker"),
        deliverable_shares_per_contract=data.get("deliverable_shares_per_contract"),
    )


def _serialize_ex_dividend_dates(dates: set[date], *, degraded: bool) -> str:
    return json.dumps(
        {
            "dates": sorted(item.isoformat() for item in dates),
            "degraded": degraded,
        }
    )


def _deserialize_ex_dividend_dates(raw: str) -> tuple[set[date], bool]:
    data = json.loads(raw)
    return (
        {date.fromisoformat(item) for item in data.get("dates", [])},
        bool(data.get("degraded", False)),
    )


class OptionDataRedisCache:
    """Redis-backed shared cache for option contract and quote data.

    Sits between the in-memory LRU caches in ``MassiveOptionGateway`` and the
    Massive API.  All operations degrade gracefully: a Redis failure is logged
    and treated as a cache miss so the gateway falls through to the API.
    """

    def __init__(self, redis_url: str, ttl_seconds: int = 604_800) -> None:
        self._pool = redis.ConnectionPool.from_url(
            redis_url,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
        )
        self._ttl = ttl_seconds
        self._client: redis.Redis | None = None
        self._closed = False
        self._disabled = False
        self._disabled_reason: str | None = None
        self._lock = threading.Lock()

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._disabled = True
            self._client = None
            self._pool.disconnect()

    def _conn(self) -> redis.Redis:
        if getattr(self, "_disabled", False):
            raise RuntimeError(
                f"OptionDataRedisCache is disabled: {getattr(self, '_disabled_reason', 'unavailable')}"
            )
        client = self._client
        if client is not None:
            return client
        with self._lock:
            if self._closed:
                raise RuntimeError("OptionDataRedisCache is closed")
            if self._client is None:
                self._client = redis.Redis(connection_pool=self._pool)
            return self._client

    def ping(self) -> bool:
        if getattr(self, "_disabled", False):
            return False
        try:
            return bool(self._conn().ping())
        except Exception as exc:
            self._disable_for_exception(exc, operation="ping")
            return False

    def _disable_for_exception(self, exc: Exception, *, operation: str) -> bool:
        if not self._should_disable_for_exception(exc):
            return False
        lock = getattr(self, "_lock", None)
        context = lock if lock is not None else contextlib.nullcontext()
        with context:
            if getattr(self, "_disabled", False):
                return True
            self._disabled = True
            self._disabled_reason = f"{operation}:{type(exc).__name__}"
            self._client = None
            pool = getattr(self, "_pool", None)
            if pool is not None:
                with contextlib.suppress(Exception):
                    pool.disconnect()
        logger.warning(
            "redis_cache.disabled",
            operation=operation,
            exc_type=type(exc).__name__,
            reason=str(exc),
        )
        return True

    @staticmethod
    def _should_disable_for_exception(exc: Exception) -> bool:
        if isinstance(exc, redis.exceptions.AuthenticationError):
            return True
        if isinstance(exc, redis.exceptions.ResponseError):
            message = str(exc).upper()
            if "NOAUTH" in message or "AUTH" in message:
                return True
        return False

    # -- contracts -----------------------------------------------------------

    def get_contracts(
        self,
        symbol: str,
        as_of_date: date,
        contract_type: str,
        exp_gte: date,
        exp_lte: date,
    ) -> list[OptionContractRecord] | None:
        if getattr(self, "_disabled", False):
            return None
        key = _contract_key(symbol, as_of_date, contract_type, exp_gte, exp_lte)
        try:
            raw = self._conn().get(key)
            if raw is None:
                return None
            return _deserialize_contracts(raw)
        except Exception as exc:
            if self._disable_for_exception(exc, operation="get_contracts"):
                return None
            logger.debug("redis_cache.get_contracts_failed", symbol=symbol, exc_info=True)
            try:
                self._conn().delete(key)
            except Exception:
                logger.debug("redis_cache.delete_corrupted_failed", key=key, exc_info=True)
            return None

    def set_contracts(
        self,
        symbol: str,
        as_of_date: date,
        contract_type: str,
        exp_gte: date,
        exp_lte: date,
        contracts: list[OptionContractRecord],
        *,
        ttl_seconds: int | None = None,
    ) -> None:
        if getattr(self, "_disabled", False):
            return
        try:
            key = _contract_key(symbol, as_of_date, contract_type, exp_gte, exp_lte)
            pipe = self._conn().pipeline(transaction=False)
            ttl = ttl_seconds if ttl_seconds is not None else self._ttl
            pipe.set(key, _serialize_contracts(contracts), ex=ttl)
            pipe.set(f"{key}:ts", str(int(time.time())), ex=ttl)
            pipe.execute()
            self.track_symbol_write(symbol, cache_key=key)
        except Exception as exc:
            if self._disable_for_exception(exc, operation="set_contracts"):
                return
            logger.debug("redis_cache.set_contracts_failed", symbol=symbol, exc_info=True)

    def get_exact_contracts(
        self,
        symbol: str,
        as_of_date: date,
        contract_type: str,
        expiration_date: date,
        strike_price_gte: float | None = None,
        strike_price_lte: float | None = None,
    ) -> list[OptionContractRecord] | None:
        if getattr(self, "_disabled", False):
            return None
        key = _exact_contract_key(
            symbol,
            as_of_date,
            contract_type,
            expiration_date,
            strike_price_gte,
            strike_price_lte,
        )
        try:
            raw = self._conn().get(key)
            if raw is None:
                return None
            return _deserialize_contracts(raw)
        except Exception as exc:
            if self._disable_for_exception(exc, operation="get_exact_contracts"):
                return None
            logger.debug("redis_cache.get_exact_contracts_failed", symbol=symbol, exc_info=True)
            try:
                self._conn().delete(key)
            except Exception:
                logger.debug("redis_cache.delete_corrupted_failed", key=key, exc_info=True)
            return None

    def set_exact_contracts(
        self,
        symbol: str,
        as_of_date: date,
        contract_type: str,
        expiration_date: date,
        contracts: list[OptionContractRecord],
        *,
        strike_price_gte: float | None = None,
        strike_price_lte: float | None = None,
        ttl_seconds: int | None = None,
    ) -> None:
        if getattr(self, "_disabled", False):
            return
        try:
            key = _exact_contract_key(
                symbol,
                as_of_date,
                contract_type,
                expiration_date,
                strike_price_gte,
                strike_price_lte,
            )
            pipe = self._conn().pipeline(transaction=False)
            ttl = ttl_seconds if ttl_seconds is not None else self._ttl
            pipe.set(key, _serialize_contracts(contracts), ex=ttl)
            pipe.set(f"{key}:ts", str(int(time.time())), ex=ttl)
            pipe.execute()
            self.track_symbol_write(symbol, cache_key=key)
        except Exception as exc:
            if self._disable_for_exception(exc, operation="set_exact_contracts"):
                return
            logger.debug("redis_cache.set_exact_contracts_failed", symbol=symbol, exc_info=True)

    # -- quotes --------------------------------------------------------------

    def get_quote(
        self,
        option_ticker: str,
        trade_date: date,
    ) -> OptionQuoteRecord | None | _CacheMiss:
        """Return the cached quote, ``None`` (cached negative), or ``CACHE_MISS``."""
        if getattr(self, "_disabled", False):
            return CACHE_MISS
        key = _quote_key(option_ticker, trade_date)
        try:
            raw = self._conn().get(key)
            if raw is None:
                return CACHE_MISS
            return _deserialize_quote(raw)
        except Exception as exc:
            if self._disable_for_exception(exc, operation="get_quote"):
                return CACHE_MISS
            logger.debug("redis_cache.get_quote_failed", ticker=option_ticker, exc_info=True)
            try:
                self._conn().delete(key)
            except Exception:
                logger.debug("redis_cache.delete_corrupted_failed", key=key, exc_info=True)
            return CACHE_MISS

    def set_quote(
        self,
        option_ticker: str,
        trade_date: date,
        quote: OptionQuoteRecord | None,
        *,
        ttl_seconds: int | None = None,
    ) -> None:
        if getattr(self, "_disabled", False):
            return
        try:
            key = _quote_key(option_ticker, trade_date)
            pipe = self._conn().pipeline(transaction=False)
            ttl = ttl_seconds if ttl_seconds is not None else self._ttl
            pipe.set(key, _serialize_quote(quote), ex=ttl)
            pipe.set(f"{key}:ts", str(int(time.time())), ex=ttl)
            pipe.execute()
            if ":" in option_ticker:
                symbol = option_ticker.split(":")[0]
            else:
                import re
                m = re.match(r"^([A-Z.^/]+)", option_ticker)
                if m:
                    symbol = m.group(1)
                else:
                    symbol = option_ticker[:6]
                    logger.warning(
                        "redis_cache.symbol_extraction_fallback",
                        option_ticker=option_ticker,
                        fallback_symbol=symbol,
                    )
            self.track_symbol_write(symbol, cache_key=key)
        except Exception as exc:
            if self._disable_for_exception(exc, operation="set_quote"):
                return
            logger.debug("redis_cache.set_quote_failed", ticker=option_ticker, exc_info=True)

    # -- ex-dividend dates ---------------------------------------------------

    def get_ex_dividend_dates(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> tuple[set[date], bool] | _CacheMiss:
        if getattr(self, "_disabled", False):
            return CACHE_MISS
        key = _ex_dividend_key(symbol, start_date, end_date)
        try:
            raw = self._conn().get(key)
            if raw is None:
                return CACHE_MISS
            return _deserialize_ex_dividend_dates(raw)
        except Exception as exc:
            if self._disable_for_exception(exc, operation="get_ex_dividend_dates"):
                return CACHE_MISS
            logger.debug("redis_cache.get_ex_dividend_dates_failed", symbol=symbol, exc_info=True)
            try:
                self._conn().delete(key)
            except Exception:
                logger.debug("redis_cache.delete_corrupted_failed", key=key, exc_info=True)
            return CACHE_MISS

    def set_ex_dividend_dates(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        dates: set[date],
        *,
        degraded: bool = False,
        ttl_seconds: int | None = None,
    ) -> None:
        if getattr(self, "_disabled", False):
            return
        try:
            key = _ex_dividend_key(symbol, start_date, end_date)
            ttl = ttl_seconds if ttl_seconds is not None else self._ttl
            pipe = self._conn().pipeline(transaction=False)
            pipe.set(key, _serialize_ex_dividend_dates(dates, degraded=degraded), ex=ttl)
            pipe.execute()
        except Exception as exc:
            if self._disable_for_exception(exc, operation="set_ex_dividend_dates"):
                return
            logger.debug("redis_cache.set_ex_dividend_dates_failed", symbol=symbol, exc_info=True)

    def _symbol_meta_key(self, symbol: str) -> str:
        return f"{_KEY_PREFIX}:meta:{symbol}"

    def _symbol_keys_set(self, symbol: str) -> str:
        return f"{_KEY_PREFIX}:keys:{symbol}"

    def track_symbol_write(self, symbol: str, cache_key: str | None = None) -> None:
        """Record that a cache entry was written for *symbol*.

        Maintains a lightweight per-symbol hash with ``oldest_ts`` and
        ``entry_count`` so freshness checks avoid expensive SCAN operations.
        When *cache_key* is provided, also tracks the key in a per-symbol SET
        so ``invalidate_symbol`` can delete by set membership (O(M)) instead
        of SCAN (O(N)).
        """
        if getattr(self, "_disabled", False):
            return
        now_s = str(int(time.time()))
        try:
            r = self._conn()
            meta_key = self._symbol_meta_key(symbol)
            pipe = r.pipeline(transaction=False)
            pipe.hsetnx(meta_key, "oldest_ts", now_s)
            pipe.hincrby(meta_key, "entry_count", 1)
            pipe.hset(meta_key, "latest_ts", now_s)
            pipe.expire(meta_key, self._ttl + 3600)
            if cache_key is not None:
                keys_set = self._symbol_keys_set(symbol)
                pipe.sadd(keys_set, cache_key, f"{cache_key}:ts")
                pipe.expire(keys_set, self._ttl + 3600)
            pipe.execute()
        except Exception as exc:
            if self._disable_for_exception(exc, operation="track_symbol_write"):
                return
            logger.debug("redis_cache.track_symbol_write_failed", symbol=symbol, exc_info=True)

    def get_oldest_cache_age_seconds(self, symbol: str) -> float | None:
        """Return age in seconds of the oldest cached entry for *symbol*.

        Uses the per-symbol metadata hash instead of SCAN. O(1).
        """
        if getattr(self, "_disabled", False):
            return None
        try:
            r = self._conn()
            ts_raw = r.hget(self._symbol_meta_key(symbol), "oldest_ts")
            if ts_raw is None:
                return None
            return time.time() - float(ts_raw)
        except Exception:
            return None

    def check_freshness(self, symbol: str) -> dict[str, object]:
        """Return freshness info for a symbol's cached data.

        Uses the per-symbol metadata hash instead of SCAN. O(1).
        """
        info: dict[str, object] = {"symbol": symbol, "stale_entries": 0, "total_entries": 0}
        if getattr(self, "_disabled", False):
            return info
        try:
            r = self._conn()
            meta = r.hgetall(self._symbol_meta_key(symbol))
            if not meta:
                return info
            entry_count = int(meta.get("entry_count", 0))
            info["total_entries"] = entry_count
            oldest_ts = meta.get("oldest_ts")
            newest_ts = meta.get("newest_ts")
            if oldest_ts is not None:
                oldest_age = time.time() - float(oldest_ts)
                if oldest_age > _FRESHNESS_WARN_SECONDS:
                    if newest_ts is not None:
                        newest_age = time.time() - float(newest_ts)
                        if newest_age > _FRESHNESS_WARN_SECONDS:
                            info["stale_entries"] = entry_count
                        else:
                            info["stale_entries"] = "partial"
                    else:
                        info["stale_entries"] = entry_count
                info["oldest_age_seconds"] = round(oldest_age, 1)
        except Exception as exc:
            if self._disable_for_exception(exc, operation="check_freshness"):
                return info
            logger.debug("redis_cache.freshness_check_failed", symbol=symbol, exc_info=True)
        return info

    def invalidate_symbol(self, symbol: str) -> int:
        """Delete all cached data for *symbol*. Returns count of keys deleted.

        Uses the per-symbol key set for O(M) deletion (M = keys for this
        symbol) instead of O(N) SCAN over all Redis keys.  Falls back to
        SCAN if the key set is empty (e.g. entries written before this
        tracking was added).
        """
        deleted = 0
        if getattr(self, "_disabled", False):
            return deleted
        try:
            r = self._conn()
            meta_key = self._symbol_meta_key(symbol)
            keys_set = self._symbol_keys_set(symbol)

            tracked_keys = r.smembers(keys_set)
            if tracked_keys:
                batch: list[str | bytes] = list(tracked_keys)
                batch.append(keys_set)
                batch.append(meta_key)
                r.delete(*batch)
                deleted = len(tracked_keys)
            else:
                escaped_symbol = symbol.replace("*", "\\*").replace("?", "\\?").replace("[", "\\[").replace("]", "\\]")
                pattern = f"{_KEY_PREFIX}:*:{escaped_symbol}:*"
                cursor = 0
                while True:
                    cursor, keys = r.scan(cursor, match=pattern, count=200)
                    if keys:
                        r.delete(*keys)
                        deleted += len(keys)
                    if cursor == 0:
                        break
                r.delete(meta_key, keys_set)
            logger.info("redis_cache.symbol_invalidated", symbol=symbol, keys_deleted=deleted)
        except Exception as exc:
            if self._disable_for_exception(exc, operation="invalidate_symbol"):
                return deleted
            logger.warning("redis_cache.invalidate_failed", symbol=symbol, exc_info=True)
        return deleted

    def invalidate_all(self) -> int:
        """Delete ALL cached option data. Returns count of keys deleted.

        WARNING: This clears the entire cache. All subsequent requests will
        hit the upstream API until the cache is repopulated.
        """
        deleted = 0
        if getattr(self, "_disabled", False):
            return deleted
        try:
            r = self._conn()
            pattern = f"{_KEY_PREFIX}:*"
            cursor = 0
            while True:
                cursor, keys = r.scan(cursor, match=pattern, count=500)
                if keys:
                    r.delete(*keys)
                    deleted += len(keys)
                if cursor == 0:
                    break
            logger.warning("redis_cache.all_invalidated", keys_deleted=deleted)
        except Exception as exc:
            if self._disable_for_exception(exc, operation="invalidate_all"):
                return deleted
            logger.warning("redis_cache.invalidate_all_failed", exc_info=True)
        return deleted
