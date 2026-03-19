from __future__ import annotations

import json
import time
from datetime import date
from enum import Enum

import threading

import structlog
import redis

from backtestforecast.market_data.types import OptionContractRecord, OptionQuoteRecord
from backtestforecast.observability.metrics import CACHE_HITS_TOTAL, CACHE_MISSES_TOTAL

logger = structlog.get_logger("market_data.redis_cache")

_KEY_PREFIX = "bff:optcache"
_FRESHNESS_WARN_SECONDS = 86_400 * 3  # 3 days


class _CacheMiss(Enum):
    """Sentinel distinguishing a Redis miss from a cached None (no quote available)."""
    MISS = "MISS"


CACHE_MISS = _CacheMiss.MISS


def _contract_key(symbol: str, as_of: date, contract_type: str, exp_gte: date, exp_lte: date) -> str:
    return f"{_KEY_PREFIX}:contracts:{symbol}:{as_of.isoformat()}:{contract_type}:{exp_gte.isoformat()}:{exp_lte.isoformat()}"


def _quote_key(option_ticker: str, trade_date: date) -> str:
    return f"{_KEY_PREFIX}:quote:{option_ticker}:{trade_date.isoformat()}"


def _serialize_contracts(contracts: list[OptionContractRecord]) -> str:
    return json.dumps([
        {
            "ticker": c.ticker,
            "contract_type": c.contract_type,
            "expiration_date": c.expiration_date.isoformat(),
            "strike_price": c.strike_price,
            "shares_per_contract": c.shares_per_contract,
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
        self._lock = threading.Lock()

    def close(self) -> None:
        with self._lock:
            self._client = None
        self._pool.disconnect()

    def _conn(self) -> redis.Redis:
        if self._client is not None:
            return self._client
        with self._lock:
            if self._client is None:
                self._client = redis.Redis(connection_pool=self._pool)
            return self._client

    # -- contracts -----------------------------------------------------------

    def get_contracts(
        self,
        symbol: str,
        as_of_date: date,
        contract_type: str,
        exp_gte: date,
        exp_lte: date,
    ) -> list[OptionContractRecord] | None:
        key = _contract_key(symbol, as_of_date, contract_type, exp_gte, exp_lte)
        try:
            raw = self._conn().get(key)
            if raw is None:
                CACHE_MISSES_TOTAL.labels(cache="option_contracts").inc()
                return None
            CACHE_HITS_TOTAL.labels(cache="option_contracts").inc()
            return _deserialize_contracts(raw)
        except Exception:
            CACHE_MISSES_TOTAL.labels(cache="option_contracts").inc()
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
    ) -> None:
        try:
            key = _contract_key(symbol, as_of_date, contract_type, exp_gte, exp_lte)
            pipe = self._conn().pipeline(transaction=False)
            pipe.set(key, _serialize_contracts(contracts), ex=self._ttl)
            pipe.set(f"{key}:ts", str(int(time.time())), ex=self._ttl)
            pipe.execute()
            self.track_symbol_write(symbol)
        except Exception:
            logger.debug("redis_cache.set_contracts_failed", symbol=symbol, exc_info=True)

    # -- quotes --------------------------------------------------------------

    def get_quote(
        self,
        option_ticker: str,
        trade_date: date,
    ) -> OptionQuoteRecord | None | _CacheMiss:
        """Return the cached quote, ``None`` (cached negative), or ``CACHE_MISS``."""
        key = _quote_key(option_ticker, trade_date)
        try:
            raw = self._conn().get(key)
            if raw is None:
                CACHE_MISSES_TOTAL.labels(cache="option_quotes").inc()
                return CACHE_MISS
            CACHE_HITS_TOTAL.labels(cache="option_quotes").inc()
            return _deserialize_quote(raw)
        except Exception:
            CACHE_MISSES_TOTAL.labels(cache="option_quotes").inc()
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
    ) -> None:
        try:
            key = _quote_key(option_ticker, trade_date)
            pipe = self._conn().pipeline(transaction=False)
            pipe.set(key, _serialize_quote(quote), ex=self._ttl)
            pipe.set(f"{key}:ts", str(int(time.time())), ex=self._ttl)
            pipe.execute()
            symbol = option_ticker.split(":")[0] if ":" in option_ticker else option_ticker[:6]
            self.track_symbol_write(symbol)
        except Exception:
            logger.debug("redis_cache.set_quote_failed", ticker=option_ticker, exc_info=True)

    def get_cache_age_seconds(self, key: str) -> int | None:
        """Return the age of a cached entry in seconds, or None if no timestamp found."""
        try:
            ts_raw = self._conn().get(f"{key}:ts")
            if ts_raw is None:
                return None
            return int(time.time()) - int(ts_raw)
        except Exception:
            return None

    def _symbol_meta_key(self, symbol: str) -> str:
        return f"{_KEY_PREFIX}:meta:{symbol}"

    def track_symbol_write(self, symbol: str) -> None:
        """Record that a cache entry was written for *symbol*.

        Maintains a lightweight per-symbol hash with ``oldest_ts`` and
        ``entry_count`` so freshness checks avoid expensive SCAN operations.
        """
        now_s = str(int(time.time()))
        try:
            r = self._conn()
            key = self._symbol_meta_key(symbol)
            pipe = r.pipeline(transaction=False)
            pipe.hsetnx(key, "oldest_ts", now_s)
            pipe.hincrby(key, "entry_count", 1)
            pipe.hset(key, "latest_ts", now_s)
            pipe.expire(key, self._ttl + 3600)
            pipe.execute()
        except Exception:
            logger.debug("redis_cache.track_symbol_write_failed", symbol=symbol, exc_info=True)

    def get_oldest_cache_age_seconds(self, symbol: str) -> float | None:
        """Return age in seconds of the oldest cached entry for *symbol*.

        Uses the per-symbol metadata hash instead of SCAN. O(1).
        """
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
        try:
            r = self._conn()
            meta = r.hgetall(self._symbol_meta_key(symbol))
            if not meta:
                return info
            entry_count = int(meta.get("entry_count", 0))
            info["total_entries"] = entry_count
            oldest_ts = meta.get("oldest_ts")
            if oldest_ts is not None:
                age = time.time() - float(oldest_ts)
                if age > _FRESHNESS_WARN_SECONDS:
                    info["stale_entries"] = entry_count
                info["oldest_age_seconds"] = round(age, 1)
        except Exception:
            logger.debug("redis_cache.freshness_check_failed", symbol=symbol, exc_info=True)
        return info

    def invalidate_symbol(self, symbol: str) -> int:
        """Delete all cached data for *symbol*. Returns count of keys deleted.

        Uses the per-symbol metadata to find cached keys efficiently.
        Falls back to SCAN if metadata is unavailable.
        """
        deleted = 0
        try:
            r = self._conn()
            meta_key = self._symbol_meta_key(symbol)
            pattern = f"{_KEY_PREFIX}:*{symbol}*"
            cursor = 0
            while True:
                cursor, keys = r.scan(cursor, match=pattern, count=200)
                if keys:
                    r.delete(*keys)
                    deleted += len(keys)
                if cursor == 0:
                    break
            r.delete(meta_key)
            logger.info("redis_cache.symbol_invalidated", symbol=symbol, keys_deleted=deleted)
        except Exception:
            logger.warning("redis_cache.invalidate_failed", symbol=symbol, exc_info=True)
        return deleted

    def invalidate_all(self) -> int:
        """Delete ALL cached option data. Returns count of keys deleted.

        WARNING: This clears the entire cache. All subsequent requests will
        hit the upstream API until the cache is repopulated.
        """
        deleted = 0
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
        except Exception:
            logger.warning("redis_cache.invalidate_all_failed", exc_info=True)
        return deleted
