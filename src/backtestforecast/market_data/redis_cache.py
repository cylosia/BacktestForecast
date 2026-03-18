from __future__ import annotations

import json
from datetime import date
from enum import Enum

import structlog
import redis

from backtestforecast.market_data.types import OptionContractRecord, OptionQuoteRecord

logger = structlog.get_logger("market_data.redis_cache")

_KEY_PREFIX = "bff:optcache"


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
        self._pool = redis.ConnectionPool.from_url(redis_url, decode_responses=True)
        self._ttl = ttl_seconds
        self._client: redis.Redis | None = None

    def close(self) -> None:
        self._pool.disconnect()

    def _conn(self) -> redis.Redis:
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
        try:
            raw = self._conn().get(_contract_key(symbol, as_of_date, contract_type, exp_gte, exp_lte))
            if raw is None:
                return None
            return _deserialize_contracts(raw)
        except Exception:
            logger.debug("redis_cache.get_contracts_failed", symbol=symbol, exc_info=True)
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
            self._conn().set(key, _serialize_contracts(contracts), ex=self._ttl)
        except Exception:
            logger.debug("redis_cache.set_contracts_failed", symbol=symbol, exc_info=True)

    # -- quotes --------------------------------------------------------------

    def get_quote(
        self,
        option_ticker: str,
        trade_date: date,
    ) -> OptionQuoteRecord | None | _CacheMiss:
        """Return the cached quote, ``None`` (cached negative), or ``CACHE_MISS``."""
        try:
            raw = self._conn().get(_quote_key(option_ticker, trade_date))
            if raw is None:
                return CACHE_MISS
            return _deserialize_quote(raw)
        except Exception:
            logger.debug("redis_cache.get_quote_failed", ticker=option_ticker, exc_info=True)
            return CACHE_MISS

    def set_quote(
        self,
        option_ticker: str,
        trade_date: date,
        quote: OptionQuoteRecord | None,
    ) -> None:
        try:
            key = _quote_key(option_ticker, trade_date)
            self._conn().set(key, _serialize_quote(quote), ex=self._ttl)
        except Exception:
            logger.debug("redis_cache.set_quote_failed", ticker=option_ticker, exc_info=True)
