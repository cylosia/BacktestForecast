"""Tests for the Redis option data cache, gateway integration, prefetcher,
and sweep schema validation."""
from __future__ import annotations

import threading
import time
from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from backtestforecast.market_data.types import (
    DailyBar,
    OptionContractRecord,
    OptionQuoteRecord,
)

# ---------------------------------------------------------------------------
# Redis cache serialization round-trip
# ---------------------------------------------------------------------------

class TestOptionDataRedisCacheSerialization:
    def test_contract_serialization_round_trip(self):
        from backtestforecast.market_data.redis_cache import (
            _deserialize_contracts,
            _serialize_contracts,
        )

        contracts = [
            OptionContractRecord(
                ticker="O:TSLA250321P00250000",
                contract_type="put",
                expiration_date=date(2025, 3, 21),
                strike_price=250.0,
                shares_per_contract=100.0,
            ),
            OptionContractRecord(
                ticker="O:TSLA250321P00260000",
                contract_type="put",
                expiration_date=date(2025, 3, 21),
                strike_price=260.0,
                shares_per_contract=100.0,
            ),
        ]

        raw = _serialize_contracts(contracts)
        restored = _deserialize_contracts(raw)

        assert len(restored) == 2
        assert restored[0].ticker == "O:TSLA250321P00250000"
        assert restored[0].strike_price == 250.0
        assert restored[0].expiration_date == date(2025, 3, 21)
        assert restored[1].ticker == "O:TSLA250321P00260000"

    def test_quote_serialization_round_trip(self):
        from backtestforecast.market_data.redis_cache import (
            _deserialize_quote,
            _serialize_quote,
        )

        quote = OptionQuoteRecord(
            trade_date=date(2025, 3, 17),
            bid_price=2.50,
            ask_price=2.70,
            participant_timestamp=1710700000,
        )

        raw = _serialize_quote(quote)
        restored = _deserialize_quote(raw)

        assert restored is not None
        assert restored.bid_price == 2.50
        assert restored.ask_price == 2.70
        assert restored.trade_date == date(2025, 3, 17)
        assert restored.participant_timestamp == 1710700000

    def test_null_quote_serialization(self):
        from backtestforecast.market_data.redis_cache import (
            _deserialize_quote,
            _serialize_quote,
        )

        raw = _serialize_quote(None)
        restored = _deserialize_quote(raw)
        assert restored is None

    def test_empty_contracts_list(self):
        from backtestforecast.market_data.redis_cache import (
            _deserialize_contracts,
            _serialize_contracts,
        )

        raw = _serialize_contracts([])
        restored = _deserialize_contracts(raw)
        assert restored == []


# ---------------------------------------------------------------------------
# Redis cache graceful degradation
# ---------------------------------------------------------------------------

class TestOptionDataRedisCacheGracefulDegradation:
    def test_get_contracts_returns_none_on_redis_error(self):
        from backtestforecast.market_data.redis_cache import OptionDataRedisCache

        cache = OptionDataRedisCache.__new__(OptionDataRedisCache)
        cache._ttl = 600

        mock_conn = MagicMock()
        mock_conn.get.side_effect = ConnectionError("Redis down")
        cache._conn = MagicMock(return_value=mock_conn)

        result = cache.get_contracts("TSLA", date(2025, 1, 1), "put", date(2025, 1, 5), date(2025, 1, 15))
        assert result is None

    def test_auth_error_disables_cache_after_first_failure(self):
        import redis

        from backtestforecast.market_data.redis_cache import OptionDataRedisCache

        cache = OptionDataRedisCache.__new__(OptionDataRedisCache)
        cache._ttl = 600
        cache._disabled = False
        cache._disabled_reason = None
        cache._client = None
        cache._pool = MagicMock()
        cache._lock = threading.Lock()

        mock_conn = MagicMock()
        mock_conn.get.side_effect = redis.exceptions.AuthenticationError("AUTH called without any password configured")
        cache._conn = MagicMock(return_value=mock_conn)

        first = cache.get_contracts("TSLA", date(2025, 1, 1), "put", date(2025, 1, 5), date(2025, 1, 15))
        second = cache.get_contracts("TSLA", date(2025, 1, 1), "put", date(2025, 1, 5), date(2025, 1, 15))

        assert first is None
        assert second is None
        assert cache._disabled is True
        assert mock_conn.get.call_count == 1

    def test_set_contracts_does_not_raise_on_redis_error(self):
        from backtestforecast.market_data.redis_cache import OptionDataRedisCache

        cache = OptionDataRedisCache.__new__(OptionDataRedisCache)
        cache._ttl = 600

        mock_conn = MagicMock()
        mock_conn.set.side_effect = ConnectionError("Redis down")
        cache._conn = MagicMock(return_value=mock_conn)

        cache.set_contracts("TSLA", date(2025, 1, 1), "put", date(2025, 1, 5), date(2025, 1, 15), [])

    def test_get_quote_returns_cache_miss_on_redis_error(self):
        from backtestforecast.market_data.redis_cache import (
            CACHE_MISS,
            OptionDataRedisCache,
        )

        cache = OptionDataRedisCache.__new__(OptionDataRedisCache)
        cache._ttl = 600

        mock_conn = MagicMock()
        mock_conn.get.side_effect = ConnectionError("Redis down")
        cache._conn = MagicMock(return_value=mock_conn)

        result = cache.get_quote("O:TSLA250321P00250000", date(2025, 3, 17))
        assert result is CACHE_MISS


# ---------------------------------------------------------------------------
# Gateway 3-tier lookup
# ---------------------------------------------------------------------------

class TestGatewayThreeTierLookup:
    def _make_gateway(self, redis_cache=None):
        from backtestforecast.market_data.service import MassiveOptionGateway

        mock_client = MagicMock()
        return MassiveOptionGateway(mock_client, "TSLA", redis_cache=redis_cache)

    def test_list_contracts_hits_redis_before_api(self):
        from backtestforecast.market_data.redis_cache import OptionDataRedisCache

        cached_contracts = [
            OptionContractRecord("O:TEST", "put", date(2025, 3, 21), 250.0, 100.0),
        ]

        redis_cache = MagicMock(spec=OptionDataRedisCache)
        redis_cache.get_contracts.return_value = cached_contracts

        gw = self._make_gateway(redis_cache=redis_cache)
        result = gw.list_contracts(date(2025, 3, 14), "put", 8, 2)

        assert len(result) == 1
        assert result[0].ticker == "O:TEST"
        redis_cache.get_contracts.assert_called_once()
        gw.client.list_option_contracts.assert_not_called()

    def test_list_contracts_falls_through_to_api_on_redis_miss(self):
        from backtestforecast.market_data.redis_cache import OptionDataRedisCache

        redis_cache = MagicMock(spec=OptionDataRedisCache)
        redis_cache.get_contracts.return_value = None

        api_contracts = [
            OptionContractRecord("O:API", "put", date(2025, 3, 21), 250.0, 100.0),
        ]

        gw = self._make_gateway(redis_cache=redis_cache)
        gw.client.list_option_contracts.return_value = api_contracts

        result = gw.list_contracts(date(2025, 3, 14), "put", 8, 2)

        assert len(result) == 1
        assert result[0].ticker == "O:API"
        redis_cache.set_contracts.assert_called_once()

    def test_get_quote_hits_redis_before_api(self):
        from backtestforecast.market_data.redis_cache import OptionDataRedisCache

        cached_quote = OptionQuoteRecord(date(2025, 3, 14), 2.5, 2.7, None)

        redis_cache = MagicMock(spec=OptionDataRedisCache)
        redis_cache.get_quote.return_value = cached_quote

        gw = self._make_gateway(redis_cache=redis_cache)
        result = gw.get_quote("O:TEST", date(2025, 3, 14))

        assert result is not None
        assert result.bid_price == 2.5
        gw.client.get_option_quote_for_date.assert_not_called()

    def test_get_quote_caches_api_none_in_redis(self):
        from backtestforecast.market_data.redis_cache import (
            CACHE_MISS,
            OptionDataRedisCache,
        )

        redis_cache = MagicMock(spec=OptionDataRedisCache)
        redis_cache.get_quote.return_value = CACHE_MISS

        gw = self._make_gateway(redis_cache=redis_cache)
        gw.client.get_option_quote_for_date.return_value = None

        result = gw.get_quote("O:TEST", date(2025, 3, 14))

        assert result is None
        redis_cache.set_quote.assert_called_once()
        args, kwargs = redis_cache.set_quote.call_args
        assert args == ("O:TEST", date(2025, 3, 14), None)
        assert kwargs["ttl_seconds"] is not None

    def test_no_redis_falls_through_to_api(self):
        gw = self._make_gateway(redis_cache=None)
        api_quote = OptionQuoteRecord(date(2025, 3, 14), 3.0, 3.2, None)
        gw.client.get_option_quote_for_date.return_value = api_quote

        result = gw.get_quote("O:TEST", date(2025, 3, 14))
        assert result is not None
        assert result.bid_price == 3.0

    def test_in_memory_cache_takes_priority(self):
        from backtestforecast.market_data.redis_cache import OptionDataRedisCache

        redis_cache = MagicMock(spec=OptionDataRedisCache)

        gw = self._make_gateway(redis_cache=redis_cache)
        # Seed the in-memory cache
        cached_quote = OptionQuoteRecord(date(2025, 3, 14), 1.0, 1.2, None)
        gw._quote_cache[("O:TEST", date(2025, 3, 14))] = cached_quote

        result = gw.get_quote("O:TEST", date(2025, 3, 14))
        assert result is not None
        assert result.bid_price == 1.0
        redis_cache.get_quote.assert_not_called()
        gw.client.get_option_quote_for_date.assert_not_called()

    def test_concurrent_list_contracts_coalesces_provider_calls(self):
        gw = self._make_gateway(redis_cache=None)

        contracts = [OptionContractRecord("O:API", "put", date(2025, 3, 21), 250.0, 100.0)]

        def fetch_contracts(**kwargs):
            time.sleep(0.05)
            return contracts

        gw.client.list_option_contracts.side_effect = fetch_contracts
        results: list[list[OptionContractRecord]] = []

        def worker() -> None:
            results.append(gw.list_contracts(date(2025, 3, 14), "put", 8, 2))

        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start()
        t2.start()
        t1.join(timeout=2)
        t2.join(timeout=2)

        assert len(results) == 2
        assert gw.client.list_option_contracts.call_count == 1

    def test_preferred_expiration_queries_exact_dates_in_priority_order(self):
        gw = self._make_gateway(redis_cache=None)
        target_contracts = [
            OptionContractRecord("O:API", "call", date(2025, 4, 7), 250.0, 100.0),
        ]

        def fetch_exact(**kwargs):
            if kwargs["expiration_date"] == date(2025, 4, 8):
                return []
            if kwargs["expiration_date"] == date(2025, 4, 9):
                return []
            if kwargs["expiration_date"] == date(2025, 4, 7):
                return target_contracts
            raise AssertionError(f"unexpected expiration probe {kwargs['expiration_date']}")

        gw.client.list_option_contracts_for_expiration.side_effect = fetch_exact

        result = gw.list_contracts_for_preferred_expiration(
            entry_date=date(2025, 4, 1),
            contract_type="call",
            target_dte=7,
            dte_tolerance_days=2,
        )

        assert result == target_contracts
        assert [
            call.kwargs["expiration_date"]
            for call in gw.client.list_option_contracts_for_expiration.call_args_list
        ] == [date(2025, 4, 8), date(2025, 4, 9), date(2025, 4, 7)]

    def test_concurrent_get_quote_coalesces_provider_calls(self):
        gw = self._make_gateway(redis_cache=None)
        quote = OptionQuoteRecord(date(2025, 3, 14), 3.0, 3.2, None)

        def fetch_quote(*args, **kwargs):
            time.sleep(0.05)
            return quote

        gw.client.get_option_quote_for_date.side_effect = fetch_quote
        results: list[OptionQuoteRecord | None] = []

        def worker() -> None:
            results.append(gw.get_quote("O:TEST", date(2025, 3, 14)))

        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start()
        t2.start()
        t1.join(timeout=2)
        t2.join(timeout=2)

        assert len(results) == 2
        assert all(r is not None for r in results)
        assert gw.client.get_option_quote_for_date.call_count == 1


# ---------------------------------------------------------------------------
# Historical gateway run-scoped cache
# ---------------------------------------------------------------------------

class TestHistoricalGatewayRunScopedCache:
    def test_list_contracts_hits_store_once_per_key(self):
        from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway

        store = MagicMock()
        contracts = [
            OptionContractRecord("O:TEST", "put", date(2025, 3, 21), 250.0, 100.0),
        ]
        store.list_option_contracts.return_value = contracts

        gw = HistoricalOptionGateway(store, "TSLA")
        result1 = gw.list_contracts(date(2025, 3, 14), "put", 8, 2)
        result2 = gw.list_contracts(date(2025, 3, 14), "put", 8, 2)

        assert result1 == contracts
        assert result2 == contracts
        store.list_option_contracts.assert_called_once()

    def test_list_contracts_cache_is_shared_across_gateway_instances_for_same_store(self):
        from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway

        store = MagicMock()
        contracts = [
            OptionContractRecord("O:TEST", "put", date(2025, 3, 21), 250.0, 100.0),
        ]
        store.list_option_contracts.return_value = contracts

        first = HistoricalOptionGateway(store, "TSLA")
        second = HistoricalOptionGateway(store, "TSLA")

        assert first.list_contracts(date(2025, 3, 14), "put", 8, 2) == contracts
        assert second.list_contracts(date(2025, 3, 14), "put", 8, 2) == contracts
        store.list_option_contracts.assert_called_once()

    def test_list_contracts_prefers_store_shared_cache_over_redis_for_historical_broad_queries(self):
        from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway
        from backtestforecast.market_data.redis_cache import OptionDataRedisCache

        store = MagicMock()
        contracts = [
            OptionContractRecord("O:TEST", "put", date(2025, 3, 21), 250.0, 100.0),
        ]
        store.list_option_contracts.return_value = contracts
        redis_cache = MagicMock(spec=OptionDataRedisCache)

        gw = HistoricalOptionGateway(store, "TSLA", redis_cache=redis_cache)
        assert gw.list_contracts(date(2025, 3, 14), "put", 8, 2) == contracts

        redis_cache.get_contracts.assert_not_called()
        redis_cache.set_contracts.assert_not_called()
        store.list_option_contracts.assert_called_once()

    def test_preferred_expiration_cache_reuses_exact_queries(self):
        from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway
        from backtestforecast.backtests.strategies.common import preferred_expiration_dates

        store = MagicMock()
        target_contracts = [
            OptionContractRecord("O:API", "call", date(2025, 4, 7), 250.0, 100.0),
        ]

        def fetch_exact(**kwargs):
            assert kwargs["expiration_date"] is None
            assert kwargs["expiration_gte"] == date(2025, 4, 6)
            assert kwargs["expiration_lte"] == date(2025, 4, 10)
            return target_contracts

        store.list_option_contracts_for_expiration.side_effect = fetch_exact

        gw = HistoricalOptionGateway(store, "TSLA")
        result1 = gw.list_contracts_for_preferred_expiration(
            entry_date=date(2025, 4, 1),
            contract_type="call",
            target_dte=7,
            dte_tolerance_days=2,
        )
        result2 = gw.list_contracts_for_preferred_expiration(
            entry_date=date(2025, 4, 1),
            contract_type="call",
            target_dte=7,
            dte_tolerance_days=2,
        )

        assert result1 == target_contracts
        assert result2 == target_contracts
        assert store.list_option_contracts_for_expiration.call_count == 1
        assert result1 == target_contracts
        assert result2 == target_contracts

    def test_list_available_expirations_batches_and_caches_store_calls(self):
        from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway

        class _Store:
            def __init__(self) -> None:
                self.calls = 0

            def list_available_option_expirations(self, **kwargs):
                self.calls += 1
                return [kwargs["expiration_dates"][1]]

        store = _Store()
        gw = HistoricalOptionGateway(store, "TSLA")
        expirations = [date(2025, 4, 18), date(2025, 4, 25)]

        first = gw.list_available_expirations(
            entry_date=date(2025, 3, 14),
            contract_type="call",
            expiration_dates=expirations,
        )
        second = gw.list_available_expirations(
            entry_date=date(2025, 3, 14),
            contract_type="call",
            expiration_dates=expirations,
        )

        assert first == [date(2025, 4, 25)]
        assert second == first
        assert store.calls == 1

    def test_list_contracts_for_preferred_expiration_prefers_filtered_range_query_over_availability_probe(self):
        from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway

        target_expiration = date(2025, 4, 8)
        later_expiration = date(2025, 4, 7)
        target_contracts = [
            OptionContractRecord("O:API", "call", target_expiration, 250.0, 100.0),
        ]

        class _Store:
            def __init__(self) -> None:
                self.range_calls: list[tuple[date | None, date | None, date | None]] = []
                self.availability_calls = 0

            def list_option_contracts_for_expiration(self, **kwargs):
                self.range_calls.append(
                    (
                        kwargs["expiration_date"],
                        kwargs.get("expiration_gte"),
                        kwargs.get("expiration_lte"),
                    )
                )
                assert kwargs["expiration_date"] is None
                assert kwargs["expiration_gte"] == date(2025, 4, 6)
                assert kwargs["expiration_lte"] == date(2025, 4, 10)
                return target_contracts + [
                    OptionContractRecord("O:LATER", "call", later_expiration, 255.0, 100.0)
                ]

            def list_available_option_expirations(self, **kwargs):
                self.availability_calls += 1
                raise AssertionError("preferred expiration lookup should not probe availability first")

        store = _Store()
        gw = HistoricalOptionGateway(store, "TSLA")

        first = gw.list_contracts_for_preferred_expiration(
            entry_date=date(2025, 4, 1),
            contract_type="call",
            target_dte=7,
            dte_tolerance_days=2,
        )
        second = gw.list_contracts_for_preferred_expiration(
            entry_date=date(2025, 4, 1),
            contract_type="call",
            target_dte=7,
            dte_tolerance_days=2,
        )

        assert first == target_contracts
        assert second == target_contracts
        assert store.range_calls == [(None, date(2025, 4, 6), date(2025, 4, 10))]
        assert store.availability_calls == 0

    def test_list_contracts_for_preferred_expiration_uses_cached_empty_availability_after_range_miss(self):
        from backtestforecast.errors import DataUnavailableError
        from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway

        store = MagicMock()
        store.list_option_contracts_for_expiration.return_value = []

        gw = HistoricalOptionGateway(store, "TSLA")

        for _ in range(2):
            with pytest.raises(DataUnavailableError):
                gw.list_contracts_for_preferred_expiration(
                    entry_date=date(2025, 4, 1),
                    contract_type="call",
                    target_dte=7,
                    dte_tolerance_days=2,
                    strike_price_gte=200.0,
                    strike_price_lte=210.0,
                )

        store.list_option_contracts_for_expiration.assert_called_once()

    def test_list_contracts_for_preferred_expiration_probes_small_unknown_prefix_before_cached_hit(self, monkeypatch):
        import backtestforecast.market_data.historical_gateway as module
        from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway

        monkeypatch.setattr(module, "_PREFERRED_EXPIRATION_EXACT_PROBE_MAX_UNKNOWN", 2)

        entry_date = date(2025, 4, 1)
        preferred_expiration = date(2025, 4, 7)
        preferred_contracts = [
            OptionContractRecord("O:PREFERRED", "call", preferred_expiration, 250.0, 100.0),
        ]

        class _Store:
            def __init__(self) -> None:
                self.calls: list[date | None] = []

            def list_option_contracts_for_expiration(self, **kwargs):
                expiration_date = kwargs["expiration_date"]
                self.calls.append(expiration_date)
                if expiration_date is None:
                    raise AssertionError("small unresolved prefix should use exact probes before falling back to range")
                if expiration_date == preferred_expiration:
                    return preferred_contracts
                return []

        store = _Store()
        gw = HistoricalOptionGateway(store, "TSLA")

        warmed = gw.list_contracts_for_expiration(
            entry_date=entry_date,
            contract_type="call",
            expiration_date=preferred_expiration,
        )
        resolved = gw.list_contracts_for_preferred_expiration(
            entry_date=entry_date,
            contract_type="call",
            target_dte=7,
            dte_tolerance_days=2,
        )

        assert warmed == preferred_contracts
        assert resolved == preferred_contracts
        assert store.calls == [preferred_expiration, date(2025, 4, 8), date(2025, 4, 9)]

    def test_list_contracts_for_preferred_expiration_uses_range_when_unknown_prefix_is_large(self, monkeypatch):
        import backtestforecast.market_data.historical_gateway as module
        from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway

        monkeypatch.setattr(module, "_PREFERRED_EXPIRATION_EXACT_PROBE_MAX_UNKNOWN", 2)

        entry_date = date(2025, 4, 1)
        preferred_expiration = date(2025, 4, 6)
        preferred_contracts = [
            OptionContractRecord("O:PREFERRED", "call", preferred_expiration, 250.0, 100.0),
        ]

        class _Store:
            def __init__(self) -> None:
                self.range_calls = 0
                self.exact_calls: list[date | None] = []

            def list_option_contracts_for_expiration(self, **kwargs):
                expiration_date = kwargs["expiration_date"]
                self.exact_calls.append(expiration_date)
                if expiration_date is None:
                    self.range_calls += 1
                    return preferred_contracts
                if expiration_date == preferred_expiration:
                    return preferred_contracts
                return []

        store = _Store()
        gw = HistoricalOptionGateway(store, "TSLA")

        warmed = gw.list_contracts_for_expiration(
            entry_date=entry_date,
            contract_type="call",
            expiration_date=preferred_expiration,
        )
        resolved = gw.list_contracts_for_preferred_expiration(
            entry_date=entry_date,
            contract_type="call",
            target_dte=7,
            dte_tolerance_days=4,
        )

        assert warmed == preferred_contracts
        assert resolved == preferred_contracts
        assert store.range_calls == 1
        assert store.exact_calls == [preferred_expiration, None]

    def test_filtered_exact_queries_reuse_broader_filtered_cache(self):
        from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway

        expiration = date(2025, 4, 18)
        filtered_contracts = [
            OptionContractRecord("O:250", "call", expiration, 250.0, 100.0),
            OptionContractRecord("O:260", "call", expiration, 260.0, 100.0),
            OptionContractRecord("O:270", "call", expiration, 270.0, 100.0),
        ]

        store = MagicMock()
        store.list_option_contracts_for_expiration.return_value = filtered_contracts

        gw = HistoricalOptionGateway(store, "TSLA")
        first = gw.list_contracts_for_expiration(
            entry_date=date(2025, 3, 14),
            contract_type="call",
            expiration_date=expiration,
            strike_price_gte=250.0,
            strike_price_lte=275.0,
        )
        second = gw.list_contracts_for_expiration(
            entry_date=date(2025, 3, 14),
            contract_type="call",
            expiration_date=expiration,
            strike_price_gte=265.0,
            strike_price_lte=275.0,
        )

        assert [contract.ticker for contract in first] == ["O:250", "O:260", "O:270"]
        assert [contract.ticker for contract in second] == ["O:270"]
        store.list_option_contracts_for_expiration.assert_called_once_with(
            symbol="TSLA",
            as_of_date=date(2025, 3, 14),
            contract_type="call",
            expiration_date=expiration,
            strike_price_gte=250.0,
            strike_price_lte=275.0,
        )

    def test_list_contracts_for_expirations_batches_store_calls(self):
        from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway

        class _Store:
            def __init__(self) -> None:
                self.calls = 0

            def list_option_contracts_for_expirations(self, **kwargs):
                self.calls += 1
                return {
                    date(2025, 4, 18): [
                        OptionContractRecord("O:APR18", "call", date(2025, 4, 18), 250.0, 100.0),
                    ],
                    date(2025, 4, 25): [
                        OptionContractRecord("O:APR25", "call", date(2025, 4, 25), 250.0, 100.0),
                    ],
                }

        store = _Store()
        gw = HistoricalOptionGateway(store, "TSLA")

        first = gw.list_contracts_for_expirations(
            entry_date=date(2025, 3, 14),
            contract_type="call",
            expiration_dates=[date(2025, 4, 18), date(2025, 4, 25)],
        )
        second = gw.list_contracts_for_expirations(
            entry_date=date(2025, 3, 14),
            contract_type="call",
            expiration_dates=[date(2025, 4, 18), date(2025, 4, 25)],
        )

        assert first[date(2025, 4, 18)][0].ticker == "O:APR18"
        assert second[date(2025, 4, 25)][0].ticker == "O:APR25"
        assert store.calls == 1

    def test_list_contracts_for_expirations_by_type_batches_store_calls(self):
        from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway

        class _Store:
            def __init__(self) -> None:
                self.calls = 0

            def list_option_contracts_for_expirations_by_type(self, **kwargs):
                self.calls += 1
                expirations = kwargs["expiration_dates"]
                return {
                    "call": {
                        expirations[0]: [
                            OptionContractRecord("O:CALL1", "call", expirations[0], 250.0, 100.0),
                        ],
                        expirations[1]: [],
                    },
                    "put": {
                        expirations[0]: [
                            OptionContractRecord("O:PUT1", "put", expirations[0], 245.0, 100.0),
                        ],
                        expirations[1]: [],
                    },
                }

        store = _Store()
        gw = HistoricalOptionGateway(store, "TSLA")
        expirations = [date(2025, 4, 18), date(2025, 4, 25)]

        first = gw.list_contracts_for_expirations_by_type(
            entry_date=date(2025, 3, 14),
            contract_types=["call", "put"],
            expiration_dates=expirations,
        )
        second = gw.list_contracts_for_expirations_by_type(
            entry_date=date(2025, 3, 14),
            contract_types=["call", "put"],
            expiration_dates=expirations,
        )

        assert first["call"][date(2025, 4, 18)][0].ticker == "O:CALL1"
        assert first["put"][date(2025, 4, 18)][0].ticker == "O:PUT1"
        assert second["call"][date(2025, 4, 25)] == []
        assert second["put"][date(2025, 4, 25)] == []
        assert store.calls == 1

    def test_list_contracts_for_preferred_common_expiration_short_circuits_from_cached_exacts(self):
        from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway

        entry_date = date(2025, 4, 1)
        expiration = date(2025, 4, 4)
        call_contract = OptionContractRecord("O:CALL1", "call", expiration, 250.0, 100.0)
        put_contract = OptionContractRecord("O:PUT1", "put", expiration, 245.0, 100.0)

        class _Store:
            def __init__(self) -> None:
                self.calls = 0

            def list_option_contracts_for_expirations_by_type(self, **kwargs):
                self.calls += 1
                expirations = kwargs["expiration_dates"]
                return {
                    "call": {
                        expiry: ([call_contract] if expiry == expiration else [])
                        for expiry in expirations
                    },
                    "put": {
                        expiry: ([put_contract] if expiry == expiration else [])
                        for expiry in expirations
                    },
                }

        store = _Store()
        gw = HistoricalOptionGateway(store, "TSLA")

        warmed = gw.list_contracts_for_expirations_by_type(
            entry_date=entry_date,
            contract_types=["call", "put"],
            expiration_dates=[expiration],
        )
        result = gw.list_contracts_for_preferred_common_expiration(
            entry_date=entry_date,
            target_dte=3,
            dte_tolerance_days=2,
        )

        assert warmed["call"][expiration][0].ticker == "O:CALL1"
        assert warmed["put"][expiration][0].ticker == "O:PUT1"
        assert result[0] == expiration
        assert result[1][0].ticker == "O:CALL1"
        assert result[2][0].ticker == "O:PUT1"
        assert store.calls == 1

    def test_get_quote_caches_missing_result(self):
        from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway

        store = MagicMock()
        store.get_option_quote_for_date.return_value = None

        gw = HistoricalOptionGateway(store, "TSLA")
        assert gw.get_quote("O:TEST", date(2025, 3, 14)) is None
        assert gw.get_quote("O:TEST", date(2025, 3, 14)) is None
        store.get_option_quote_for_date.assert_called_once_with("O:TEST", date(2025, 3, 14))

    def test_get_quote_uses_redis_before_store(self):
        from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway
        from backtestforecast.market_data.redis_cache import OptionDataRedisCache

        store = MagicMock()
        redis_cache = MagicMock(spec=OptionDataRedisCache)
        redis_cache.get_quote.return_value = OptionQuoteRecord(date(2025, 3, 14), 3.0, 3.2, None)

        gw = HistoricalOptionGateway(store, "TSLA", redis_cache=redis_cache)
        result = gw.get_quote("O:TEST", date(2025, 3, 14))

        assert result is not None
        assert result.bid_price == 3.0
        store.get_option_quote_for_date.assert_not_called()

    def test_exact_contracts_use_redis_before_store(self):
        from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway
        from backtestforecast.market_data.redis_cache import OptionDataRedisCache

        store = MagicMock()
        redis_cache = MagicMock(spec=OptionDataRedisCache)
        redis_cache.get_exact_contracts.return_value = [
            OptionContractRecord("O:EXACT", "call", date(2025, 4, 18), 250.0, 100.0),
        ]

        gw = HistoricalOptionGateway(store, "TSLA", redis_cache=redis_cache)
        result = gw.list_contracts_for_expiration(
            entry_date=date(2025, 3, 14),
            contract_type="call",
            expiration_date=date(2025, 4, 18),
        )

        assert len(result) == 1
        assert result[0].ticker == "O:EXACT"
        store.list_option_contracts_for_expiration.assert_not_called()

    def test_filtered_exact_queries_do_not_evict_full_expiration_cache(self, monkeypatch):
        import backtestforecast.market_data.historical_gateway as module
        from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway

        monkeypatch.setattr(module, "_FULL_EXACT_CONTRACT_CACHE_MAX", 8)
        monkeypatch.setattr(module, "_FILTERED_EXACT_CONTRACT_CACHE_MAX", 2)
        expiration = date(2025, 4, 18)
        store = MagicMock()

        def fetch_exact(**kwargs):
            as_of_date = kwargs["as_of_date"]
            strike = kwargs["strike_price_gte"] or 250.0
            return [
                OptionContractRecord(
                    f"O:{as_of_date.strftime('%Y%m%d')}",
                    kwargs["contract_type"],
                    expiration,
                    float(strike),
                    100.0,
                )
            ]

        store.list_option_contracts_for_expiration.side_effect = fetch_exact
        gw = HistoricalOptionGateway(store, "TSLA")
        anchor_entry = date(2025, 3, 14)

        anchor = gw.list_contracts_for_expiration(
            entry_date=anchor_entry,
            contract_type="call",
            expiration_date=expiration,
        )
        for offset in range(3):
            gw.list_contracts_for_expiration(
                entry_date=anchor_entry + timedelta(days=offset + 1),
                contract_type="call",
                expiration_date=expiration,
                strike_price_gte=300.0 + offset,
                strike_price_lte=301.0 + offset,
            )
        anchor_again = gw.list_contracts_for_expiration(
            entry_date=anchor_entry,
            contract_type="call",
            expiration_date=expiration,
        )

        assert anchor_again == anchor
        assert store.list_option_contracts_for_expiration.call_count == 4

    def test_concurrent_get_quote_coalesces_store_calls(self):
        from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway

        store = MagicMock()
        quote = OptionQuoteRecord(date(2025, 3, 14), 3.0, 3.2, None)

        def fetch_quote(*args, **kwargs):
            time.sleep(0.05)
            return quote

        store.get_option_quote_for_date.side_effect = fetch_quote
        gw = HistoricalOptionGateway(store, "TSLA")
        results: list[OptionQuoteRecord | None] = []

        def worker() -> None:
            results.append(gw.get_quote("O:TEST", date(2025, 3, 14)))

        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start()
        t2.start()
        t1.join(timeout=2)
        t2.join(timeout=2)

        assert len(results) == 2
        assert all(result is not None for result in results)
        assert store.get_option_quote_for_date.call_count == 1

    def test_get_quotes_batches_store_calls(self):
        from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway

        class _Store:
            def __init__(self) -> None:
                self.calls = 0

            def get_option_quotes_for_date(self, option_tickers, trade_date):
                self.calls += 1
                return {
                    ticker: OptionQuoteRecord(trade_date, 3.0 + index, 3.2 + index, None)
                    for index, ticker in enumerate(option_tickers)
                }

        store = _Store()
        gw = HistoricalOptionGateway(store, "TSLA")

        first = gw.get_quotes(["O:A", "O:B"], date(2025, 3, 14))
        second = gw.get_quotes(["O:A", "O:B"], date(2025, 3, 14))

        assert first["O:A"] is not None
        assert first["O:B"] is not None
        assert second["O:A"].mid_price == first["O:A"].mid_price
        assert store.calls == 1

    def test_get_quote_series_batches_store_calls_and_warms_quote_cache(self):
        from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway

        class _Store:
            def __init__(self) -> None:
                self.series_calls = 0
                self.date_calls = 0

            def get_option_quote_series(self, option_tickers, start_date, end_date):
                self.series_calls += 1
                return {
                    ticker: {
                        start_date: OptionQuoteRecord(start_date, 3.0 + index, 3.2 + index, None),
                        end_date: OptionQuoteRecord(end_date, 4.0 + index, 4.2 + index, None),
                    }
                    for index, ticker in enumerate(option_tickers)
                }

            def get_option_quotes_for_date(self, option_tickers, trade_date):
                self.date_calls += 1
                return {
                    ticker: OptionQuoteRecord(trade_date, 9.0, 9.2, None)
                    for ticker in option_tickers
                }

        store = _Store()
        gw = HistoricalOptionGateway(store, "TSLA")

        series = gw.get_quote_series(
            ["O:A", "O:B"],
            date(2025, 3, 14),
            date(2025, 3, 17),
        )
        series_again = gw.get_quote_series(
            ["O:B", "O:A"],
            date(2025, 3, 14),
            date(2025, 3, 17),
        )
        quotes = gw.get_quotes(["O:A", "O:B"], date(2025, 3, 14))

        assert store.series_calls == 1
        assert store.date_calls == 0
        assert series["O:A"][date(2025, 3, 14)].mid_price == pytest.approx(3.1)
        assert series_again["O:B"][date(2025, 3, 17)].mid_price == pytest.approx(5.1)
        assert quotes["O:A"] is not None
        assert quotes["O:A"].mid_price == pytest.approx(3.1)


# ---------------------------------------------------------------------------
# Prefetcher
# ---------------------------------------------------------------------------

class TestOptionDataPrefetcher:
    def test_prefetch_iterates_all_dates_and_types(self):
        from backtestforecast.market_data.prefetch import OptionDataPrefetcher
        from backtestforecast.market_data.service import MassiveOptionGateway

        mock_client = MagicMock()
        gw = MassiveOptionGateway(mock_client, "TSLA")

        contracts = [
            OptionContractRecord("O:A", "put", date(2025, 3, 21), 250.0, 100.0),
            OptionContractRecord("O:B", "put", date(2025, 3, 21), 260.0, 100.0),
        ]
        mock_client.list_option_contracts.return_value = contracts
        mock_client.get_option_quote_for_date.return_value = OptionQuoteRecord(
            date(2025, 3, 14), 2.0, 2.2, None,
        )

        bars = [
            DailyBar(date(2025, 3, 14), 250, 255, 248, 252, 1000000),
            DailyBar(date(2025, 3, 17), 252, 258, 250, 256, 1200000),
        ]

        prefetcher = OptionDataPrefetcher(max_workers=2)
        summary = prefetcher.prefetch_for_symbol(
            symbol="TSLA",
            bars=bars,
            start_date=date(2025, 3, 14),
            end_date=date(2025, 3, 17),
            target_dte=8,
            dte_tolerance_days=2,
            option_gateway=gw,
        )

        assert summary.dates_processed == 2
        # 2 dates x 2 types (put, call) = 4 list_contracts calls
        assert mock_client.list_option_contracts.call_count == 4
        # 2 contracts x 2 dates x 2 types = 8 quote calls
        assert summary.quotes_fetched == 8

    def test_prefetch_uses_concurrency(self):
        """Verify that multiple dates are processed by the thread pool."""
        import threading

        from backtestforecast.market_data.prefetch import OptionDataPrefetcher
        from backtestforecast.market_data.service import MassiveOptionGateway

        mock_client = MagicMock()
        gw = MassiveOptionGateway(mock_client, "TSLA")

        observed_threads: set[int] = set()

        def _track_thread(*args, **kwargs):
            observed_threads.add(threading.current_thread().ident)
            return [OptionContractRecord("O:A", "put", date(2025, 3, 21), 250.0, 100.0)]

        mock_client.list_option_contracts.side_effect = _track_thread
        mock_client.get_option_quote_for_date.return_value = OptionQuoteRecord(
            date(2025, 3, 14), 2.0, 2.2, None,
        )

        bars = [
            DailyBar(date(2025, 3, d), 250, 255, 248, 252, 1000000)
            for d in range(3, 22) if date(2025, 3, d).weekday() < 5
        ]

        prefetcher = OptionDataPrefetcher(max_workers=4)
        summary = prefetcher.prefetch_for_symbol(
            symbol="TSLA",
            bars=bars,
            start_date=date(2025, 3, 3),
            end_date=date(2025, 3, 21),
            target_dte=8,
            dte_tolerance_days=2,
            option_gateway=gw,
        )

        assert summary.dates_processed == len(bars)
        assert len(observed_threads) > 1, "Expected multiple threads to be used"

    def test_prefetch_can_skip_quote_warming(self):
        from backtestforecast.market_data.prefetch import OptionDataPrefetcher
        from backtestforecast.market_data.service import MassiveOptionGateway

        mock_client = MagicMock()
        gw = MassiveOptionGateway(mock_client, "TSLA")
        contracts = [
            OptionContractRecord("O:A", "put", date(2025, 3, 21), 250.0, 100.0),
        ]
        mock_client.list_option_contracts.return_value = contracts
        mock_client.get_option_quote_for_date.return_value = OptionQuoteRecord(
            date(2025, 3, 14), 2.0, 2.2, None,
        )

        bars = [
            DailyBar(date(2025, 3, 14), 250, 255, 248, 252, 1000000),
            DailyBar(date(2025, 3, 17), 252, 258, 250, 256, 1200000),
        ]

        summary = OptionDataPrefetcher(max_workers=2).prefetch_for_symbol(
            symbol="TSLA",
            bars=bars,
            start_date=date(2025, 3, 14),
            end_date=date(2025, 3, 17),
            target_dte=8,
            dte_tolerance_days=2,
            option_gateway=gw,
            include_quotes=False,
        )

        assert summary.dates_processed == 2
        assert summary.contracts_fetched == 4
        assert summary.quotes_fetched == 0
        mock_client.get_option_quote_for_date.assert_not_called()

    def test_prefetch_accepts_historical_option_gateway(self):
        from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway
        from backtestforecast.market_data.prefetch import OptionDataPrefetcher

        store = MagicMock()
        store.list_option_contracts.return_value = [
            OptionContractRecord("O:A", "put", date(2025, 3, 21), 250.0, 100.0),
        ]
        store.get_option_quote_for_date.return_value = OptionQuoteRecord(
            date(2025, 3, 14), 2.0, 2.2, None,
        )
        gateway = HistoricalOptionGateway(store, "TSLA")

        bars = [
            DailyBar(date(2025, 3, 14), 250, 255, 248, 252, 1000000),
            DailyBar(date(2025, 3, 17), 252, 258, 250, 256, 1200000),
        ]

        summary = OptionDataPrefetcher(max_workers=2).prefetch_for_symbol(
            symbol="TSLA",
            bars=bars,
            start_date=date(2025, 3, 14),
            end_date=date(2025, 3, 17),
            target_dte=8,
            dte_tolerance_days=2,
            option_gateway=gateway,
        )

        assert summary.dates_processed == 2
        assert summary.contracts_fetched == 4
        assert summary.quotes_fetched == 4
        assert store.list_option_contracts.call_count == 4
        assert store.get_option_quote_for_date.call_count == 2


# ---------------------------------------------------------------------------
# Sweep schema validation
# ---------------------------------------------------------------------------

class TestSweepSchemaValidation:
    def test_valid_sweep_request(self):
        from backtestforecast.schemas.sweeps import CreateSweepRequest

        with patch("backtestforecast.schemas.sweeps.get_settings") as mock_settings, \
             patch("backtestforecast.utils.dates.market_date_today", return_value=date(2025, 12, 31)):
            mock_settings.return_value.max_sweep_window_days = 1825

            req = CreateSweepRequest(
                symbol="TSLA",
                strategy_types=["bull_put_credit_spread"],
                start_date=date(2025, 1, 1),
                end_date=date(2025, 6, 30),
                target_dte=8,
                max_holding_days=8,
                account_size=Decimal("10000"),
                risk_per_trade_pct=Decimal("5"),
                commission_per_contract=Decimal("0.65"),
                entry_rule_sets=[{"name": "no_rules", "entry_rules": []}],
                delta_grid=[{"value": 30}, {"value": 16}],
                width_grid=[{"mode": "dollar_width", "value": Decimal("10")}],
                exit_rule_sets=[
                    {"name": "no_exit", "profit_target_pct": None, "stop_loss_pct": None},
                    {"name": "tight_pt", "profit_target_pct": 50.0, "stop_loss_pct": None},
                ],
            )
            assert req.symbol == "TSLA"
            assert len(req.delta_grid) == 2
            assert len(req.exit_rule_sets) == 2

    def test_delta_grid_validation_range(self):
        from backtestforecast.schemas.sweeps import DeltaGridItem

        with pytest.raises(Exception):
            DeltaGridItem(value=0)
        with pytest.raises(Exception):
            DeltaGridItem(value=100)

        item = DeltaGridItem(value=30)
        assert item.value == 30

    def test_width_grid_validation(self):
        from backtestforecast.schemas.sweeps import WidthGridItem

        item = WidthGridItem(mode="dollar_width", value=Decimal("10"))
        assert item.mode == "dollar_width"

        with pytest.raises(Exception):
            WidthGridItem(mode="dollar_width", value=Decimal("200"))

    def test_duplicate_entry_rule_set_names_rejected(self):
        from backtestforecast.schemas.sweeps import CreateSweepRequest

        with patch("backtestforecast.schemas.sweeps.get_settings") as mock_settings, \
             patch("backtestforecast.utils.dates.market_date_today", return_value=date(2025, 12, 31)):
            mock_settings.return_value.max_sweep_window_days = 1825

            with pytest.raises(Exception, match="duplicate"):
                CreateSweepRequest(
                    symbol="TSLA",
                    strategy_types=["bull_put_credit_spread"],
                    start_date=date(2025, 1, 1),
                    end_date=date(2025, 6, 30),
                    target_dte=8,
                    max_holding_days=8,
                    account_size=Decimal("10000"),
                    risk_per_trade_pct=Decimal("5"),
                    commission_per_contract=Decimal("0.65"),
                    entry_rule_sets=[
                        {"name": "ruleset_a", "entry_rules": []},
                        {"name": "ruleset_a", "entry_rules": []},
                    ],
                )

    def test_exit_rule_set_bounds(self):
        from backtestforecast.schemas.sweeps import ExitRuleSet

        es = ExitRuleSet(name="test", profit_target_pct=50.0, stop_loss_pct=100.0)
        assert es.profit_target_pct == 50.0

        with pytest.raises(Exception):
            ExitRuleSet(name="test", profit_target_pct=0.5)


# ---------------------------------------------------------------------------
# Sweep service scoring
# ---------------------------------------------------------------------------

class TestSweepServiceScoring:
    def test_score_candidate_from_summary(self):
        from backtestforecast.services.sweeps import SweepService

        summary = {
            "trade_count": 20,
            "win_rate": 70.0,
            "total_roi_pct": 15.0,
            "max_drawdown_pct": 5.0,
            "sharpe_ratio": 1.5,
        }

        score = SweepService._score_candidate_from_summary(summary)
        assert score > 0

    def test_score_zero_for_few_trades(self):
        from backtestforecast.services.sweeps import SweepService

        summary = {
            "trade_count": 2,
            "win_rate": 100.0,
            "total_roi_pct": 50.0,
            "max_drawdown_pct": 0.0,
            "sharpe_ratio": 5.0,
        }

        score = SweepService._score_candidate_from_summary(summary)
        assert score == 0.0

    def test_build_overrides_with_delta_only(self):
        from backtestforecast.services.sweeps import SweepService

        overrides = SweepService._build_overrides(delta_val=30, width_val=None)
        assert overrides is not None
        assert overrides.short_put_strike is not None
        assert overrides.short_put_strike.mode.value == "delta_target"
        assert overrides.spread_width is None

    def test_build_overrides_with_both(self):
        from backtestforecast.services.sweeps import SweepService

        overrides = SweepService._build_overrides(
            delta_val=16,
            width_val=("dollar_width", Decimal("10")),
        )
        assert overrides is not None
        assert overrides.short_put_strike is not None
        assert overrides.spread_width is not None
        assert overrides.spread_width.mode.value == "dollar_width"

    def test_build_overrides_none_when_no_params(self):
        from backtestforecast.services.sweeps import SweepService

        overrides = SweepService._build_overrides(delta_val=None, width_val=None)
        assert overrides is None

    def test_compute_candidate_count(self):
        from backtestforecast.schemas.sweeps import (
            CreateSweepRequest,
            DeltaGridItem,
            ExitRuleSet,
            WidthGridItem,
        )
        from backtestforecast.services.sweeps import SweepService

        with patch("backtestforecast.schemas.sweeps.get_settings") as mock_settings, \
             patch("backtestforecast.utils.dates.market_date_today", return_value=date(2025, 12, 31)):
            mock_settings.return_value.max_sweep_window_days = 1825

            payload = CreateSweepRequest(
                symbol="TSLA",
                strategy_types=["bull_put_credit_spread", "bear_call_credit_spread"],
                start_date=date(2025, 1, 1),
                end_date=date(2025, 6, 30),
                target_dte=8,
                max_holding_days=8,
                account_size=Decimal("10000"),
                risk_per_trade_pct=Decimal("5"),
                commission_per_contract=Decimal("0.65"),
                entry_rule_sets=[
                    {"name": "set_a", "entry_rules": []},
                    {"name": "set_b", "entry_rules": []},
                ],
                delta_grid=[DeltaGridItem(value=16), DeltaGridItem(value=30), DeltaGridItem(value=45)],
                width_grid=[WidthGridItem(mode="dollar_width", value=Decimal("5"))],
                exit_rule_sets=[
                    ExitRuleSet(name="none"),
                    ExitRuleSet(name="tight", profit_target_pct=50.0),
                ],
            )

            count = SweepService._compute_candidate_count(payload)
            # 2 strategies x 2 entry_sets x 3 deltas x 1 width x 2 exits = 24
            assert count == 24


# ---------------------------------------------------------------------------
# IV estimation cache
# ---------------------------------------------------------------------------

class TestIVEstimationCache:
    def test_iv_cache_avoids_bisection_on_second_call(self):
        from backtestforecast.backtests.strategies.common import _estimate_iv_for_strike

        contracts = [
            OptionContractRecord("O:TSLA250321P00250000", "put", date(2025, 3, 21), 250.0, 100.0),
        ]
        mock_gateway = MagicMock()
        mock_gateway.get_iv = None
        mock_gateway.store_iv = None
        mock_gateway.get_quote.return_value = OptionQuoteRecord(date(2025, 3, 14), 5.0, 5.2, None)

        iv_cache: dict = {}

        with patch("backtestforecast.backtests.rules.implied_volatility_from_price", return_value=0.35) as mock_iv:
            result1 = _estimate_iv_for_strike(250.0, "put", 260.0, 8, contracts, mock_gateway, date(2025, 3, 14), iv_cache=iv_cache)
            result2 = _estimate_iv_for_strike(250.0, "put", 260.0, 8, contracts, mock_gateway, date(2025, 3, 14), iv_cache=iv_cache)

        assert result1 == 0.35
        assert result2 == 0.35
        assert mock_iv.call_count == 1, "Bisection should only run once; second call should hit cache"
        assert len(iv_cache) == 1

    def test_iv_cache_stores_none_for_missing_quote(self):
        from backtestforecast.backtests.strategies.common import _estimate_iv_for_strike

        contracts = [
            OptionContractRecord("O:TSLA250321P00250000", "put", date(2025, 3, 21), 250.0, 100.0),
        ]
        mock_gateway = MagicMock()
        mock_gateway.get_iv = None
        mock_gateway.store_iv = None
        mock_gateway.get_quote.return_value = None

        iv_cache: dict = {}

        result1 = _estimate_iv_for_strike(250.0, "put", 260.0, 8, contracts, mock_gateway, date(2025, 3, 14), iv_cache=iv_cache)
        result2 = _estimate_iv_for_strike(250.0, "put", 260.0, 8, contracts, mock_gateway, date(2025, 3, 14), iv_cache=iv_cache)

        assert result1 is None
        assert result2 is None
        assert ("O:TSLA250321P00250000", date(2025, 3, 14)) in iv_cache
        assert iv_cache[("O:TSLA250321P00250000", date(2025, 3, 14))] is None
        assert mock_gateway.get_quote.call_count == 1, "Second call should hit cache, not re-fetch quote"

    def test_iv_cache_shared_across_resolve_strike_calls(self):
        from backtestforecast.backtests.strategies.common import resolve_strike
        from backtestforecast.schemas.backtests import StrikeSelection, StrikeSelectionMode

        contracts = [
            OptionContractRecord("O:PUT240", "put", date(2025, 3, 21), 240.0, 100.0),
            OptionContractRecord("O:PUT250", "put", date(2025, 3, 21), 250.0, 100.0),
            OptionContractRecord("O:PUT260", "put", date(2025, 3, 21), 260.0, 100.0),
        ]
        strikes = [240.0, 250.0, 260.0]

        mock_gateway = MagicMock()
        mock_gateway.get_iv = None
        mock_gateway.store_iv = None
        mock_gateway.get_quote.return_value = OptionQuoteRecord(date(2025, 3, 14), 3.0, 3.2, None)

        iv_cache: dict = {}

        with patch("backtestforecast.backtests.rules.implied_volatility_from_price", return_value=0.30) as mock_iv:
            sel_30 = StrikeSelection(mode=StrikeSelectionMode.DELTA_TARGET, value=30)
            resolve_strike(
                strikes, 255.0, "put", sel_30, 8,
                contracts=contracts, option_gateway=mock_gateway, trade_date=date(2025, 3, 14), iv_cache=iv_cache,
            )
            first_call_count = mock_iv.call_count

            sel_16 = StrikeSelection(mode=StrikeSelectionMode.DELTA_TARGET, value=16)
            resolve_strike(
                strikes, 255.0, "put", sel_16, 8,
                contracts=contracts, option_gateway=mock_gateway, trade_date=date(2025, 3, 14), iv_cache=iv_cache,
            )
            second_call_count = mock_iv.call_count

        assert first_call_count == 3, "First call should compute IV for all 3 strikes"
        assert second_call_count == 3, "Second call should hit cache for all 3 strikes (0 new bisections)"
        assert len(iv_cache) == 3

    def test_iv_cache_none_does_not_break_existing_behavior(self):
        from backtestforecast.backtests.strategies.common import _estimate_iv_for_strike

        contracts = [
            OptionContractRecord("O:TSLA250321P00250000", "put", date(2025, 3, 21), 250.0, 100.0),
        ]
        mock_gateway = MagicMock()
        mock_gateway.get_iv = None
        mock_gateway.store_iv = None
        mock_gateway.get_quote.return_value = OptionQuoteRecord(date(2025, 3, 14), 5.0, 5.2, None)

        with patch("backtestforecast.backtests.rules.implied_volatility_from_price", return_value=0.40) as mock_iv:
            result = _estimate_iv_for_strike(250.0, "put", 260.0, 8, contracts, mock_gateway, date(2025, 3, 14), iv_cache=None)

        assert result == 0.40
        assert mock_iv.call_count == 1

    def test_gateway_has_iv_cache(self):
        """MassiveOptionGateway exposes _iv_cache used by strategy modules
        for per-gateway IV caching during backtest execution."""
        from backtestforecast.market_data.service import MassiveOptionGateway

        mock_client = MagicMock()
        gw = MassiveOptionGateway(mock_client, "TSLA")
        assert hasattr(gw, '_iv_cache')
        assert isinstance(gw._iv_cache, dict)
