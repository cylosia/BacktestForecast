"""Tests for the Redis option data cache, gateway integration, prefetcher,
and sweep schema validation."""
from __future__ import annotations

import json
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
            _serialize_contracts,
            _deserialize_contracts,
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
            _serialize_quote,
            _deserialize_quote,
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
            _serialize_quote,
            _deserialize_quote,
        )

        raw = _serialize_quote(None)
        restored = _deserialize_quote(raw)
        assert restored is None

    def test_empty_contracts_list(self):
        from backtestforecast.market_data.redis_cache import (
            _serialize_contracts,
            _deserialize_contracts,
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
        redis_cache.set_quote.assert_called_once_with("O:TEST", date(2025, 3, 14), None)

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
        # 2 dates × 2 types (put, call) = 4 list_contracts calls
        assert mock_client.list_option_contracts.call_count == 4
        # 2 contracts × 2 dates × 2 types = 8 quote calls
        assert summary.quotes_fetched == 8

    def test_prefetch_uses_concurrency(self):
        """Verify that multiple dates are processed by the thread pool."""
        import threading
        from backtestforecast.market_data.prefetch import OptionDataPrefetcher
        from backtestforecast.market_data.service import MassiveOptionGateway

        mock_client = MagicMock()
        gw = MassiveOptionGateway(mock_client, "TSLA")

        observed_threads: set[int] = set()
        original_list = mock_client.list_option_contracts

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


# ---------------------------------------------------------------------------
# Sweep schema validation
# ---------------------------------------------------------------------------

class TestSweepSchemaValidation:
    def test_valid_sweep_request(self):
        from backtestforecast.schemas.sweeps import CreateSweepRequest

        with patch("backtestforecast.schemas.sweeps.get_settings") as mock_settings, \
             patch("backtestforecast.utils.dates.market_date_today", return_value=date(2025, 12, 31)):
            mock_settings.return_value.max_backtest_window_days = 1825

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
            mock_settings.return_value.max_backtest_window_days = 1825

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
            mock_settings.return_value.max_backtest_window_days = 1825

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
            # 2 strategies × 2 entry_sets × 3 deltas × 1 width × 2 exits = 24
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
        mock_gateway.get_quote.return_value = OptionQuoteRecord(date(2025, 3, 14), 5.0, 5.2, None)

        with patch("backtestforecast.backtests.rules.implied_volatility_from_price", return_value=0.40) as mock_iv:
            result = _estimate_iv_for_strike(250.0, "put", 260.0, 8, contracts, mock_gateway, date(2025, 3, 14), iv_cache=None)

        assert result == 0.40
        assert mock_iv.call_count == 1

    def test_gateway_has_iv_cache(self):
        from backtestforecast.market_data.service import MassiveOptionGateway

        mock_client = MagicMock()
        gw = MassiveOptionGateway(mock_client, "TSLA")
        assert hasattr(gw, '_iv_cache')
        assert isinstance(gw._iv_cache, dict)
        assert len(gw._iv_cache) == 0
