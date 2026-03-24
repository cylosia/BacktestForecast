"""Unit tests for MarketDataService.

Tests the bars cache, request coalescing, and Redis cache fallback
without needing a live Massive API or Redis instance.
"""
from __future__ import annotations

import threading
import time
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

from backtestforecast.errors import ExternalServiceError
from backtestforecast.market_data.service import MarketDataService
from backtestforecast.market_data.types import DailyBar
from backtestforecast.schemas.backtests import CreateBacktestRunRequest


def _make_bar(offset: int, base_date: date | None = None) -> DailyBar:
    base = base_date or date(2024, 1, 2)
    return DailyBar(
        trade_date=base + timedelta(days=offset),
        open_price=100.0 + offset,
        high_price=101.0 + offset,
        low_price=99.0 + offset,
        close_price=100.5 + offset,
        volume=1_000_000 + offset * 1000,
    )


def _make_bars(count: int = 5, base_date: date | None = None) -> list[DailyBar]:
    return [_make_bar(i, base_date) for i in range(count)]


def _make_service(bars: list[DailyBar] | None = None) -> MarketDataService:
    client = MagicMock()
    client.get_stock_daily_bars.return_value = bars or _make_bars()
    with patch.object(MarketDataService, "_build_redis_cache", return_value=None):
        svc = MarketDataService(client)
    return svc


class TestBarsCacheHit:
    def test_second_fetch_returns_cached_bars(self):
        svc = _make_service()
        key = ("AAPL", date(2024, 1, 1), date(2024, 3, 31))

        first = svc._fetch_bars_coalesced(*key)
        second = svc._fetch_bars_coalesced(*key)

        assert first is second
        svc.client.get_stock_daily_bars.assert_called_once()

    def test_different_keys_are_independent(self):
        svc = _make_service()
        svc._fetch_bars_coalesced("AAPL", date(2024, 1, 1), date(2024, 3, 31))
        svc._fetch_bars_coalesced("MSFT", date(2024, 1, 1), date(2024, 3, 31))

        assert svc.client.get_stock_daily_bars.call_count == 2


class TestBarsCacheMiss:
    def test_first_fetch_calls_api(self):
        bars = _make_bars()
        svc = _make_service(bars)
        result = svc._fetch_bars_coalesced("AAPL", date(2024, 1, 1), date(2024, 3, 31))

        assert result == bars
        svc.client.get_stock_daily_bars.assert_called_once_with(
            "AAPL", date(2024, 1, 1), date(2024, 3, 31),
        )

    def test_cache_eviction_on_max_size(self):
        svc = _make_service()
        svc._MAX_BARS_CACHE_SIZE = 2

        svc._fetch_bars_coalesced("A", date(2024, 1, 1), date(2024, 1, 31))
        svc._fetch_bars_coalesced("B", date(2024, 1, 1), date(2024, 1, 31))
        svc._fetch_bars_coalesced("C", date(2024, 1, 1), date(2024, 1, 31))

        assert len(svc._bars_cache) <= 2
        assert ("A", date(2024, 1, 1), date(2024, 1, 31)) not in svc._bars_cache


class TestRedisFailureGracefulDegradation:
    def test_service_works_without_redis(self):
        svc = _make_service()
        assert svc._redis_cache is None
        result = svc._fetch_bars_coalesced("AAPL", date(2024, 1, 1), date(2024, 3, 31))
        assert len(result) == 5


class TestRequestCoalescing:
    def test_concurrent_requests_for_same_key_coalesce(self):
        """Only one thread should call the API; the other waits for the result."""
        bars = _make_bars()
        svc = _make_service(bars)
        call_count_lock = threading.Lock()
        api_calls = [0]

        original_get = svc.client.get_stock_daily_bars

        def slow_get(*args, **kwargs):
            with call_count_lock:
                api_calls[0] += 1
            time.sleep(0.1)
            return original_get(*args, **kwargs)

        svc.client.get_stock_daily_bars = MagicMock(side_effect=slow_get)

        results: list[list[DailyBar] | None] = [None, None]
        errors: list[Exception | None] = [None, None]

        def fetch(idx: int) -> None:
            try:
                results[idx] = svc._fetch_bars_coalesced(
                    "AAPL", date(2024, 1, 1), date(2024, 3, 31),
                )
            except Exception as exc:
                errors[idx] = exc

        t1 = threading.Thread(target=fetch, args=(0,))
        t2 = threading.Thread(target=fetch, args=(1,))
        t1.start()
        time.sleep(0.02)
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)

        assert errors[0] is None
        assert errors[1] is None
        assert results[0] is not None
        assert results[1] is not None
        assert api_calls[0] == 1, "API should only be called once due to coalescing"


class TestValidateBars:
    def test_drops_bars_with_non_finite_prices(self):
        bars = [
            _make_bar(0),
            DailyBar(
                trade_date=date(2024, 1, 3),
                open_price=float("inf"),
                high_price=101.0,
                low_price=99.0,
                close_price=100.0,
                volume=1_000_000,
            ),
        ]
        result = MarketDataService._validate_bars(bars, "TEST")
        assert len(result) == 1

    def test_drops_bars_with_zero_volume(self):
        bars = [
            _make_bar(0),
            DailyBar(
                trade_date=date(2024, 1, 3),
                open_price=100.0,
                high_price=101.0,
                low_price=99.0,
                close_price=100.5,
                volume=0,
            ),
        ]
        result = MarketDataService._validate_bars(bars, "TEST")
        assert len(result) == 1

    def test_deduplicates_same_date(self):
        same_date = date(2024, 1, 2)
        bars = [
            DailyBar(trade_date=same_date, open_price=100, high_price=101, low_price=99, close_price=100, volume=1000),
            DailyBar(trade_date=same_date, open_price=200, high_price=201, low_price=199, close_price=200, volume=2000),
        ]
        result = MarketDataService._validate_bars(bars, "TEST")
        assert len(result) == 1

    def test_sorts_by_date(self):
        bars = [_make_bar(5), _make_bar(0), _make_bar(2)]
        result = MarketDataService._validate_bars(bars, "TEST")
        dates = [b.trade_date for b in result]
        assert dates == sorted(dates)


class TestServiceClose:
    def test_close_is_idempotent(self):
        svc = _make_service()
        svc.close()
        svc.close()


class TestPrepareBacktestExDividendDates:
    def test_prepare_backtest_loads_and_sets_ex_dividend_dates(self):
        bars = _make_bars(60, base_date=date(2023, 12, 1))
        svc = _make_service(bars)
        svc.client.list_ex_dividend_dates.return_value = {date(2024, 1, 12), date(2024, 1, 26)}

        request = CreateBacktestRunRequest(
            symbol="AAPL",
            strategy_type="long_call",
            start_date=date(2024, 1, 10),
            end_date=date(2024, 1, 31),
            target_dte=30,
            dte_tolerance_days=5,
            max_holding_days=10,
            account_size=10000,
            risk_per_trade_pct=5,
            commission_per_contract=1,
            entry_rules=[{"type": "rsi", "operator": "lte", "threshold": 35, "period": 14}],
        )

        bundle = svc.prepare_backtest(request)

        assert bundle.ex_dividend_dates == {date(2024, 1, 12), date(2024, 1, 26)}
        assert bundle.option_gateway.get_ex_dividend_dates(date(2024, 1, 1), date(2024, 2, 29)) == {
            date(2024, 1, 12),
            date(2024, 1, 26),
        }
        svc.client.list_ex_dividend_dates.assert_called_once()

    def test_prepare_backtest_populates_live_gateway_ex_dividend_cache(self):
        bars = _make_bars(60, base_date=date(2023, 12, 1))
        svc = _make_service(bars)
        svc.client.list_ex_dividend_dates.return_value = {date(2024, 1, 12)}

        request = CreateBacktestRunRequest(
            symbol="AAPL",
            strategy_type="long_call",
            start_date=date(2024, 1, 10),
            end_date=date(2024, 1, 31),
            target_dte=30,
            dte_tolerance_days=5,
            max_holding_days=10,
            account_size=10000,
            risk_per_trade_pct=5,
            commission_per_contract=1,
            entry_rules=[{"type": "rsi", "operator": "lte", "threshold": 35, "period": 14}],
        )

        bundle = svc.prepare_backtest(request)

        assert bundle.option_gateway._ex_dividend_dates == {date(2024, 1, 12)}

    def test_prepare_backtest_degrades_gracefully_when_ex_dividend_dates_fail(self):
        bars = _make_bars(60, base_date=date(2023, 12, 1))
        svc = _make_service(bars)
        svc.client.list_ex_dividend_dates.side_effect = ExternalServiceError("provider failed")

        request = CreateBacktestRunRequest(
            symbol="AAPL",
            strategy_type="long_call",
            start_date=date(2024, 1, 10),
            end_date=date(2024, 1, 31),
            target_dte=30,
            dte_tolerance_days=5,
            max_holding_days=10,
            account_size=10000,
            risk_per_trade_pct=5,
            commission_per_contract=1,
            entry_rules=[{"type": "rsi", "operator": "lte", "threshold": 35, "period": 14}],
        )

        bundle = svc.prepare_backtest(request)

        assert bundle.ex_dividend_dates == set()
