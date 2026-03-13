"""Tests for EARNINGS_IMMINENT regime detection and strategy map filtering."""
from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock

from backtestforecast.market_data.types import DailyBar
from backtestforecast.pipeline.regime import Regime, classify_regime
from backtestforecast.pipeline.strategy_map import (
    _EARNINGS_SUPPRESS,
    strategies_for_regime,
)


def _make_bars(count: int = 220, end_date: date | None = None) -> list[DailyBar]:
    """Generate a deterministic series of daily bars for testing."""
    end = end_date or date(2025, 6, 15)
    bars: list[DailyBar] = []
    for i in range(count):
        d = end - timedelta(days=count - 1 - i)
        price = 100.0 + i * 0.05
        bars.append(
            DailyBar(
                trade_date=d,
                open_price=price - 0.5,
                high_price=price + 1.0,
                low_price=price - 1.0,
                close_price=price,
                volume=1_000_000.0,
            )
        )
    return bars


# ---------------------------------------------------------------------------
# Regime classifier: EARNINGS_IMMINENT
# ---------------------------------------------------------------------------


class TestClassifyRegimeEarnings:
    def test_earnings_within_10_days_adds_label(self):
        bars = _make_bars(220)
        last_date = bars[-1].trade_date
        earnings = {last_date + timedelta(days=5)}

        result = classify_regime("TEST", bars, earnings_dates=earnings)

        assert result is not None
        assert Regime.EARNINGS_IMMINENT in result.regimes

    def test_earnings_on_same_day_adds_label(self):
        bars = _make_bars(220)
        last_date = bars[-1].trade_date
        earnings = {last_date}

        result = classify_regime("TEST", bars, earnings_dates=earnings)

        assert result is not None
        assert Regime.EARNINGS_IMMINENT in result.regimes

    def test_earnings_at_day_10_boundary_adds_label(self):
        bars = _make_bars(220)
        last_date = bars[-1].trade_date
        earnings = {last_date + timedelta(days=10)}

        result = classify_regime("TEST", bars, earnings_dates=earnings)

        assert result is not None
        assert Regime.EARNINGS_IMMINENT in result.regimes

    def test_earnings_beyond_10_days_no_label(self):
        bars = _make_bars(220)
        last_date = bars[-1].trade_date
        earnings = {last_date + timedelta(days=11)}

        result = classify_regime("TEST", bars, earnings_dates=earnings)

        assert result is not None
        assert Regime.EARNINGS_IMMINENT not in result.regimes

    def test_earnings_in_past_no_label(self):
        bars = _make_bars(220)
        last_date = bars[-1].trade_date
        earnings = {last_date - timedelta(days=5)}

        result = classify_regime("TEST", bars, earnings_dates=earnings)

        assert result is not None
        assert Regime.EARNINGS_IMMINENT not in result.regimes

    def test_no_earnings_dates_no_label(self):
        bars = _make_bars(220)

        result = classify_regime("TEST", bars)

        assert result is not None
        assert Regime.EARNINGS_IMMINENT not in result.regimes

    def test_empty_earnings_set_no_label(self):
        bars = _make_bars(220)

        result = classify_regime("TEST", bars, earnings_dates=set())

        assert result is not None
        assert Regime.EARNINGS_IMMINENT not in result.regimes


# ---------------------------------------------------------------------------
# Strategy map: earnings filtering
# ---------------------------------------------------------------------------


class TestStrategyMapEarnings:
    def test_no_earnings_returns_unfiltered(self):
        regimes = frozenset({Regime.NEUTRAL, Regime.HIGH_IV})
        result = strategies_for_regime(regimes)
        assert "iron_condor" in result
        assert "short_strangle" in result

    def test_earnings_suppresses_short_premium(self):
        regimes = frozenset({Regime.BULLISH, Regime.HIGH_IV, Regime.EARNINGS_IMMINENT})
        result = strategies_for_regime(regimes)
        for s in _EARNINGS_SUPPRESS:
            if s != "iron_condor":  # iron_condor may appear in fallback (defined-risk)
                assert s not in result

    def test_earnings_keeps_defined_risk(self):
        regimes = frozenset({Regime.NEUTRAL, Regime.LOW_IV, Regime.EARNINGS_IMMINENT})
        result = strategies_for_regime(regimes)
        assert "long_straddle" in result
        assert "long_strangle" in result

    def test_earnings_fallback_when_all_suppressed(self):
        regimes = frozenset({Regime.NEUTRAL, Regime.HIGH_IV, Regime.EARNINGS_IMMINENT})
        result = strategies_for_regime(regimes)
        assert len(result) > 0

    def test_bullish_earnings_keeps_long_call(self):
        regimes = frozenset({Regime.BULLISH, Regime.LOW_IV, Regime.EARNINGS_IMMINENT})
        result = strategies_for_regime(regimes)
        assert "long_call" in result
        assert "covered_call" not in result

    def test_bearish_earnings_keeps_long_put(self):
        regimes = frozenset({Regime.BEARISH, Regime.LOW_IV, Regime.EARNINGS_IMMINENT})
        result = strategies_for_regime(regimes)
        assert "long_put" in result


# ---------------------------------------------------------------------------
# Adapter: graceful degradation
# ---------------------------------------------------------------------------


class TestPipelineMarketDataFetcherEarnings:
    def test_returns_empty_set_on_api_failure(self):
        from backtestforecast.errors import ExternalServiceError
        from backtestforecast.pipeline.adapters import PipelineMarketDataFetcher

        mock_client = MagicMock()
        mock_client.list_earnings_event_dates.side_effect = ExternalServiceError("API down")

        fetcher = PipelineMarketDataFetcher(mock_client)
        result = fetcher.get_earnings_dates("AAPL", date(2025, 1, 1), date(2025, 6, 30))

        assert result == set()

    def test_caches_result_across_calls(self):
        from backtestforecast.pipeline.adapters import PipelineMarketDataFetcher

        expected = {date(2025, 4, 25)}
        mock_client = MagicMock()
        mock_client.list_earnings_event_dates.return_value = expected

        fetcher = PipelineMarketDataFetcher(mock_client)
        r1 = fetcher.get_earnings_dates("AAPL", date(2025, 1, 1), date(2025, 6, 30))
        r2 = fetcher.get_earnings_dates("AAPL", date(2025, 1, 1), date(2025, 6, 30))

        assert r1 == expected
        assert r2 == expected
        assert mock_client.list_earnings_event_dates.call_count == 1

    def test_different_symbols_not_shared(self):
        from backtestforecast.pipeline.adapters import PipelineMarketDataFetcher

        mock_client = MagicMock()
        mock_client.list_earnings_event_dates.side_effect = [
            {date(2025, 4, 25)},
            {date(2025, 7, 15)},
        ]

        fetcher = PipelineMarketDataFetcher(mock_client)
        r1 = fetcher.get_earnings_dates("AAPL", date(2025, 1, 1), date(2025, 6, 30))
        r2 = fetcher.get_earnings_dates("MSFT", date(2025, 1, 1), date(2025, 6, 30))

        assert r1 == {date(2025, 4, 25)}
        assert r2 == {date(2025, 7, 15)}
        assert mock_client.list_earnings_event_dates.call_count == 2
