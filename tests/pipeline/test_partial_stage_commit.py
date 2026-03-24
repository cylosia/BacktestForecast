"""Test 67: Partial stage persistence on failure.

Verifies that when a later stage (e.g. forecast) fails during
execute_analysis, data from earlier completed stages is preserved
in the database and the analysis record reflects the failure with
the correct stage marker.
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from backtestforecast.pipeline.deep_analysis import SymbolDeepAnalysisService


class TestPartialStageCommit:
    """Verify that stage data is committed incrementally so partial
    results survive a later-stage failure."""

    @staticmethod
    def _mock_session():
        session = MagicMock()
        session.scalar = MagicMock()
        session.commit = MagicMock()
        session.rollback = MagicMock()
        session.add = MagicMock()
        session.get = MagicMock()
        session.refresh = MagicMock()
        session.execute = MagicMock()
        return session

    def test_commit_called_after_each_stage(self):
        """Verify session.commit() is called after regime, landscape,
        deep_dive, and forecast stages."""
        session = self._mock_session()

        service = SymbolDeepAnalysisService(
            session,
            market_data_fetcher=MagicMock(),
            backtest_executor=MagicMock(),
            forecaster=None,
        )

        analysis_id = uuid4()
        mock_analysis = MagicMock()
        mock_analysis.id = analysis_id
        mock_analysis.symbol = "AAPL"
        mock_analysis.user_id = uuid4()
        mock_analysis.created_at = datetime(2025, 5, 31, 12, 0, 0)
        mock_analysis.started_at = None
        mock_analysis.status = "queued"
        mock_analysis.stage = "pending"
        mock_analysis.strategies_tested = None
        mock_analysis.configs_tested = None
        mock_analysis.top_results_count = None
        mock_analysis.regime_json = None
        mock_analysis.landscape_json = None
        mock_analysis.top_results_json = None
        mock_analysis.forecast_json = None

        mock_user = MagicMock()
        mock_user.id = mock_analysis.user_id
        mock_user.plan_tier = "pro"
        mock_user.subscription_status = "active"
        mock_user.subscription_current_period_end = None

        session.scalar.side_effect = [mock_analysis, 0]

        def get_side_effect(model, key):
            if key == mock_analysis.user_id:
                return mock_user
            if key == analysis_id:
                return mock_analysis
            return None

        session.get.side_effect = get_side_effect

        mock_regime = MagicMock()
        mock_regime.regimes = []
        mock_regime.rsi_14 = 50.0
        mock_regime.ema_8 = 100.0
        mock_regime.ema_21 = 99.0
        mock_regime.sma_50 = 98.0
        mock_regime.sma_200 = 97.0
        mock_regime.realized_vol_20 = 0.15
        mock_regime.iv_rank_proxy = 30.0
        mock_regime.volume_ratio = 1.2
        mock_regime.close_price = 150.0

        mock_policy = MagicMock()
        mock_policy.forecasting_access = True
        mock_policy.tier.value = "pro"

        with patch.object(service, "_market_data") as mock_md, \
             patch("backtestforecast.pipeline.deep_analysis.classify_regime", return_value=mock_regime), \
             patch("backtestforecast.pipeline.deep_analysis.validate_json_shape"), \
             patch("backtestforecast.billing.entitlements.resolve_feature_policy", return_value=mock_policy), \
             patch("backtestforecast.utils.dates.market_date_today", return_value=datetime(2025, 6, 1).date()), \
             patch.object(service, "_build_landscape", return_value=[]), \
             patch.object(service, "_deep_dive", return_value=[]):
            mock_md.get_daily_bars.return_value = []
            mock_md.get_earnings_dates.return_value = set()

            service.execute_analysis(analysis_id)

        commit_count = session.commit.call_count
        assert commit_count >= 4, (
            f"Expected at least 4 commits (status->running, regime, landscape, "
            f"deep_dive/forecast), got {commit_count}"
        )

    def test_failure_at_forecast_preserves_earlier_stage_marker(self):
        """When the forecast stage fails, the analysis status should be
        'failed' and the stage should reflect where it failed."""
        session = self._mock_session()

        service = SymbolDeepAnalysisService(
            session,
            market_data_fetcher=MagicMock(),
            backtest_executor=MagicMock(),
            forecaster=None,
        )

        analysis_id = uuid4()
        mock_analysis = MagicMock()
        mock_analysis.id = analysis_id
        mock_analysis.symbol = "AAPL"
        mock_analysis.user_id = uuid4()
        mock_analysis.created_at = datetime(2025, 5, 31, 12, 0, 0)
        mock_analysis.started_at = None
        mock_analysis.status = "queued"
        mock_analysis.stage = "pending"
        mock_analysis.strategies_tested = 0
        mock_analysis.configs_tested = 0
        mock_analysis.top_results_count = 0
        mock_analysis.regime_json = None
        mock_analysis.landscape_json = None
        mock_analysis.top_results_json = None
        mock_analysis.forecast_json = None

        mock_user = MagicMock()
        mock_user.id = mock_analysis.user_id
        mock_user.plan_tier = "pro"
        mock_user.subscription_status = "active"
        mock_user.subscription_current_period_end = None

        session.scalar.side_effect = [mock_analysis, 0]

        def get_side_effect(model, key):
            if key == mock_analysis.user_id:
                return mock_user
            if key == analysis_id:
                return mock_analysis
            return None

        session.get.side_effect = get_side_effect

        mock_regime = MagicMock()
        mock_regime.regimes = []
        mock_regime.rsi_14 = 50.0
        mock_regime.ema_8 = 100.0
        mock_regime.ema_21 = 99.0
        mock_regime.sma_50 = 98.0
        mock_regime.sma_200 = 97.0
        mock_regime.realized_vol_20 = 0.15
        mock_regime.iv_rank_proxy = 30.0
        mock_regime.volume_ratio = 1.2
        mock_regime.close_price = 150.0

        mock_top_result = MagicMock()
        mock_top_result.rank = 1
        mock_top_result.strategy_type = "long_call"
        mock_top_result.strategy_label = "Long Call"
        mock_top_result.target_dte = 30
        mock_top_result.config_snapshot = {}
        mock_top_result.summary = {}
        mock_top_result.trades = []
        mock_top_result.equity_curve = []
        mock_top_result.forecast = {"some": "forecast"}
        mock_top_result.score = 1.0

        commit_count = 0
        forecast_commit_failed = False

        def commit_side_effect():
            nonlocal commit_count, forecast_commit_failed
            commit_count += 1
            if mock_analysis.stage == "forecast" and not forecast_commit_failed:
                forecast_commit_failed = True
                raise RuntimeError("Forecast computation failed")

        session.commit.side_effect = commit_side_effect

        mock_policy = MagicMock()
        mock_policy.forecasting_access = True
        mock_policy.tier.value = "pro"

        with patch.object(service, "_market_data") as mock_md, \
             patch("backtestforecast.pipeline.deep_analysis.classify_regime", return_value=mock_regime), \
             patch("backtestforecast.pipeline.deep_analysis.validate_json_shape"), \
             patch("backtestforecast.billing.entitlements.resolve_feature_policy", return_value=mock_policy), \
             patch("backtestforecast.utils.dates.market_date_today", return_value=datetime(2025, 6, 1).date()), \
             patch.object(service, "_build_landscape", return_value=[]), \
             patch.object(service, "_deep_dive", return_value=[mock_top_result]):
            mock_md.get_daily_bars.return_value = []
            mock_md.get_earnings_dates.return_value = set()

            with pytest.raises(RuntimeError, match="Forecast computation failed"):
                service.execute_analysis(analysis_id)

        assert mock_analysis.status == "failed", (
            f"Expected status 'failed', got '{mock_analysis.status}'"
        )
        assert mock_analysis.stage == "forecast", (
            f"Expected stage 'forecast', got '{mock_analysis.stage}'"
        )
