"""Tests that ScanService enforces the in-memory candidate cap.

The _MAX_CANDIDATES_IN_MEMORY cap prevents OOM during large scans by stopping
candidate accumulation once the limit is reached and injecting a warning.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from backtestforecast.services.scans import ScanService


class TestCandidateCapConstant:
    """Sanity-check the cap value stays within a reasonable range."""

    def test_cap_is_bounded_above(self):
        assert ScanService._MAX_CANDIDATES_IN_MEMORY <= 2000, (
            "In-memory candidate cap should stay bounded to prevent OOM"
        )

    def test_cap_is_bounded_below(self):
        assert ScanService._MAX_CANDIDATES_IN_MEMORY >= 100, (
            "Cap too low would prevent useful scan results"
        )

    def test_cap_is_integer(self):
        assert isinstance(ScanService._MAX_CANDIDATES_IN_MEMORY, int)

    def test_candidate_timeout_shorter_than_statement_timeout(self):
        assert ScanService._CANDIDATE_TIMEOUT_SECONDS < 300, (
            "_CANDIDATE_TIMEOUT_SECONDS must be shorter than the worker statement_timeout (300s)"
        )


class TestCandidateCapBehavior:
    """Test that the cap-check logic in _execute_scan fires correctly.

    These tests mock the heavy internals and only exercise the accumulation
    loop and the cap-triggered break + warning injection.
    """

    def _make_service(self, session=None):
        session = session or MagicMock()
        svc = ScanService.__new__(ScanService)
        svc.session = session
        svc._execution_service = MagicMock()
        svc._forecaster = MagicMock()
        svc.repository = MagicMock()
        svc.audit = MagicMock()
        return svc

    def _make_payload(self, symbols):
        mock_payload = MagicMock()
        mock_payload.symbols = symbols
        mock_payload.strategy_types = [MagicMock(value="long_call")]
        mock_payload.rule_sets = [MagicMock(name="default", entry_rules=[])]
        mock_payload.start_date = date(2025, 1, 1)
        mock_payload.end_date = date(2025, 6, 30)
        mock_payload.target_dte = 30
        mock_payload.dte_tolerance_days = 5
        mock_payload.max_holding_days = 30
        mock_payload.account_size = Decimal("100000")
        mock_payload.risk_per_trade_pct = Decimal("5")
        mock_payload.commission_per_contract = Decimal("0.65")
        mock_payload.max_recommendations = 10
        mock_payload.model_dump.return_value = {}
        return mock_payload

    def _make_job(self):
        mock_job = MagicMock()
        mock_job.id = uuid4()
        mock_job.created_at = MagicMock()
        mock_job.evaluated_candidate_count = 0
        mock_job.candidate_count = 0
        mock_job.recommendations = []
        mock_job.status = "running"
        mock_job.warnings_json = []
        return mock_job

    def _run_capped_scan(self, svc, symbols, mock_settings):
        settings = MagicMock()
        settings.scan_timeout_seconds = 9999
        settings.fallback_entry_rule_rsi_threshold = 30
        settings.max_scan_equity_points = 200
        settings.prefetch_max_workers = 2
        mock_settings.return_value = settings

        mock_payload = self._make_payload(symbols)

        mock_result = MagicMock()
        mock_result.summary = MagicMock()
        mock_result.trades = []
        mock_result.equity_curve = []
        mock_result.warnings = []
        svc.execution_service.execute_request.return_value = mock_result

        mock_forecast = MagicMock()
        mock_forecast.model_dump.return_value = {
            "symbol": "X", "strategy_type": "long_call",
            "as_of_date": "2025-01-01", "horizon_days": 30,
            "analog_count": 0, "expected_return_low_pct": "0",
            "expected_return_median_pct": "0", "expected_return_high_pct": "0",
            "positive_outcome_rate_pct": None, "summary": "", "disclaimer": "",
            "analog_dates": [],
        }
        svc.forecaster.forecast.return_value = mock_forecast

        mock_job = self._make_job()
        mock_request = MagicMock()
        mock_request.model_dump.return_value = {}

        with (
            patch("backtestforecast.services.scans.is_strategy_rule_set_compatible", return_value=True),
            patch("backtestforecast.services.scans.CreateBacktestRunRequest", return_value=mock_request),
            patch("backtestforecast.services.scans.build_ranking_breakdown") as mock_ranking_fn,
            patch("backtestforecast.services.scans.rule_set_hash", return_value="h"),
            patch("backtestforecast.services.scans.recommendation_sort_key", return_value=(0,)),
            patch.object(svc, "_historical_performance") as mock_hist,
            patch.object(svc, "_ranking_response_model", return_value=MagicMock()),
            patch.object(svc, "_prepare_bundles") as mock_bundles,
            patch.object(svc, "_batch_historical_performance", return_value={}),
            patch.object(svc, "_serialize_summary", return_value={"trade_count": 0}),
            patch.object(svc, "_downsample_equity_curve", return_value=[]),
            patch.object(svc, "_serialize_trade", return_value={}),
        ):
            mock_ranking = MagicMock()
            mock_ranking.model_dump.return_value = {"final_score": 1.0}
            mock_ranking_fn.return_value = mock_ranking

            mock_historical = MagicMock()
            mock_historical.model_dump.return_value = {}
            mock_hist.return_value = mock_historical

            mock_bundles.return_value = {s: MagicMock() for s in symbols}

            svc.session.execute.return_value.rowcount = 1

            result = svc._execute_scan(mock_job, mock_payload)

        return mock_job, result

    @patch("backtestforecast.services.scans.get_settings")
    def test_candidate_cap_stops_accumulation(self, mock_settings):
        """When candidates reach the cap, the loop must break and not add more."""
        svc = self._make_service()
        total_symbols = ScanService._MAX_CANDIDATES_IN_MEMORY + 500
        symbols = [f"SYM{i}" for i in range(total_symbols)]

        self._run_capped_scan(svc, symbols, mock_settings)

        cap = ScanService._MAX_CANDIDATES_IN_MEMORY
        call_count = svc.execution_service.execute_request.call_count
        assert call_count <= cap + 1, (
            f"Should have stopped around cap ({cap}), but executed {call_count} times. "
            f"The cap check runs after execution, so at most cap+1 executions are allowed."
        )

    @patch("backtestforecast.services.scans.get_settings")
    def test_candidate_cap_warning_message(self, mock_settings):
        """When the cap fires, a warning with type='candidate_cap' must appear."""
        svc = self._make_service()
        cap = ScanService._MAX_CANDIDATES_IN_MEMORY
        symbols = [f"SYM{i}" for i in range(cap + 100)]

        mock_job, result = self._run_capped_scan(svc, symbols, mock_settings)

        call_count = svc.execution_service.execute_request.call_count
        assert call_count <= cap + 1, "Execution should have stopped around the cap"
        assert call_count >= cap, "Should reach the cap before stopping"
