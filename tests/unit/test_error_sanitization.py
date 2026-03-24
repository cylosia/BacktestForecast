"""Test 74: Verify error message sanitization across response schemas.

BacktestRunDetailResponse, AnalysisSummaryResponse, and PipelineHistoryItemResponse
must all sanitize error messages containing SQL, tracebacks, or file paths.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

from backtestforecast.schemas.common import sanitize_error_message

_SQL_ERROR = "SELECT * FROM users WHERE id = 1; DROP TABLE users;"
_TRACEBACK_ERROR = 'Traceback (most recent call last):\n  File "/app/main.py", line 42'
_FILE_PATH_LINUX = "Error in /home/deploy/app/services/billing.py line 99"
_FILE_PATH_WINDOWS = r"Error in C:\Users\Admin\project\services\billing.py line 99"
_INTERNAL_URL = "Failed connecting to http://localhost:8000/internal/health"
_PSYCOPG_ERROR = "psycopg.OperationalError: connection refused"
_SQLALCHEMY_ERROR = "sqlalchemy.exc.IntegrityError: duplicate key"
_SAFE_MESSAGE = "Analysis failed due to insufficient data."
_SANITIZED = "An internal error occurred."


class TestSanitizeErrorMessage:
    def test_none_returns_none(self):
        assert sanitize_error_message(None) is None

    def test_safe_message_passes_through(self):
        assert sanitize_error_message(_SAFE_MESSAGE) == _SAFE_MESSAGE

    def test_sql_redacted(self):
        assert sanitize_error_message(_SQL_ERROR) == _SANITIZED

    def test_traceback_redacted(self):
        assert sanitize_error_message(_TRACEBACK_ERROR) == _SANITIZED

    def test_linux_file_path_redacted(self):
        assert sanitize_error_message(_FILE_PATH_LINUX) == _SANITIZED

    def test_windows_file_path_redacted(self):
        assert sanitize_error_message(_FILE_PATH_WINDOWS) == _SANITIZED

    def test_internal_url_redacted(self):
        assert sanitize_error_message(_INTERNAL_URL) == _SANITIZED

    def test_psycopg_redacted(self):
        assert sanitize_error_message(_PSYCOPG_ERROR) == _SANITIZED

    def test_sqlalchemy_redacted(self):
        assert sanitize_error_message(_SQLALCHEMY_ERROR) == _SANITIZED

    def test_long_message_truncated(self):
        msg = "A" * 600
        result = sanitize_error_message(msg)
        assert result is not None
        assert len(result) <= 503
        assert result.endswith("...")


class TestBacktestRunDetailResponseSanitization:
    def test_sql_in_error_message_sanitized(self):
        from backtestforecast.schemas.backtests import BacktestRunDetailResponse

        data = {
            "id": uuid.uuid4(),
            "symbol": "AAPL",
            "strategy_type": "long_call",
            "status": "failed",
            "date_from": "2024-01-01",
            "date_to": "2024-06-01",
            "target_dte": 30,
            "dte_tolerance_days": 5,
            "max_holding_days": 20,
            "account_size": "10000",
            "risk_per_trade_pct": "5",
            "commission_per_contract": "1",
            "engine_version": "options-multileg-v2",
            "data_source": "massive",
            "created_at": datetime.now(UTC),
            "completed_at": datetime.now(UTC),
            "warnings": [],
            "error_code": "execution_failed",
            "error_message": _SQL_ERROR,
            "summary": {
                "trade_count": 0, "win_rate": 0, "total_roi_pct": 0,
                "average_win_amount": 0, "average_loss_amount": 0,
                "average_holding_period_days": 0, "average_dte_at_open": 0,
                "max_drawdown_pct": 0, "total_commissions": 0, "total_net_pnl": 0,
                "starting_equity": 10000, "ending_equity": 10000,
            },
            "trades": [],
            "equity_curve": [],
        }
        resp = BacktestRunDetailResponse(**data)
        assert resp.error_message == _SANITIZED

    def test_traceback_in_error_message_sanitized(self):
        from backtestforecast.schemas.backtests import BacktestRunDetailResponse

        data = {
            "id": uuid.uuid4(),
            "symbol": "AAPL",
            "strategy_type": "long_call",
            "status": "failed",
            "date_from": "2024-01-01",
            "date_to": "2024-06-01",
            "target_dte": 30,
            "dte_tolerance_days": 5,
            "max_holding_days": 20,
            "account_size": "10000",
            "risk_per_trade_pct": "5",
            "commission_per_contract": "1",
            "engine_version": "options-multileg-v2",
            "data_source": "massive",
            "created_at": datetime.now(UTC),
            "completed_at": datetime.now(UTC),
            "warnings": [],
            "error_message": _TRACEBACK_ERROR,
            "summary": {
                "trade_count": 0, "win_rate": 0, "total_roi_pct": 0,
                "average_win_amount": 0, "average_loss_amount": 0,
                "average_holding_period_days": 0, "average_dte_at_open": 0,
                "max_drawdown_pct": 0, "total_commissions": 0, "total_net_pnl": 0,
                "starting_equity": 10000, "ending_equity": 10000,
            },
            "trades": [],
            "equity_curve": [],
        }
        resp = BacktestRunDetailResponse(**data)
        assert resp.error_message == _SANITIZED


class TestAnalysisSummaryResponseSanitization:
    def test_sql_in_error_message_sanitized(self):
        from backtestforecast.schemas.analysis import AnalysisSummaryResponse

        data = {
            "id": uuid.uuid4(),
            "symbol": "AAPL",
            "status": "failed",
            "stage": "regime",
            "strategies_tested": 0,
            "configs_tested": 0,
            "top_results_count": 0,
            "created_at": datetime.now(UTC),
            "error_message": _SQL_ERROR,
        }
        resp = AnalysisSummaryResponse(**data)
        assert resp.error_message == _SANITIZED

    def test_file_path_in_error_message_sanitized(self):
        from backtestforecast.schemas.analysis import AnalysisSummaryResponse

        data = {
            "id": uuid.uuid4(),
            "symbol": "AAPL",
            "status": "failed",
            "stage": "regime",
            "strategies_tested": 0,
            "configs_tested": 0,
            "top_results_count": 0,
            "created_at": datetime.now(UTC),
            "error_message": _FILE_PATH_LINUX,
        }
        resp = AnalysisSummaryResponse(**data)
        assert resp.error_message == _SANITIZED


class TestPipelineHistoryItemResponseSanitization:
    def test_sql_in_error_message_sanitized(self):
        from backtestforecast.schemas.analysis import PipelineHistoryItemResponse

        data = {
            "id": uuid.uuid4(),
            "trade_date": "2024-06-01",
            "status": "failed",
            "symbols_screened": 0,
            "recommendations_produced": 0,
            "error_message": _SQL_ERROR,
        }
        resp = PipelineHistoryItemResponse(**data)
        assert resp.error_message == _SANITIZED

    def test_traceback_in_error_message_sanitized(self):
        from backtestforecast.schemas.analysis import PipelineHistoryItemResponse

        data = {
            "id": uuid.uuid4(),
            "trade_date": "2024-06-01",
            "status": "failed",
            "symbols_screened": 0,
            "recommendations_produced": 0,
            "error_message": _TRACEBACK_ERROR,
        }
        resp = PipelineHistoryItemResponse(**data)
        assert resp.error_message == _SANITIZED
