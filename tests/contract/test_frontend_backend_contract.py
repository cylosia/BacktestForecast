"""Contract tests verifying frontend TypeScript types match backend Pydantic schemas.

These tests ensure that fields added to the backend are reflected in the
TypeScript API client types, preventing silent frontend-backend drift.
"""
from __future__ import annotations

import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TS_SCHEMA_PATH = PROJECT_ROOT / "packages" / "api-client" / "src" / "schema.d.ts"


def _read_ts_schema() -> str:
    assert TS_SCHEMA_PATH.exists(), f"TypeScript schema not found: {TS_SCHEMA_PATH}"
    return TS_SCHEMA_PATH.read_text(encoding="utf-8")


def test_ts_schema_has_decided_trades():
    """BacktestSummaryResponse must include decided_trades for win rate context."""
    ts = _read_ts_schema()
    assert "decided_trades" in ts, (
        "TypeScript schema missing 'decided_trades' field. "
        "Regenerate with: pnpm --filter @backtestforecast/api-client generate"
    )


def test_ts_schema_has_equity_curve_truncated():
    """Backtest detail payload must expose equity_curve_truncated to the frontend."""
    ts = _read_ts_schema()
    assert "equity_curve_truncated" in ts, (
        "TypeScript schema missing 'equity_curve_truncated' field. "
        "Regenerate with: pnpm --filter @backtestforecast/api-client generate"
    )


def test_ts_schema_has_compare_truncation_flag():
    """Compare payload must expose trades_truncated to the frontend."""
    ts = _read_ts_schema()
    assert "trades_truncated" in ts, (
        "TypeScript schema missing 'trades_truncated' field. "
        "Regenerate with: pnpm --filter @backtestforecast/api-client generate"
    )


def test_ts_schema_has_holding_period_trading_days_on_trade():
    """BacktestTradeResponse must include holding_period_trading_days."""
    ts = _read_ts_schema()
    assert "holding_period_trading_days" in ts, (
        "TypeScript schema missing 'holding_period_trading_days' field. "
        "Regenerate with: pnpm --filter @backtestforecast/api-client generate"
    )


def test_ts_schema_entry_mid_has_description():
    """entry_mid field must have a description explaining the /100 convention."""
    ts = _read_ts_schema()
    assert "Per-unit position value divided by 100" in ts, (
        "TypeScript schema entry_mid missing divided-by-100 description"
    )


def test_ts_schema_exit_mid_has_description():
    """exit_mid field must have a description matching entry_mid convention."""
    ts = _read_ts_schema()
    assert "Same convention as entry_mid" in ts


def test_backend_decided_trades_field_exists():
    """Backend BacktestSummaryResponse must have decided_trades."""
    from backtestforecast.schemas.backtests import BacktestSummaryResponse
    assert "decided_trades" in BacktestSummaryResponse.model_fields


def test_backend_holding_period_trading_days_exists():
    """Backend BacktestTradeResponse must have holding_period_trading_days."""
    from backtestforecast.schemas.backtests import BacktestTradeResponse
    assert "holding_period_trading_days" in BacktestTradeResponse.model_fields


def test_backend_entry_mid_description():
    """Backend entry_mid must have a description explaining the convention."""
    from backtestforecast.schemas.backtests import BacktestTradeResponse
    desc = BacktestTradeResponse.model_fields["entry_mid"].description
    assert desc is not None and "100" in desc


def test_export_format_enum_matches_db_constraint():
    """ExportFormat enum values must match the DB CHECK constraint."""
    from backtestforecast.billing.entitlements import ExportFormat
    enum_values = {e.value for e in ExportFormat}
    assert enum_values == {"csv", "pdf"}, (
        f"ExportFormat enum {enum_values} does not match DB CHECK constraint"
    )


def test_trade_json_response_has_trading_days():
    """TradeJsonResponse (scanner/sweep) must also have the trading days field."""
    from backtestforecast.schemas.backtests import TradeJsonResponse
    assert "holding_period_trading_days" in TradeJsonResponse.model_fields


# ---- Expanded schema coverage ----

def test_backtest_run_detail_response_fields():
    """BacktestRunDetailResponse must have core fields."""
    from backtestforecast.schemas.backtests import BacktestRunDetailResponse
    fields = BacktestRunDetailResponse.model_fields
    for f in ("id", "symbol", "strategy_type", "status", "summary", "trades", "equity_curve", "equity_curve_truncated", "risk_free_rate"):
        assert f in fields, f"BacktestRunDetailResponse missing field: {f}"


def test_backtest_summary_core_fields_match_frontend_contract():
    """Core summary fields used by the frontend must exist in both backend and generated TS types."""
    from backtestforecast.schemas.backtests import BacktestSummaryResponse

    required = {
        "trade_count",
        "decided_trades",
        "win_rate",
        "total_roi_pct",
        "total_net_pnl",
        "max_drawdown_pct",
        "profit_factor",
        "sharpe_ratio",
        "sortino_ratio",
        "expectancy",
    }
    backend_fields = set(BacktestSummaryResponse.model_fields.keys())
    assert required.issubset(backend_fields)

    ts = _read_ts_schema()
    for field_name in required:
        assert field_name in ts, (
            f"TypeScript schema missing summary field '{field_name}'. "
            "Regenerate with: pnpm --filter @backtestforecast/api-client generate"
        )


def test_compare_response_fields_match_frontend_contract():
    """Compare response must expose summary items and truncation metadata end to end."""
    from backtestforecast.schemas.backtests import CompareBacktestsResponse

    fields = CompareBacktestsResponse.model_fields
    for field_name in ("items", "comparison_limit", "trade_limit_per_run", "trades_truncated"):
        assert field_name in fields


def test_cursor_paginated_list_responses_share_contract_fields():
    from backtestforecast.schemas.analysis import AnalysisListResponse
    from backtestforecast.schemas.backtests import BacktestRunListResponse
    from backtestforecast.schemas.exports import ExportJobListResponse
    from backtestforecast.schemas.scans import ScannerJobListResponse
    from backtestforecast.schemas.sweeps import SweepJobListResponse

    expected = {"items", "total", "offset", "limit", "next_cursor"}
    for model in (
        BacktestRunListResponse,
        ExportJobListResponse,
        ScannerJobListResponse,
        SweepJobListResponse,
        AnalysisListResponse,
    ):
        assert expected.issubset(model.model_fields.keys()), model.__name__


def test_scanner_recommendation_response_fields():
    """ScannerRecommendationResponse must have core fields."""
    from backtestforecast.schemas.scans import ScannerRecommendationResponse
    fields = ScannerRecommendationResponse.model_fields
    for f in ("rank", "score", "symbol", "strategy_type", "summary"):
        assert f in fields, f"ScannerRecommendationResponse missing field: {f}"


def test_sweep_result_response_fields():
    """SweepResultResponse must have core fields."""
    from backtestforecast.schemas.sweeps import SweepResultResponse
    fields = SweepResultResponse.model_fields
    for f in ("rank", "score", "strategy_type", "summary"):
        assert f in fields, f"SweepResultResponse missing field: {f}"


def test_current_user_response_fields():
    """CurrentUserResponse must have user + features + usage."""
    from backtestforecast.schemas.backtests import CurrentUserResponse
    fields = CurrentUserResponse.model_fields
    for f in ("id", "plan_tier", "features", "usage"):
        assert f in fields, f"CurrentUserResponse missing field: {f}"


def test_strategy_type_enum_completeness():
    """StrategyType enum must have at least 25 strategies."""
    from backtestforecast.schemas.backtests import StrategyType
    assert len(StrategyType) >= 25


def test_job_status_enum_values():
    """JobStatus must include all standard values."""
    from backtestforecast.schemas.common import JobStatus
    values = {s.value for s in JobStatus}
    for v in ("queued", "running", "succeeded", "failed", "cancelled", "expired"):
        assert v in values, f"JobStatus missing value: {v}"


def test_plan_tier_enum_values():
    """PlanTier must include free, pro, premium."""
    from backtestforecast.schemas.common import PlanTier
    values = {t.value for t in PlanTier}
    assert values == {"free", "pro", "premium"}


def test_error_response_structure():
    """ErrorResponse must wrap an ErrorDetail."""
    from backtestforecast.schemas.common import ErrorResponse
    fields = ErrorResponse.model_fields
    assert "error" in fields


def test_state_machine_covers_all_statuses():
    """The job state machine must have transitions for all known statuses."""
    from backtestforecast.job_states import ALLOWED_TRANSITIONS
    from backtestforecast.schemas.common import JobStatus
    for status in JobStatus:
        assert status.value in ALLOWED_TRANSITIONS, (
            f"Status '{status.value}' missing from ALLOWED_TRANSITIONS"
        )


def test_compare_backtests_max_ids():
    """Frontend and backend must agree on max compare IDs (8)."""
    from backtestforecast.schemas.backtests import CompareBacktestsRequest
    run_ids_field = CompareBacktestsRequest.model_fields["run_ids"]
    assert run_ids_field.metadata is not None
    max_length = None
    for m in run_ids_field.metadata:
        if hasattr(m, "max_length"):
            max_length = m.max_length
            break
    assert max_length == 8, (
        f"CompareBacktestsRequest.run_ids max_length should be 8, got {max_length}. "
        "Frontend client.ts must use the same limit."
    )


def test_backtest_window_max_days():
    """Backend max_backtest_window_days must be documented for frontend alignment."""
    from backtestforecast.config import get_settings
    settings = get_settings()
    assert settings.max_backtest_window_days <= 1825, (
        "max_backtest_window_days exceeds 1825 (5 years). "
        "If this is intentional, update frontend validation.ts to match."
    )


def test_scanner_window_limit_and_error_semantics_match_backend() -> None:
    """Frontend scanner limit constant and message should stay aligned with backend validation semantics."""
    from datetime import date
    from decimal import Decimal

    import pytest
    from pydantic import ValidationError as PydanticValidationError

    from backtestforecast.config import get_settings
    from backtestforecast.schemas.scans import CreateScannerJobRequest

    constants_ts = (PROJECT_ROOT / "apps" / "web" / "lib" / "scanner" / "constants.ts").read_text(encoding="utf-8")
    match = re.search(r"export const MAX_SCANNER_WINDOW_DAYS = (\d+);", constants_ts)
    assert match is not None, "Frontend scanner constants must export MAX_SCANNER_WINDOW_DAYS"

    max_days = get_settings().max_scanner_window_days
    assert int(match.group(1)) == max_days

    with pytest.raises(PydanticValidationError, match=re.escape(
        f"scanner window exceeds the configured maximum of {max_days} days"
    )):
        CreateScannerJobRequest(
            mode="basic",
            symbols=["AAPL"],
            strategy_types=["long_call"],
            rule_sets=[
                {
                    "name": "Default",
                    "entry_rules": [
                        {"type": "rsi", "operator": "lte", "threshold": Decimal("35"), "period": 14},
                    ],
                }
            ],
            start_date=date(2023, 1, 1),
            end_date=date(2025, 1, 1),
            target_dte=30,
            dte_tolerance_days=5,
            max_holding_days=20,
            account_size=Decimal("10000"),
            risk_per_trade_pct=Decimal("5"),
            commission_per_contract=Decimal("1"),
        )
