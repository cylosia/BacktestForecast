from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import uuid4


def _trade_payload() -> dict[str, object]:
    return {
        "option_ticker": "AAPL240621C00100000",
        "strategy_type": "long_call",
        "underlying_symbol": "AAPL",
        "entry_date": date(2024, 1, 2).isoformat(),
        "exit_date": date(2024, 1, 10).isoformat(),
        "expiration_date": date(2024, 2, 16).isoformat(),
        "quantity": 1,
        "dte_at_open": 30,
        "holding_period_days": 8,
        "entry_underlying_close": 100.0,
        "exit_underlying_close": 105.0,
        "entry_mid": 2.0,
        "exit_mid": 3.0,
        "gross_pnl": 100.0,
        "net_pnl": 99.0,
        "total_commissions": 1.0,
        "entry_reason": "signal",
        "exit_reason": "target",
        "detail_json": {},
    }


def _equity_payload() -> dict[str, object]:
    return {
        "trade_date": date(2024, 1, 2).isoformat(),
        "equity": 10000.0,
        "cash": 9800.0,
        "position_value": 200.0,
        "drawdown_pct": 0.0,
    }


def _summary_payload(trade_count: int) -> dict[str, object]:
    return {
        "trade_count": trade_count,
        "total_commissions": 10.0,
        "total_net_pnl": 250.0,
        "starting_equity": 10000.0,
        "ending_equity": 10250.0,
        "win_rate": 60.0,
        "total_roi_pct": 2.5,
        "max_drawdown_pct": 1.5,
    }


def test_scanner_recommendation_uses_persisted_trade_counts_for_truncation() -> None:
    from backtestforecast.models import ScannerRecommendation
    from backtestforecast.services.scans import ScanService

    rec = ScannerRecommendation(
        id=uuid4(),
        scanner_job_id=uuid4(),
        rank=1,
        score=Decimal("1.0"),
        symbol="AAPL",
        strategy_type="long_call",
        rule_set_name="baseline",
        rule_set_hash="abc123",
        request_snapshot_json={"symbol": "AAPL"},
        summary_json=_summary_payload(trade_count=55),
        warnings_json=[],
        historical_performance_json=None,
        forecast_json=None,
        ranking_features_json={
            "current_performance_score": 1.0,
            "historical_performance_score": 1.0,
            "forecast_alignment_score": 1.0,
            "final_score": 1.0,
            "trade_count": 55,
            "serialized_trade_count": 50,
        },
        trades_json=[_trade_payload() for _ in range(50)],
        equity_curve_json=[_equity_payload()],
        created_at=datetime.now(UTC),
    )

    response = ScanService._to_recommendation_response(rec)

    assert response.summary.trade_count == 55
    assert len(response.trades) == 50
    assert response.trades_truncated is True
    assert response.trade_items_omitted == 5
    assert response.equity_curve_points_omitted == 0


def test_sweep_result_uses_persisted_trade_counts_for_truncation() -> None:
    from backtestforecast.models import SweepResult
    from backtestforecast.services.sweeps import SweepService

    result = SweepResult(
        id=uuid4(),
        sweep_job_id=uuid4(),
        rank=1,
        score=Decimal("0.5"),
        strategy_type="bull_put_spread",
        parameter_snapshot_json={
            "strategy_type": "bull_put_spread",
            "trade_count": 55,
            "serialized_trade_count": 50,
        },
        summary_json=_summary_payload(trade_count=55),
        warnings_json=[],
        trades_json=[_trade_payload() for _ in range(50)],
        equity_curve_json=[_equity_payload()],
        created_at=datetime.now(UTC),
    )

    response = SweepService._to_result_response(result)

    assert response.summary.trade_count == 55
    assert len(response.trades_json) == 50
    assert response.trades_truncated is True
    assert response.trade_items_omitted == 5
    assert response.equity_curve_points_omitted == 0
