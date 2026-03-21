from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel
from starlette.requests import Request

from apps.api.app.routers import meta as meta_router
from backtestforecast import __version__
from backtestforecast.backtests.summary import build_summary
from backtestforecast.backtests.types import EquityPointResult, TradeResult
from backtestforecast.config import Settings
from backtestforecast.repositories.audit_events import AuditEventRepository
from backtestforecast.schemas.scans import ScannerRecommendationResponse
from backtestforecast.schemas.sweeps import CreateSweepRequest
from backtestforecast.version import get_public_version


def _trade(net_pnl: float, *, day_offset: int = 0) -> TradeResult:
    entry = date(2025, 1, 2) + timedelta(days=day_offset)
    return TradeResult(
        option_ticker="O:TEST",
        strategy_type="cash_secured_put",
        underlying_symbol="TEST",
        entry_date=entry,
        exit_date=entry + timedelta(days=5),
        expiration_date=entry + timedelta(days=30),
        quantity=1,
        dte_at_open=30,
        holding_period_days=5,
        entry_underlying_close=100.0,
        exit_underlying_close=101.0,
        entry_mid=2.0,
        exit_mid=1.0,
        gross_pnl=net_pnl + 1.0,
        net_pnl=net_pnl,
        total_commissions=1.0,
        entry_reason="entry_rules_met",
        exit_reason="expiration",
    )


def _equity_curve(equities: list[float]) -> list[EquityPointResult]:
    peak = equities[0]
    result: list[EquityPointResult] = []
    for i, eq in enumerate(equities):
        peak = max(peak, eq)
        dd = ((peak - eq) / peak * 100.0) if peak > 0 else 0.0
        result.append(
            EquityPointResult(
                trade_date=date(2025, 1, 2) + timedelta(days=i),
                equity=eq,
                cash=eq,
                position_value=0.0,
                drawdown_pct=dd,
            )
        )
    return result


class DummyRecommendation(BaseModel):
    id: str = "00000000-0000-0000-0000-000000000001"
    rank: int = 1
    score: float = 1.5
    symbol: str = "SPY"
    strategy_type: str = "covered_call"
    rule_set_name: str = "default"
    request_snapshot_json: dict[str, object] = {"symbol": "SPY"}
    summary: dict[str, object] = {
        "trade_count": 6,
        "decided_trades": 6,
        "win_rate": 50.0,
        "total_roi_pct": 10.0,
        "max_drawdown_pct": 5.0,
        "total_net_pnl": 100.0,
        "total_commissions": 6.0,
        "starting_equity": 1000.0,
        "ending_equity": 1100.0,
        "average_win_amount": 50.0,
        "average_loss_amount": -20.0,
        "average_holding_period_days": 5.0,
        "average_dte_at_open": 30.0,
        "profit_factor": 2.0,
        "expectancy": 16.67,
    }
    historical_performance: dict[str, object] = {}
    forecast: dict[str, object] = {
        "symbol": "SPY",
        "as_of_date": "2025-02-01",
        "horizon_days": 30,
        "analog_count": 10,
        "expected_return_low_pct": -5.0,
        "expected_return_median_pct": 2.0,
        "expected_return_high_pct": 8.0,
        "summary": "ok",
        "disclaimer": "demo",
    }
    ranking_breakdown: dict[str, object] = {
        "current_performance_score": 1.0,
        "historical_performance_score": 1.0,
        "forecast_alignment_score": 1.0,
        "final_score": 1.5,
    }


class TestSettingsDefaults:
    def test_low_priority_settings_defaults_exist(self):
        settings = Settings()
        assert settings.me_read_rate_limit == 60
        assert settings.delete_rate_limit == 60
        assert settings.max_sweep_window_days == 730


class TestVersionDerivation:
    def test_meta_router_uses_package_version(self) -> None:
        assert __version__ == get_public_version()

    def test_health_router_uses_package_version(self) -> None:
        assert __version__ == get_public_version()


class TestSweepWindowLimit:
    def test_sweep_uses_sweep_specific_window_limit(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(
            "backtestforecast.schemas.sweeps.get_settings",
            lambda: SimpleNamespace(max_sweep_window_days=30),
        )
        with pytest.raises(ValueError, match="configured maximum of 30 days"):
            CreateSweepRequest(
                symbol="SPY",
                strategy_types=["covered_call"],
                entry_rule_sets=[{"name": "default", "entry_rules": []}],
                start_date=date(2025, 1, 1),
                end_date=date(2025, 2, 5),
                target_dte=45,
                max_holding_days=10,
                account_size="10000",
                risk_per_trade_pct="1",
                commission_per_contract="1",
            )


class TestScannerRecommendationAlias:
    def test_request_snapshot_alias_maps_from_attributes(self):
        rec = ScannerRecommendationResponse.model_validate(DummyRecommendation())
        assert rec.request_snapshot == {"symbol": "SPY"}


class TestAuditEventRepositoryPaging:
    def test_negative_limit_and_offset_are_clamped(self):
        session = MagicMock()
        session.scalars.return_value = []
        repo = AuditEventRepository(session)

        repo.list_recent(limit=-5, offset=-10)

        stmt = session.scalars.call_args.args[0]
        assert stmt._limit_clause.value == 1
        assert stmt._offset_clause.value == 0


class TestSummaryNegativeEquity:
    def test_sharpe_and_sortino_use_positive_prefix_before_negative_equity(self):
        equities = [10000.0]
        for i in range(34):
            equities.append(equities[-1] + (50.0 if i % 2 == 0 else -20.0))
        equities.append(-1.0)
        curve = _equity_curve(equities)
        trades = [_trade(25.0 if i % 2 == 0 else -10.0, day_offset=i * 6) for i in range(6)]

        summary = build_summary(10000.0, equities[-2], trades, curve, risk_free_rate=0.0)

        assert summary.sharpe_ratio is not None
        assert summary.sortino_ratio is not None


class TestMetaGracefulDegradation:
    def test_meta_returns_public_payload_when_db_auth_lookup_fails(self, monkeypatch: pytest.MonkeyPatch):
        request = Request({"type": "http", "headers": []})

        monkeypatch.setattr(meta_router, "_extract_client_ip", lambda _request: None)
        monkeypatch.setattr(meta_router, "get_rate_limiter", lambda: SimpleNamespace(check=lambda **_: None))
        monkeypatch.setattr(meta_router, "get_settings", lambda: SimpleNamespace(
            rate_limit_window_seconds=60,
            stripe_billing_enabled=True,
            feature_backtests_enabled=True,
            feature_scanner_enabled=True,
            feature_exports_enabled=True,
            feature_forecasts_enabled=True,
            feature_analysis_enabled=True,
            feature_daily_picks_enabled=True,
            feature_billing_enabled=True,
            feature_sweeps_enabled=True,
            app_env="production",
        ))
        monkeypatch.setattr(meta_router, "_try_authenticate", lambda _request, _db: (_ for _ in ()).throw(ConnectionError("db down")))

        payload = meta_router.get_meta(request, db=MagicMock())

        assert payload == {
            "service": "backtestforecast-api",
            "version": get_public_version(),
        }


class TestServerApiCaching:
    def test_server_get_helpers_are_request_cached(self):
        source = Path("apps/web/lib/api/server.ts").read_text()

        assert "export const getBacktestHistory = cache(async" in source
        assert "export const getBacktestRun = cache(async" in source
        assert "export const getTemplates = cache(async" in source
        assert "export const getScannerJobs = cache(async" in source
        assert "export const getScannerJob = cache(async" in source
        assert "export const getSweepJobs = cache(async" in source
        assert "export const getSweepJob = cache(async" in source
        assert "export const getDailyPicks = cache(async" in source
        assert "export const getAnalysisHistory = cache(async" in source
        assert "export const getDailyPicksHistory = cache(async" in source


class TestStartupSideEffects:
    def test_strategy_catalog_defers_missing_entry_logging_until_runtime(self):
        source = Path("src/backtestforecast/strategy_catalog/catalog.py").read_text()

        assert "def log_missing_catalog_entries()" in source
        assert "if _missing:" not in source

    def test_worker_sqlite_warning_is_not_logged_at_import_time(self):
        source = Path("tests/worker/test_tasks.py").read_text()

        assert "_sqlite_warning_logged = False" in source
        assert "def db_engine():" in source
