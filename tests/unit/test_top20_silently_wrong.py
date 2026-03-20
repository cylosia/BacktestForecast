"""Verification tests for the Top 20 Things That Look Correct But May Be Silently Wrong.

Tests cover all 20 items: confirmed issues have fix-verification tests,
false alarms have investigative assertions proving the code is correct.
"""
from __future__ import annotations

import inspect
import math
import warnings
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest


# ---- S1: onupdate vs trigger timezone — FALSE ALARM ----

def test_s1_updated_at_has_timezone():
    from backtestforecast.models import User
    col = User.__table__.columns["updated_at"]
    assert col.type.timezone is True


# ---- S2: lazy='raise' — DESIGN CHOICE ----

def test_s2_all_relationships_lazy_raise():
    from backtestforecast.models import User
    for rel in User.__mapper__.relationships:
        assert rel.lazy == "raise" or rel.lazy == "raise_on_sql"


# ---- S3: _VALID_TARGET_STATUSES correctly excludes expired for non-exports ----

def test_s3_export_valid_statuses_include_expired():
    from backtestforecast.events import _VALID_TARGET_STATUSES, _EXPORT_VALID_TARGET_STATUSES
    assert "expired" not in _VALID_TARGET_STATUSES
    assert "expired" in _EXPORT_VALID_TARGET_STATUSES


def test_s3_fallback_uses_export_set_for_exports():
    from backtestforecast.events import _fallback_persist_status
    source = inspect.getsource(_fallback_persist_status)
    assert '_EXPORT_VALID_TARGET_STATUSES if job_type == "export"' in source


# ---- S4: Sweep score handles None sharpe_ratio ----

@pytest.mark.filterwarnings("ignore::UserWarning")
def test_s4_sweep_score_handles_none_sharpe():
    from backtestforecast.services.sweeps import SweepService
    score = SweepService._score_candidate_from_summary({
        "trade_count": 10,
        "win_rate": 60.0,
        "total_roi_pct": 10.0,
        "max_drawdown_pct": 5.0,
        "sharpe_ratio": None,
    })
    assert isinstance(score, float)
    assert math.isfinite(score)


# ---- S5: _normalize_utc warns on non-UTC TZ ----

def test_s5_normalize_utc_handles_naive():
    from backtestforecast.services.billing import BillingService
    naive = datetime(2025, 6, 15, 12, 0, 0)
    result = BillingService._normalize_utc(naive)
    assert result.tzinfo is not None
    assert result.tzinfo == UTC


def test_s5_normalize_utc_passes_aware():
    from backtestforecast.services.billing import BillingService
    aware = datetime(2025, 6, 15, 12, 0, 0, tzinfo=UTC)
    result = BillingService._normalize_utc(aware)
    assert result is aware


# ---- S6: _coerce_stripe_id handles SDK objects ----

def test_s6_coerce_stripe_id_string():
    from backtestforecast.services.billing import BillingService
    assert BillingService._coerce_stripe_id("sub_123") == "sub_123"


def test_s6_coerce_stripe_id_dict():
    from backtestforecast.services.billing import BillingService
    assert BillingService._coerce_stripe_id({"id": "cus_abc"}) == "cus_abc"


def test_s6_coerce_stripe_id_object_with_id():
    """Stripe SDK returns objects with .id attribute, not plain dicts."""
    from backtestforecast.services.billing import BillingService

    class FakeStripeObj:
        id = "sub_xyz"

    assert BillingService._coerce_stripe_id(FakeStripeObj()) == "sub_xyz"


def test_s6_coerce_stripe_id_none():
    from backtestforecast.services.billing import BillingService
    assert BillingService._coerce_stripe_id(None) is None


# ---- S7: _LOOKS_NUMERIC — FIXED (prior round) ----

def test_s7_looks_numeric_rejects_leading_zeros():
    from backtestforecast.services.exports import _LOOKS_NUMERIC
    assert not _LOOKS_NUMERIC.match("007")
    assert _LOOKS_NUMERIC.match("0")
    assert _LOOKS_NUMERIC.match("42")


# ---- S8: Negative equity warning ----

def test_s8_engine_warns_on_negative_equity():
    from backtestforecast.backtests.engine import OptionsBacktestEngine
    source = inspect.getsource(OptionsBacktestEngine.run)
    assert "negative_equity" in source


# ---- S9: CAGR capped at extreme values ----

def test_s9_cagr_capped():
    from backtestforecast.backtests.summary import _compute_cagr
    from backtestforecast.backtests.types import EquityPointResult

    curve = [
        EquityPointResult(trade_date=date(2025, 1, 1) + timedelta(days=i),
                          equity=Decimal("10000") + Decimal(str(i * 200)),
                          cash=Decimal("10000"), position_value=Decimal("0"),
                          drawdown_pct=Decimal("0"))
        for i in range(65)
    ]
    ws: list[dict] = []
    result = _compute_cagr(10000.0, 23000.0, curve, warnings=ws)
    assert result is not None
    assert result <= 10_000.0


def test_s9_cagr_cap_warning_emitted():
    from backtestforecast.backtests.summary import _compute_cagr
    from backtestforecast.backtests.types import EquityPointResult

    curve = [
        EquityPointResult(trade_date=date(2025, 1, 1) + timedelta(days=i),
                          equity=Decimal("10000"), cash=Decimal("10000"),
                          position_value=Decimal("0"), drawdown_pct=Decimal("0"))
        for i in range(65)
    ]
    ws: list[dict] = []
    result = _compute_cagr(1000.0, 100000.0, curve, warnings=ws)
    assert result is not None
    codes = [w["code"] for w in ws]
    assert "cagr_capped" in codes


# ---- S10: _D cache — FIXED (prior round) ----

def test_s10_d_cache_exists():
    from backtestforecast.backtests.engine import _D_CACHE
    assert 1 in _D_CACHE
    assert -1 in _D_CACHE


# ---- S11: 1MB body limit — FALSE ALARM ----

def test_s11_body_limit_adequate_for_custom_legs():
    import json
    leg = {"contract_type": "call", "side": "long", "strike_selection": {"method": "delta", "value": 0.3}}
    payload = {"symbol": "AAPL", "strategy_type": "custom_2_leg", "custom_legs": [leg] * 8,
               "start_date": "2024-01-01", "end_date": "2025-01-01", "target_dte": 30,
               "max_holding_days": 30, "account_size": 10000, "risk_per_trade_pct": 5,
               "commission_per_contract": 0.65, "entry_rules": [{"type": "rsi", "threshold": 30}]}
    assert len(json.dumps(payload)) < 1_048_576


# ---- S12: JWT leeway increased to 10s ----

def test_s12_jwt_leeway_at_least_10():
    from backtestforecast.config import Settings
    default = Settings.model_fields["jwt_leeway_seconds"].default
    assert default >= 10


# ---- S13: forecast_max_analogs configurable — FALSE ALARM ----

def test_s13_forecast_max_analogs_configurable():
    from backtestforecast.config import Settings
    assert "forecast_max_analogs" in Settings.model_fields


# ---- S14: Worker uses 300s timeout — FALSE ALARM ----

def test_s14_worker_timeout_300s():
    from backtestforecast.config import Settings
    assert Settings.model_fields["db_worker_statement_timeout_ms"].default == 300_000


# ---- S15: fail_closed rejects immediately when Redis is unavailable ----

def test_s15_fail_closed_rejects_immediately():
    from backtestforecast.security.rate_limits import RateLimiter
    source = inspect.getsource(RateLimiter.check)
    assert "fail_closed_redis_unavailable" in source
    assert "fail_closed_redis_error" in source


# ---- S16: SSE connections separately managed — FALSE ALARM ----

def test_s16_sse_has_process_limit():
    from apps.api.app.routers.events import SSE_MAX_CONNECTIONS_PROCESS
    assert SSE_MAX_CONNECTIONS_PROCESS >= 100


# ---- S17: Outbox max retries increased to 30 ----

def test_s17_outbox_max_retries_increased():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import poll_outbox
    source = inspect.getsource(poll_outbox)
    assert "_OUTBOX_MAX_RETRIES = 30" in source


# ---- S18: result_expires handles timedelta — FALSE ALARM ----

def test_s18_result_expires_handles_timedelta():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import _reap_stale_jobs_inner
    source = inspect.getsource(_reap_stale_jobs_inner)
    assert "isinstance(_result_expires, timedelta)" in source


# ---- S19: IN clause chunked to avoid query bloat ----

def test_s19_orphan_batch_chunked():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import _process_orphan_batch, _ORPHAN_IN_CHUNK_SIZE
    assert _ORPHAN_IN_CHUNK_SIZE <= 200
    source = inspect.getsource(_process_orphan_batch)
    assert "_ORPHAN_IN_CHUNK_SIZE" in source


# ---- S20: scan_timeout reduced for larger cleanup gap ----

def test_s20_scan_timeout_leaves_cleanup_gap():
    from backtestforecast.config import Settings
    default_timeout = Settings.model_fields["scan_timeout_seconds"].default
    celery_soft_limit = 600
    gap = celery_soft_limit - default_timeout
    assert gap >= 120, f"Gap is only {gap}s; need at least 120s for cleanup"
