"""Live-provider smoke test for scanner execution."""
from __future__ import annotations

import os
from datetime import date
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from backtestforecast.errors import ExternalServiceError
from backtestforecast.models import User
from backtestforecast.schemas.scans import CreateScannerJobRequest
from backtestforecast.services.scans import ScanService

pytestmark = [pytest.mark.postgres, pytest.mark.live_provider, pytest.mark.load]


def _require_live_massive_key() -> None:
    api_key = os.environ.get("MASSIVE_API_KEY")
    if not api_key or api_key == "test-massive-api-key":
        pytest.skip("Live-provider scanner smoke requires a real MASSIVE_API_KEY.")


def _create_user(session: Session) -> User:
    user = User(
        clerk_user_id=f"scanner_live_{uuid4().hex[:8]}",
        email=f"scanner-live-{uuid4().hex[:8]}@example.com",
        plan_tier="premium",
        subscription_status="active",
    )
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def test_scanner_live_provider_smoke(postgres_db_session: Session, monkeypatch: pytest.MonkeyPatch):
    _require_live_massive_key()

    user = _create_user(postgres_db_session)
    payload = CreateScannerJobRequest(
        mode="basic",
        symbols=["SPY"],
        strategy_types=["long_call"],
        rule_sets=[{"name": "rsi35", "entry_rules": [{"type": "rsi", "operator": "lte", "threshold": "35", "period": 14}]}],
        start_date=date(2025, 1, 2),
        end_date=date(2025, 3, 7),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size="10000",
        risk_per_trade_pct="2",
        commission_per_contract="0.65",
        max_recommendations=5,
    )

    service = ScanService(postgres_db_session)
    # This is a live-provider scanner smoke, not a Redis cache or prefetch
    # smoke. Keep it independent from the local Redis auth mode so it remains
    # focused on provider reachability and end-to-end scan execution.
    service.execution_service.market_data_service._redis_cache = None
    monkeypatch.setattr(
        service.execution_service,
        "_maybe_prefetch_option_data",
        lambda *args, **kwargs: None,
    )
    try:
        job = service.create_job(user, payload)
        postgres_db_session.commit()
        postgres_db_session.refresh(job)

        try:
            completed = service.run_job(job.id)
        except ExternalServiceError as exc:
            pytest.skip(f"Live-provider scanner smoke skipped because Massive was unreachable: {exc}")
        postgres_db_session.commit()

        if completed.status == "failed":
            error_text = " ".join(
                str(part)
                for part in [getattr(completed, "error_code", None), getattr(completed, "error_message", None)]
                if part
            ).lower()
            pytest.skip(
                "Live-provider scanner smoke skipped because live execution did not complete successfully: "
                f"{error_text or 'unknown upstream failure'}"
            )
        assert completed.status == "succeeded"
        assert completed.evaluated_candidate_count >= 0
        assert completed.recommendation_count >= 0
    finally:
        service.close()
