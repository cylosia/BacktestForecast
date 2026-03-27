"""Live-provider smoke test for sweep execution."""
from __future__ import annotations

import os
from datetime import date
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from backtestforecast.errors import ExternalServiceError
from backtestforecast.models import User
from backtestforecast.schemas.sweeps import CreateSweepRequest
from backtestforecast.services.sweeps import SweepService

pytestmark = [pytest.mark.postgres, pytest.mark.live_provider, pytest.mark.load]


def _require_live_massive_key() -> None:
    api_key = os.environ.get("MASSIVE_API_KEY")
    if not api_key or api_key == "test-massive-api-key":
        pytest.skip("Live-provider sweep smoke requires a real MASSIVE_API_KEY.")


def _create_user(session: Session) -> User:
    user = User(
        clerk_user_id=f"sweep_live_{uuid4().hex[:8]}",
        email=f"sweep-live-{uuid4().hex[:8]}@example.com",
        plan_tier="premium",
        subscription_status="active",
    )
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def test_sweep_live_provider_smoke(postgres_db_session: Session):
    _require_live_massive_key()

    user = _create_user(postgres_db_session)
    payload = CreateSweepRequest(
        mode="grid",
        symbol="SPY",
        strategy_types=["long_call"],
        start_date=date(2025, 1, 2),
        end_date=date(2025, 3, 7),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size="10000",
        risk_per_trade_pct="2",
        commission_per_contract="0.65",
        entry_rule_sets=[{"name": "rsi35", "entry_rules": [{"type": "rsi", "operator": "lte", "threshold": "35", "period": 14}]}],
        delta_grid=[{"value": 25}],
        max_results=5,
    )

    service = SweepService(postgres_db_session)
    try:
        job = service.create_job(user, payload)
        postgres_db_session.commit()
        postgres_db_session.refresh(job)

        try:
            completed = service.run_job(job.id)
        except ExternalServiceError as exc:
            pytest.skip(f"Live-provider sweep smoke skipped because Massive was unreachable: {exc}")
        postgres_db_session.commit()

        if completed.status == "failed":
            error_text = " ".join(
                str(part)
                for part in [getattr(completed, "error_code", None), getattr(completed, "error_message", None)]
                if part
            ).lower()
            if any(token in error_text for token in ("network", "provider", "unavailable", "massive", "upstream")):
                pytest.skip(f"Live-provider sweep smoke skipped because upstream data was unavailable: {error_text}")
        assert completed.status == "succeeded"
        assert completed.evaluated_candidate_count >= 0
        assert completed.result_count >= 0
    finally:
        service.close()
