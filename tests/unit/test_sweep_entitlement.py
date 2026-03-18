"""Verify sweep creation requires forecasting access."""
from __future__ import annotations

import pytest
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

from backtestforecast.errors import FeatureLockedError
from backtestforecast.schemas.sweeps import CreateSweepRequest


def test_free_tier_cannot_create_sweep():
    """Free-tier users must be rejected when creating sweeps."""
    from backtestforecast.services.sweeps import SweepService

    session = MagicMock()
    service = SweepService(session)
    user = MagicMock()
    user.plan_tier = "free"
    user.subscription_status = None
    user.subscription_current_period_end = None

    payload = MagicMock(spec=CreateSweepRequest)
    payload.idempotency_key = None

    with pytest.raises(FeatureLockedError):
        service.create_job(user, payload)
