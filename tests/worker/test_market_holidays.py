"""Verify market holidays task structure."""
from __future__ import annotations

import inspect

from apps.worker.app.tasks import refresh_market_holidays


def test_refresh_holidays_uses_massive_client():
    source = inspect.getsource(refresh_market_holidays)
    assert "MassiveClient" in source
    assert "store_holidays_in_redis" in source
