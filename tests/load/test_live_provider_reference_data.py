"""Direct live-provider smoke tests for reference-data endpoints."""
from __future__ import annotations

import os
from datetime import date

import pytest

from backtestforecast.errors import ExternalServiceError
from backtestforecast.integrations.massive_client import MassiveClient

pytestmark = [pytest.mark.live_provider, pytest.mark.load]


def _require_live_massive_key() -> None:
    api_key = os.environ.get("MASSIVE_API_KEY")
    if not api_key or api_key == "test-massive-api-key":
        pytest.skip("Live-provider reference-data smoke requires a real MASSIVE_API_KEY.")


def test_massive_reference_data_smoke() -> None:
    _require_live_massive_key()

    client = MassiveClient()
    try:
        try:
            holidays = client.get_market_holidays()
            treasury_rate = client.get_average_treasury_yield(date(2025, 1, 2), date(2025, 1, 10))
            contracts = client.list_option_contracts(
                "SPY",
                as_of_date=date(2025, 1, 10),
                contract_type="call",
                expiration_gte=date(2025, 1, 17),
                expiration_lte=date(2025, 2, 21),
            )
        except ExternalServiceError as exc:
            pytest.skip(f"Live-provider reference-data smoke skipped because Massive was unreachable: {exc}")
    finally:
        client.close()

    assert isinstance(holidays, list)
    assert holidays, "Expected at least one upcoming market holiday from Massive."
    assert treasury_rate is not None
    assert 0.0 <= treasury_rate <= 1.0
    assert contracts, "Expected at least one SPY option contract in the requested live window."
