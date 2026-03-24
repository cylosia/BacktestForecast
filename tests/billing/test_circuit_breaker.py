"""Verify Stripe circuit breaker behavior."""
from __future__ import annotations


def test_circuit_breaker_key_exists():
    from backtestforecast.services.billing import _STRIPE_CIRCUIT_KEY
    assert _STRIPE_CIRCUIT_KEY == "bff:stripe_circuit_open"
