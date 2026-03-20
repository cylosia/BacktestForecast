"""Test that regime IV labels document their realized-vol proxy nature.

The Regime enum's HIGH_IV / LOW_IV labels are based on realized volatility
(20-day HV rank), not actual implied volatility from options data. The enum
must document this clearly to prevent misinterpretation by consumers.
"""
from __future__ import annotations

import inspect

from backtestforecast.pipeline.regime import Regime, classify_regime


def test_regime_enum_documents_iv_proxy() -> None:
    """Regime enum source must mention that HIGH_IV/LOW_IV are realized-vol proxies."""
    source = inspect.getsource(Regime)
    assert "realized" in source.lower() or "proxy" in source.lower(), (
        "Regime enum must document that HIGH_IV/LOW_IV are based on "
        "realized volatility, not actual implied volatility"
    )


def test_classify_regime_documents_proxy_nature() -> None:
    """classify_regime source must mention the vol proxy nature."""
    source = inspect.getsource(classify_regime)
    assert "proxy" in source.lower(), (
        "classify_regime must document the realized-vol proxy for IV ranking"
    )


def test_regime_high_iv_and_low_iv_exist() -> None:
    """HIGH_IV and LOW_IV must remain as enum values for backward compatibility."""
    assert hasattr(Regime, "HIGH_IV")
    assert hasattr(Regime, "LOW_IV")
    assert Regime.HIGH_IV.value == "high_iv"
    assert Regime.LOW_IV.value == "low_iv"
