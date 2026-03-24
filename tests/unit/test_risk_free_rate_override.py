"""Test that CreateBacktestRunRequest accepts an optional risk_free_rate override.

Users need to specify a custom risk-free rate for historical backtests
where the default 4.5% is inappropriate (e.g., ZIRP era 2009-2021).
"""
from __future__ import annotations

from decimal import Decimal

from backtestforecast.schemas.backtests import CreateBacktestRunRequest


def test_risk_free_rate_field_exists() -> None:
    """CreateBacktestRunRequest must have an optional risk_free_rate field."""
    fields = CreateBacktestRunRequest.model_fields
    assert "risk_free_rate" in fields
    field_info = fields["risk_free_rate"]
    assert field_info.default is None, "risk_free_rate should default to None (use server default)"


def test_risk_free_rate_validation_range() -> None:
    """risk_free_rate must be between 0.0 and 0.20 when provided."""
    fields = CreateBacktestRunRequest.model_fields
    field_info = fields["risk_free_rate"]
    constraints = field_info.metadata
    ge_found = False
    le_found = False
    for m in constraints:
        if hasattr(m, "ge") and m.ge is not None:
            ge_found = True
            assert m.ge == Decimal("0")
        if hasattr(m, "le") and m.le is not None:
            le_found = True
            assert m.le == Decimal("0.20")
    assert ge_found, "risk_free_rate missing ge=0 constraint"
    assert le_found, "risk_free_rate missing le=0.20 constraint"
