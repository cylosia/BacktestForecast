"""Tests for the to_decimal utility function."""
from __future__ import annotations

from decimal import Decimal

from backtestforecast.services.backtests import to_decimal


def test_to_decimal_nan_returns_none():
    assert to_decimal(float("nan")) is None


def test_to_decimal_decimal_nan_returns_none():
    assert to_decimal(Decimal("NaN")) is None


def test_to_decimal_normal_value():
    result = to_decimal(1.5)
    assert result == Decimal("1.5000")


def test_to_decimal_negative_value():
    result = to_decimal(-3.14159)
    assert result == Decimal("-3.1416")


def test_to_decimal_inf_raises_by_default():
    import pytest

    with pytest.raises(ValueError, match="Non-finite"):
        to_decimal(float("inf"))


def test_to_decimal_inf_returns_none_when_allowed():
    assert to_decimal(float("inf"), allow_infinite=True) is None
