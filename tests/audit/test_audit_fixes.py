"""Tests verifying key audit fixes remain in place."""
from __future__ import annotations

from decimal import Decimal

import pytest


class TestToDecimalInfiniteHandling:
    """Verify to_decimal handles infinite values gracefully."""

    def test_infinite_with_allow_infinite_returns_decimal_infinity(self):
        from backtestforecast.services.backtests import to_decimal
        result = to_decimal(float("inf"), allow_infinite=True)
        assert result == Decimal("Infinity")

    def test_negative_infinite_with_allow_infinite_returns_decimal_infinity(self):
        from backtestforecast.services.backtests import to_decimal
        result = to_decimal(float("-inf"), allow_infinite=True)
        assert result == Decimal("-Infinity")

    def test_infinite_without_allow_infinite_raises(self):
        from backtestforecast.services.backtests import to_decimal
        with pytest.raises(ValueError, match="Non-finite"):
            to_decimal(float("inf"))

    def test_nan_returns_none(self):
        from backtestforecast.services.backtests import to_decimal
        result = to_decimal(float("nan"), allow_infinite=True)
        assert result is None

    def test_decimal_infinite_with_allow_returns_decimal_infinity(self):
        from backtestforecast.services.backtests import to_decimal
        result = to_decimal(Decimal("Infinity"), allow_infinite=True)
        assert result == Decimal("Infinity")


class TestRedisUrlSanitization:
    """Verify Redis URLs are redacted from error messages."""

    def test_redis_url_with_password_is_redacted(self):
        from backtestforecast.schemas.common import sanitize_error_message
        msg = "Error connecting to redis://:s3cret@redis-host:6379/0"
        result = sanitize_error_message(msg)
        assert "s3cret" not in (result or "")

    def test_rediss_url_is_redacted(self):
        from backtestforecast.schemas.common import sanitize_error_message
        msg = "Error connecting to rediss://:password@host:6380/0"
        result = sanitize_error_message(msg)
        assert "password" not in (result or "")


class TestScannerDteToleranceValidation:
    """Verify scanner rejects dte_tolerance >= target_dte."""

    def test_dte_tolerance_exceeds_target_dte_rejected(self):
        from datetime import date

        from pydantic import ValidationError

        from backtestforecast.schemas.scans import CreateScannerJobRequest
        with pytest.raises(ValidationError, match="dte_tolerance_days must be less than target_dte"):
            CreateScannerJobRequest(
                mode="basic",
                symbols=["AAPL"],
                strategy_types=["long_call"],
                rule_sets=[{"name": "Test", "entry_rules": [{"type": "rsi", "operator": "lte", "threshold": 35, "period": 14}]}],
                start_date=date(2024, 1, 1),
                end_date=date(2024, 6, 1),
                target_dte=7,
                dte_tolerance_days=60,
                max_holding_days=20,
                account_size=10000,
                risk_per_trade_pct=5,
                commission_per_contract=1,
            )
