from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from pydantic import ValidationError as PydanticValidationError

from backtestforecast.schemas.scans import CreateScannerJobRequest


def test_scanner_request_normalizes_symbols_and_rejects_duplicate_rule_names() -> None:
    with pytest.raises(PydanticValidationError):
        CreateScannerJobRequest(
            mode="basic",
            symbols=["aapl", " AAPL ", "msft"],
            strategy_types=["long_call"],
            rule_sets=[
                {
                    "name": "Mean Reversion",
                    "entry_rules": [{"type": "rsi", "operator": "lte", "threshold": Decimal("35"), "period": 14}],
                },
                {
                    "name": "mean reversion",
                    "entry_rules": [{"type": "rsi", "operator": "lte", "threshold": Decimal("30"), "period": 14}],
                },
            ],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 6, 1),
            target_dte=30,
            dte_tolerance_days=5,
            max_holding_days=20,
            account_size=Decimal("10000"),
            risk_per_trade_pct=Decimal("5"),
            commission_per_contract=Decimal("1"),
        )


def test_scanner_request_accepts_unique_symbols_after_normalization() -> None:
    request = CreateScannerJobRequest(
        mode="basic",
        symbols=["aapl", " AAPL ", "msft"],
        strategy_types=["long_call"],
        rule_sets=[
            {
                "name": "Mean Reversion",
                "entry_rules": [{"type": "rsi", "operator": "lte", "threshold": Decimal("35"), "period": 14}],
            }
        ],
        start_date=date(2024, 1, 1),
        end_date=date(2024, 6, 1),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=20,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("1"),
    )

    assert request.symbols == ["AAPL", "MSFT"]


# ---------------------------------------------------------------------------
# Item 78: Scan date validation - timezone edge cases
# ---------------------------------------------------------------------------


class TestScanDateValidationTimezoneEdge:
    """Verify the scan schema rejects future dates and dates exactly equal
    to today (end_date >= start_date < end_date constraint)."""

    def _base_kwargs(self, **overrides):
        defaults = dict(
            mode="basic",
            symbols=["AAPL"],
            strategy_types=["long_call"],
            rule_sets=[
                {
                    "name": "Default",
                    "entry_rules": [
                        {"type": "rsi", "operator": "lte", "threshold": Decimal("35"), "period": 14},
                    ],
                }
            ],
            target_dte=30,
            dte_tolerance_days=5,
            max_holding_days=20,
            account_size=Decimal("10000"),
            risk_per_trade_pct=Decimal("5"),
            commission_per_contract=Decimal("1"),
        )
        defaults.update(overrides)
        return defaults

    def test_future_end_date_rejected(self):
        from datetime import timedelta

        future = date.today() + timedelta(days=1)
        past = date(2024, 1, 1)
        with pytest.raises(PydanticValidationError, match="end_date cannot be in the future"):
            CreateScannerJobRequest(**self._base_kwargs(start_date=past, end_date=future))

    def test_future_start_date_rejected(self):
        from datetime import timedelta

        from backtestforecast.utils.dates import market_date_today

        today_market = market_date_today()
        future_start = today_market + timedelta(days=2)
        future_end = today_market + timedelta(days=5)
        with pytest.raises(PydanticValidationError, match="end_date cannot be in the future"):
            CreateScannerJobRequest(**self._base_kwargs(start_date=future_start, end_date=future_end))

    def test_start_equals_end_rejected(self):
        same_day = date(2024, 6, 1)
        with pytest.raises(PydanticValidationError, match="start_date must be earlier than end_date"):
            CreateScannerJobRequest(**self._base_kwargs(start_date=same_day, end_date=same_day))

    def test_start_after_end_rejected(self):
        with pytest.raises(PydanticValidationError):
            CreateScannerJobRequest(
                **self._base_kwargs(start_date=date(2024, 7, 1), end_date=date(2024, 6, 1))
            )

    def test_valid_past_dates_accepted(self):
        req = CreateScannerJobRequest(
            **self._base_kwargs(start_date=date(2024, 1, 1), end_date=date(2024, 3, 1))
        )
        assert req.start_date == date(2024, 1, 1)
        assert req.end_date == date(2024, 3, 1)
