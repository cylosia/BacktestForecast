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
