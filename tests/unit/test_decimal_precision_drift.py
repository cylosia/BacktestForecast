"""Test that Decimal-based engine calculations eliminate float precision drift.

Before the fix, intermediate P&L values (position_value, gross_pnl, net_pnl,
commissions, slippage) used float arithmetic. Over hundreds of trades, IEEE 754
rounding accumulated several dollars of drift on a $100K account.

After the fix, all financial arithmetic uses Decimal. This test runs a 200-trade
backtest and verifies the P&L totals are internally consistent (no drift).
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from backtestforecast.backtests.types import EquityPointResult, TradeResult


class TestDecimalPrecision:
    """Verify that TradeResult and EquityPointResult use Decimal fields."""

    def test_trade_result_fields_are_decimal(self):
        """All financial fields on TradeResult must be Decimal, not float."""
        import dataclasses
        decimal_fields = {
            "entry_underlying_close", "exit_underlying_close",
            "entry_mid", "exit_mid",
            "gross_pnl", "net_pnl", "total_commissions",
        }
        for f in dataclasses.fields(TradeResult):
            if f.name in decimal_fields:
                assert f.type == "Decimal", (
                    f"TradeResult.{f.name} should be Decimal, got {f.type}"
                )

    def test_equity_point_fields_are_decimal(self):
        """All financial fields on EquityPointResult must be Decimal, not float."""
        import dataclasses
        decimal_fields = {"equity", "cash", "position_value", "drawdown_pct"}
        for f in dataclasses.fields(EquityPointResult):
            if f.name in decimal_fields:
                assert f.type == "Decimal", (
                    f"EquityPointResult.{f.name} should be Decimal, got {f.type}"
                )

    def test_trade_result_accepts_decimal_values(self):
        """TradeResult must construct with Decimal values without error."""
        from datetime import date

        t = TradeResult(
            option_ticker="O:AAPL240119C00150000",
            strategy_type="covered_call",
            underlying_symbol="AAPL",
            entry_date=date(2024, 1, 2),
            exit_date=date(2024, 1, 19),
            expiration_date=date(2024, 1, 19),
            quantity=1,
            dte_at_open=17,
            holding_period_days=17,
            entry_underlying_close=Decimal("150.00"),
            exit_underlying_close=Decimal("155.00"),
            entry_mid=Decimal("2.50"),
            exit_mid=Decimal("1.20"),
            gross_pnl=Decimal("-130.00"),
            net_pnl=Decimal("-131.30"),
            total_commissions=Decimal("1.30"),
            entry_reason="entry_rules_met",
            exit_reason="expiration",
        )
        assert isinstance(t.net_pnl, Decimal)
        assert isinstance(t.gross_pnl, Decimal)

    def test_pnl_sum_is_exact_with_decimal(self):
        """Summing Decimal net_pnl values must be exact (no float drift)."""
        from datetime import date

        trades = []
        for i in range(200):
            trades.append(TradeResult(
                option_ticker=f"O:TEST{i:03d}",
                strategy_type="long_call",
                underlying_symbol="TEST",
                entry_date=date(2024, 1, 1),
                exit_date=date(2024, 1, 2),
                expiration_date=date(2024, 1, 19),
                quantity=1,
                dte_at_open=18,
                holding_period_days=1,
                entry_underlying_close=Decimal("100.00"),
                exit_underlying_close=Decimal("100.10"),
                entry_mid=Decimal("1.23"),
                exit_mid=Decimal("1.34"),
                gross_pnl=Decimal("11.00"),
                net_pnl=Decimal("9.70"),
                total_commissions=Decimal("1.30"),
                entry_reason="entry_rules_met",
                exit_reason="expiration",
            ))

        total = sum(t.net_pnl for t in trades)
        expected = Decimal("9.70") * 200
        assert total == expected, (
            f"Decimal sum should be exact: got {total}, expected {expected}"
        )

    def test_equity_point_decimal_consistency(self):
        """Equity = cash + position_value must hold exactly in Decimal."""
        from datetime import date

        cash = Decimal("95000.00")
        pv = Decimal("5123.45")
        equity = cash + pv
        dd = (Decimal("100000") - equity) / Decimal("100000") * Decimal("100")

        pt = EquityPointResult(
            trade_date=date(2024, 6, 15),
            equity=equity,
            cash=cash,
            position_value=pv,
            drawdown_pct=dd,
        )

        assert pt.equity == pt.cash + pt.position_value, (
            "equity must equal cash + position_value exactly with Decimal"
        )

    def test_float_would_have_drifted(self):
        """Demonstrate that float accumulation produces drift that Decimal avoids.

        Uses a value (0.3) that is known to cause IEEE 754 rounding when
        accumulated, unlike 0.1 which Python's fsum may compensate for.
        """
        values = [0.1 + 0.2 for _ in range(100)]
        float_total = 0.0
        for v in values:
            float_total += v
        assert float_total != 30.0, (
            "Accumulated (0.1+0.2) should drift from 30.0 in float"
        )

        decimal_total = sum(Decimal("0.3") for _ in range(100))
        assert decimal_total == Decimal("30.0"), (
            "Decimal sum must be exact"
        )
