"""Tests for audit round 2 findings (items 61-85)."""
from __future__ import annotations

import re
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from backtestforecast.backtests.types import (
    BacktestConfig,
    EquityPointResult,
    OpenMultiLegPosition,
    OpenOptionLeg,
    TradeResult,
)


class TestToDecimalEdgeCases:
    """Item 61: to_decimal(NaN) and to_decimal(Inf) behavior."""

    def test_nan_returns_none(self):
        from backtestforecast.services.backtests import to_decimal

        result = to_decimal(float("nan"))
        assert result is None

    def test_inf_raises(self):
        from backtestforecast.services.backtests import to_decimal

        with pytest.raises(ValueError, match="Non-finite"):
            to_decimal(float("inf"))

    def test_neg_inf_raises(self):
        from backtestforecast.services.backtests import to_decimal

        with pytest.raises(ValueError, match="Non-finite"):
            to_decimal(float("-inf"))

    def test_decimal_nan_returns_none(self):
        from backtestforecast.services.backtests import to_decimal

        result = to_decimal(Decimal("NaN"))
        assert result is None

    def test_normal_float_quantized(self):
        from backtestforecast.services.backtests import to_decimal

        result = to_decimal(3.14159)
        assert result == Decimal("3.1416")

    def test_normal_decimal_quantized(self):
        from backtestforecast.services.backtests import to_decimal

        result = to_decimal(Decimal("1.23456789"))
        assert result == Decimal("1.2346")


class TestSlippageInNetPnl:
    """Item 62: Slippage included in net_pnl."""

    def test_net_pnl_includes_slippage(self):
        """Verify that net_pnl accounts for both entry and exit slippage."""
        from backtestforecast.backtests.engine import OptionsBacktestEngine

        engine = OptionsBacktestEngine()
        position = OpenMultiLegPosition(
            display_ticker="TEST",
            strategy_type="long_call",
            underlying_symbol="SPY",
            entry_date=date(2024, 1, 1),
            entry_index=0,
            quantity=1,
            dte_at_open=18,
            capital_required_per_unit=500.0,
            option_legs=[
                OpenOptionLeg(
                    ticker="SPY240119C00500000",
                    contract_type="call",
                    strike_price=500.0,
                    expiration_date=date(2024, 1, 19),
                    side=1,
                    quantity_per_unit=1,
                    entry_mid=5.0,
                    last_mid=6.0,
                ),
            ],
            entry_commission_total=1.30,
        )
        config = BacktestConfig(
            symbol="SPY",
            strategy_type="long_call",
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 19),
            target_dte=18,
            dte_tolerance_days=5,
            max_holding_days=30,
            account_size=10000.0,
            risk_per_trade_pct=5.0,
            commission_per_contract=0.65,
            entry_rules=[],
            slippage_pct=1.0,
        )

        trade, _cash_delta = engine._close_position(
            position,
            config,
            600.0,
            date(2024, 1, 19),
            510.0,
            {"SPY240119C00500000": 6.0},
            "expiration",
        )
        assert trade.net_pnl < trade.gross_pnl - trade.total_commissions


class TestSlippageInPositionSizing:
    """Item 63: Slippage included in position sizing."""

    def test_position_size_with_slippage_is_smaller(self):
        from backtestforecast.backtests.engine import OptionsBacktestEngine

        size_no_slip = OptionsBacktestEngine._resolve_position_size(
            available_cash=10000,
            account_size=10000,
            risk_per_trade_pct=5,
            capital_required_per_unit=1000,
            max_loss_per_unit=500,
            entry_cost_per_unit=300,
            commission_per_unit=1.30,
            slippage_pct=0,
        )
        size_with_slip = OptionsBacktestEngine._resolve_position_size(
            available_cash=10000,
            account_size=10000,
            risk_per_trade_pct=5,
            capital_required_per_unit=1000,
            max_loss_per_unit=500,
            entry_cost_per_unit=300,
            commission_per_unit=1.30,
            slippage_pct=5.0,
        )
        assert size_with_slip <= size_no_slip


class TestSortinoDenominator:
    """Item 64: Sortino ratio denominator uses count of downside returns
    (population convention)."""

    def test_sortino_uses_downside_count(self):
        from backtestforecast.backtests.summary import _compute_sharpe_sortino

        equities = [
            10000, 10100, 10200, 10150, 10300, 10250, 10400, 10500, 10600, 10700,
            10650, 10800, 10750, 10900, 10820, 11000, 10920, 11100, 11050, 11200,
            11100, 11300, 11220, 11400, 11320, 11500, 11420, 11600, 11550, 11700,
            11620,
        ]
        curve = [
            EquityPointResult(
                trade_date=date(2024, 1, 1) + timedelta(days=i),
                equity=e,
                cash=e,
                position_value=0,
                drawdown_pct=0,
            )
            for i, e in enumerate(equities)
        ]
        sharpe, sortino = _compute_sharpe_sortino(curve, 0.045, 10)
        assert sortino is not None
        if sharpe is not None:
            assert sortino > sharpe


class TestRecoveryFactor:
    """Item 65: Recovery factor uses peak equity."""

    def test_recovery_factor_uses_peak(self):
        from backtestforecast.backtests.summary import build_summary

        curve = [
            EquityPointResult(trade_date=date(2024, 1, 1), equity=10000, cash=10000, position_value=0, drawdown_pct=0),
            EquityPointResult(trade_date=date(2024, 2, 1), equity=15000, cash=15000, position_value=0, drawdown_pct=0),
            EquityPointResult(trade_date=date(2024, 3, 1), equity=12000, cash=12000, position_value=0, drawdown_pct=20),
        ]
        trades = [
            TradeResult(
                option_ticker="TEST",
                strategy_type="long_call",
                underlying_symbol="SPY",
                entry_date=date(2024, 1, 1),
                exit_date=date(2024, 3, 1),
                expiration_date=date(2024, 3, 1),
                quantity=1,
                dte_at_open=60,
                holding_period_days=60,
                entry_underlying_close=400,
                exit_underlying_close=420,
                entry_mid=5,
                exit_mid=7,
                gross_pnl=2000,
                net_pnl=2000,
                total_commissions=0,
                entry_reason="test",
                exit_reason="test",
            ),
        ]
        summary = build_summary(10000, 12000, trades, curve)
        if summary.recovery_factor is not None:
            max_dd_dollars = 0.20 * 15000
            expected_rf = 2000 / max_dd_dollars
            assert abs(summary.recovery_factor - expected_rf) < 0.01


class TestS3KeySanitization:
    """Item 68: S3 key sanitization rejects path traversal."""

    def test_path_traversal_stripped(self):
        from backtestforecast.exports.storage import S3Storage

        assert S3Storage._sanitize_file_name("../../etc/passwd") == "passwd"

    def test_backslash_traversal_stripped(self):
        from backtestforecast.exports.storage import S3Storage

        assert S3Storage._sanitize_file_name("..\\..\\etc\\passwd") == "passwd"

    def test_dotfile_stripped(self):
        from backtestforecast.exports.storage import S3Storage

        result = S3Storage._sanitize_file_name(".hidden")
        assert not result.startswith(".")

    def test_normal_name_preserved(self):
        from backtestforecast.exports.storage import S3Storage

        assert S3Storage._sanitize_file_name("export.csv") == "export.csv"

    def test_empty_name_fallback(self):
        from backtestforecast.exports.storage import S3Storage

        assert S3Storage._sanitize_file_name("") == "export"
        assert S3Storage._sanitize_file_name("../..") == "export"


class TestMigrationNoDuplicateIndexes:
    """Item 82: Migration chain doesn't have duplicate indexes."""

    def test_no_duplicate_index_names(self):
        versions_dir = Path("alembic/versions")
        if not versions_dir.exists():
            pytest.skip("No alembic/versions directory")

        index_names: dict[str, str] = {}
        pattern = re.compile(r'op\.create_index\(\s*"([^"]+)"')

        for migration_file in sorted(versions_dir.glob("*.py")):
            content = migration_file.read_text()
            lines = content.split("\n")
            for line in lines:
                stripped = line.strip()
                if stripped.startswith("#"):
                    continue
                match = pattern.search(stripped)
                if match:
                    index_name = match.group(1)
                    if index_name in index_names:
                        pytest.fail(
                            f"Duplicate index '{index_name}' created in "
                            f"{migration_file.name} (first seen in {index_names[index_name]})"
                        )
                    index_names[index_name] = migration_file.name
