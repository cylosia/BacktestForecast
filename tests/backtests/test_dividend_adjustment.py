from __future__ import annotations

from datetime import date
from decimal import Decimal

from backtestforecast.backtests.engine import OptionsBacktestEngine
from backtestforecast.backtests.types import BacktestConfig, OpenMultiLegPosition, OpenStockLeg


def test_close_position_adds_prorated_dividends_for_stock_legs() -> None:
    engine = OptionsBacktestEngine()
    config = BacktestConfig(
        symbol="T",
        strategy_type="covered_call",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 2, 1),
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=30,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("0"),
        entry_rules=(),
        dividend_yield=0.12,
    )
    position = OpenMultiLegPosition(
        display_ticker="T+CALL",
        strategy_type="covered_call",
        underlying_symbol="T",
        entry_date=date(2024, 1, 2),
        entry_index=0,
        quantity=1,
        dte_at_open=30,
        option_legs=[],
        stock_legs=[
            OpenStockLeg(
                symbol="T",
                side=1,
                share_quantity_per_unit=100,
                entry_price=100.0,
                last_price=105.0,
            )
        ],
    )

    trade, cash_delta = engine._close_position(
        position=position,
        config=config,
        exit_value=Decimal("10500"),
        exit_date=date(2024, 2, 1),
        exit_underlying_close=105.0,
        exit_prices={"T": 105.0},
        exit_reason="time_exit",
    )

    assert round(float(trade.detail_json["dividends_received"]), 2) == 98.63
    assert round(float(trade.gross_pnl), 2) == 598.63
    assert round(float(trade.net_pnl), 2) == 598.63
    assert round(float(cash_delta), 2) == 10598.63
