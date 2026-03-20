"""Test that mid_price returns None for non-finite inputs instead of 0.0.

Returning 0.0 would silently corrupt downstream financial calculations.
"""
from __future__ import annotations

import math
from datetime import date

import pytest

from backtestforecast.market_data.types import OptionQuoteRecord, OptionSnapshotRecord


class TestOptionQuoteRecordMidPrice:
    def test_normal_bid_ask(self) -> None:
        q = OptionQuoteRecord(trade_date=date(2024, 1, 1), bid_price=1.0, ask_price=2.0, participant_timestamp=None)
        assert q.mid_price == 1.5

    def test_nan_bid(self) -> None:
        q = OptionQuoteRecord(trade_date=date(2024, 1, 1), bid_price=float("nan"), ask_price=2.0, participant_timestamp=None)
        assert q.mid_price is None

    def test_inf_ask(self) -> None:
        q = OptionQuoteRecord(trade_date=date(2024, 1, 1), bid_price=1.0, ask_price=float("inf"), participant_timestamp=None)
        assert q.mid_price is None

    def test_negative_inf(self) -> None:
        q = OptionQuoteRecord(trade_date=date(2024, 1, 1), bid_price=float("-inf"), ask_price=1.0, participant_timestamp=None)
        assert q.mid_price is None

    def test_both_nan(self) -> None:
        q = OptionQuoteRecord(trade_date=date(2024, 1, 1), bid_price=float("nan"), ask_price=float("nan"), participant_timestamp=None)
        assert q.mid_price is None

    def test_zero_bid_ask(self) -> None:
        q = OptionQuoteRecord(trade_date=date(2024, 1, 1), bid_price=0.0, ask_price=0.0, participant_timestamp=None)
        assert q.mid_price == 0.0


class TestOptionSnapshotRecordMidPrice:
    def test_normal(self) -> None:
        s = OptionSnapshotRecord(ticker="SPY240101C00500000", underlying_ticker="SPY", bid_price=2.0, ask_price=4.0)
        assert s.mid_price == 3.0

    def test_none_bid(self) -> None:
        s = OptionSnapshotRecord(ticker="SPY240101C00500000", underlying_ticker="SPY", bid_price=None, ask_price=4.0)
        assert s.mid_price is None

    def test_nan_returns_none(self) -> None:
        s = OptionSnapshotRecord(ticker="SPY240101C00500000", underlying_ticker="SPY", bid_price=float("nan"), ask_price=4.0)
        assert s.mid_price is None

    def test_both_none(self) -> None:
        s = OptionSnapshotRecord(ticker="SPY240101C00500000", underlying_ticker="SPY")
        assert s.mid_price is None
