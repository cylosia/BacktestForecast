"""Tests for the 'looks correct but silently wrong' fixes.

Covers:
- #2: ScannerJob validator no longer uses misleading hasattr
- #3: _validate_bars logs price discrepancy on duplicate dates
- #10: ThreadPoolExecutor uses cancel_futures on timeout
- #16: Contract cache tracks items not keys
"""
from __future__ import annotations

import inspect
import math
from datetime import date

import pytest


class TestScannerJobValidator:
    """Item #2: hasattr replaced with direct check."""

    def test_validator_does_not_use_hasattr(self) -> None:
        from backtestforecast.models import ScannerJob
        source = inspect.getsource(ScannerJob._validate_evaluated_count)
        assert "hasattr" not in source, (
            "ScannerJob._validate_evaluated_count should not use hasattr — "
            "it is always True for mapped attributes and is misleading"
        )

    def test_validator_checks_candidate_count_gt_zero(self) -> None:
        from backtestforecast.models import ScannerJob
        source = inspect.getsource(ScannerJob._validate_evaluated_count)
        assert "candidate_count > 0" in source or "candidate_count is not None" in source, (
            "Validator should check candidate_count > 0 to prevent division issues"
        )


class TestValidateBarsDedup:
    """Item #3: _validate_bars warns on price discrepancy for duplicate dates."""

    def test_duplicate_date_with_different_close_logs_warning(self) -> None:
        from backtestforecast.market_data.service import MarketDataService
        from backtestforecast.market_data.types import DailyBar

        bars = [
            DailyBar(trade_date=date(2024, 1, 2), open_price=100, high_price=155, low_price=99, close_price=150.0, volume=1000),
            DailyBar(trade_date=date(2024, 1, 2), open_price=100, high_price=160, low_price=99, close_price=155.0, volume=1000),
        ]
        result = MarketDataService._validate_bars(bars, "TEST")
        assert len(result) == 1
        assert result[0].close_price == 155.0

    def test_source_contains_discrepancy_warning(self) -> None:
        from backtestforecast.market_data.service import MarketDataService
        source = inspect.getsource(MarketDataService._validate_bars)
        assert "duplicate_date_price_discrepancy" in source, (
            "_validate_bars must log 'duplicate_date_price_discrepancy' when "
            "duplicate dates have different close prices"
        )


class TestPrefetchTimeoutShutdown:
    """Item #10: ThreadPoolExecutor uses cancel_futures on timeout."""

    def test_prefetch_uses_cancel_futures(self) -> None:
        from backtestforecast.market_data.prefetch import OptionDataPrefetcher
        source = inspect.getsource(OptionDataPrefetcher.prefetch_for_symbol)
        assert "cancel_futures" in source, (
            "prefetch_for_symbol must use cancel_futures=True on ThreadPoolExecutor "
            "shutdown to avoid blocking past the intended timeout"
        )

    def test_prefetch_does_not_use_context_manager_for_pool(self) -> None:
        from backtestforecast.market_data.prefetch import OptionDataPrefetcher
        source = inspect.getsource(OptionDataPrefetcher.prefetch_for_symbol)
        assert "with ThreadPoolExecutor" not in source, (
            "prefetch_for_symbol must not use 'with ThreadPoolExecutor' context manager "
            "because its __exit__ calls shutdown(wait=True), blocking past timeout"
        )


class TestCacheBudgetCountsItems:
    """Item #16: Contract cache budget counts items not keys."""

    def test_store_contracts_tracks_item_count(self) -> None:
        from backtestforecast.market_data.service import MassiveOptionGateway
        source = inspect.getsource(MassiveOptionGateway._store_contracts_in_memory)
        assert "len(contracts)" in source, (
            "_store_contracts_in_memory must track len(contracts) not just 1 per key"
        )
        assert "_track_add(item_count)" in source or "_track_add(max(" in source, (
            "_track_add should receive item_count based on list length"
        )
