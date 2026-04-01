from __future__ import annotations

from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from backtestforecast.market_data.service import HistoricalDataBundle
from backtestforecast.market_data.types import DailyBar
from backtestforecast.schemas.scans import CreateScannerJobRequest
from backtestforecast.services.scans import ScanService


def _payload(*, symbols: list[str] | None = None) -> CreateScannerJobRequest:
    return CreateScannerJobRequest(
        name="Shared bundle prefetch",
        mode="basic",
        symbols=symbols or ["F", "GM"],
        strategy_types=["long_put", "bear_put_debit_spread"],
        rule_sets=[
            {
                "name": "rsi40",
                "entry_rules": [
                    {"type": "rsi", "operator": "lte", "threshold": Decimal("40"), "period": 14},
                ],
            },
            {
                "name": "rsi45",
                "entry_rules": [
                    {"type": "rsi", "operator": "lte", "threshold": Decimal("45"), "period": 14},
                ],
            },
        ],
        start_date=date(2015, 1, 2),
        end_date=date(2015, 2, 27),
        target_dte=14,
        dte_tolerance_days=5,
        max_holding_days=7,
        account_size=Decimal("10000"),
        risk_per_trade_pct=Decimal("5"),
        commission_per_contract=Decimal("0.65"),
        max_recommendations=5,
    )


def _bundle() -> HistoricalDataBundle:
    return HistoricalDataBundle(
        bars=[DailyBar(date(2015, 1, 2), 15.1, 15.3, 14.9, 15.0, 10_000_000)],
        earnings_dates=set(),
        ex_dividend_dates=set(),
        option_gateway=SimpleNamespace(),
        data_source="historical_flatfile",
        warnings=[],
    )


def test_prepare_bundles_prewarms_each_symbol_once_per_strategy() -> None:
    session = MagicMock()
    execution_service = MagicMock()
    execution_service.market_data_service.prepare_backtest.side_effect = lambda req: _bundle()
    execution_service.prefetch_requests_with_shared_bundle.return_value = {
        "prefetch_count": 2,
        "skipped_count": 0,
        "dates_processed": 2,
        "contracts_fetched": 4,
        "quotes_fetched": 0,
        "errors": [],
        "requests": [],
    }

    service = ScanService(session, execution_service=execution_service)
    warnings: list[dict[str, object]] = []

    bundles = service._prepare_bundles(_payload(), warnings)

    assert set(bundles) == {"F", "GM"}
    assert execution_service.prefetch_requests_with_shared_bundle.call_count == 2

    warmed_symbols: set[str] = set()
    for call in execution_service.prefetch_requests_with_shared_bundle.call_args_list:
        requests = call.args[0]
        warmed_symbols.add(requests[0].symbol)
        assert len(requests) == 2
        assert len({request.symbol for request in requests}) == 1
        assert {request.strategy_type.value for request in requests} == {
            "long_put",
            "bear_put_debit_spread",
        }

    assert warmed_symbols == {"F", "GM"}
    assert warnings == []


def test_prepare_bundles_warns_when_shared_prefetch_fails() -> None:
    session = MagicMock()
    execution_service = MagicMock()
    execution_service.market_data_service.prepare_backtest.side_effect = lambda req: _bundle()
    execution_service.prefetch_requests_with_shared_bundle.side_effect = RuntimeError("boom")

    service = ScanService(session, execution_service=execution_service)
    warnings: list[dict[str, object]] = []

    bundles = service._prepare_bundles(_payload(symbols=["F"]), warnings)

    assert set(bundles) == {"F"}
    assert execution_service.prefetch_requests_with_shared_bundle.call_count == 1
    assert warnings == [
        {
            "code": "symbol_prefetch_failed",
            "message": (
                "F option prefetch could not be completed; "
                "candidate execution continued without a warmed bundle."
            ),
        }
    ]


def test_prepare_bundles_emits_aggregate_timing_log() -> None:
    session = MagicMock()
    execution_service = MagicMock()
    execution_service.market_data_service.prepare_backtest.side_effect = lambda req: _bundle()
    execution_service.prefetch_requests_with_shared_bundle.return_value = {
        "prefetch_count": 2,
        "skipped_count": 0,
        "dates_processed": 2,
        "contracts_fetched": 4,
        "quotes_fetched": 1,
        "errors": [],
        "requests": [],
    }

    service = ScanService(session, execution_service=execution_service)
    warnings: list[dict[str, object]] = []

    with patch("backtestforecast.services.scans.logger") as mock_logger:
        bundles = service._prepare_bundles(_payload(), warnings)

    assert set(bundles) == {"F", "GM"}
    aggregate_calls = [
        call for call in mock_logger.info.call_args_list
        if call.args and call.args[0] == "scan.bundle_stage_timing"
    ]
    assert len(aggregate_calls) == 1
    payload = aggregate_calls[0].kwargs
    assert payload["symbols_requested"] == 2
    assert payload["bundles_ready"] == 2
    assert payload["market_data_service_ms"] >= 0
    assert payload["prefetch_symbols"] == 2
    assert payload["prefetch_request_count"] == 4
    assert payload["prefetch_count"] == 4
    assert payload["skipped_count"] == 0
    assert payload["dates_processed"] == 4
    assert payload["contracts_fetched"] == 8
    assert payload["quotes_fetched"] == 2
    assert payload["prefetch_failures"] == 0
    assert payload["fetch_ms"] >= 0
    assert payload["prefetch_ms"] >= 0
    assert payload["total_ms"] >= 0
