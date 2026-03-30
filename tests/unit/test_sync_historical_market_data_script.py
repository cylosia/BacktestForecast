from __future__ import annotations

from contextlib import ExitStack
import importlib.util
import json
import os
import sys
import time
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from backtestforecast.db.base import Base
from backtestforecast.errors import ExternalServiceError
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore


def _load_sync_script_module():
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "sync_historical_market_data.py"
    sys.path.insert(0, str(script_path.parent))
    try:
        spec = importlib.util.spec_from_file_location("sync_historical_market_data", script_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.pop(0)


def _store() -> HistoricalMarketDataStore:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
    return HistoricalMarketDataStore(factory, factory)


def _status_file_path() -> Path:
    return Path(f"import-status-{uuid4().hex}.json")


def _seed_status_file(status_path: Path, **overrides: object) -> None:
    payload: dict[str, object] = {
        "started_at": "2026-03-29T14:02:55-05:00",
        "python_pid": 12345,
        "stdout_log_path": "stdout.log",
        "stderr_log_path": "stderr.log",
        "status_path": str(status_path),
    }
    payload.update(overrides)
    status_path.write_text(json.dumps(payload), encoding="utf-8")


def _run_main(
    module,
    *,
    status_path: Path | None = None,
    start_date: str = "2025-04-01",
    end_date: str = "2025-04-01",
    trade_dates: list[date] | None = None,
    results=None,
    sync_side_effect=None,
    env: dict[str, str] | None = None,
    include_status_flag: bool = False,
    resume: bool = False,
) -> int:
    fake_flatfiles = _ContextFlatFilesClient()
    argv = [
        "sync_historical_market_data.py",
        "--start-date", start_date,
        "--end-date", end_date,
        "--dry-run",
    ]
    if include_status_flag and status_path is not None:
        argv.extend(["--status-path", str(status_path)])
    if resume:
        argv.append("--resume")

    with ExitStack() as stack:
        stack.enter_context(patch.object(sys, "argv", argv))
        if env is not None:
            stack.enter_context(patch.dict(os.environ, env, clear=False))
        stack.enter_context(patch.object(module, "get_settings", return_value=SimpleNamespace(
            historical_data_start_year=2014,
            historical_data_sync_symbols=[],
        )))
        stack.enter_context(patch.object(module.MassiveFlatFilesClient, "from_settings", return_value=fake_flatfiles))
        if trade_dates is not None:
            stack.enter_context(patch.object(module, "_iter_trading_dates", return_value=trade_dates))
        if sync_side_effect is not None:
            stack.enter_context(patch.object(module, "_iter_sync_trade_dates", side_effect=sync_side_effect))
        elif results is not None:
            stack.enter_context(patch.object(module, "_iter_sync_trade_dates", return_value=results))
        return module.main()


class _FakeFlatFilesClient:
    def __init__(self) -> None:
        self._rows = {
            ("us_stocks_sip/day_aggs_v1", date(2025, 4, 1)): [
                {"ticker": "AAPL", "open": "100", "high": "101", "low": "99", "close": "100", "volume": "1000"},
                {"ticker": "MSFT", "open": "200", "high": "201", "low": "199", "close": "200", "volume": "2000"},
            ],
            ("us_stocks_sip/day_aggs_v1", date(2025, 4, 2)): [
                {"ticker": "AAPL", "open": "101", "high": "102", "low": "100", "close": "101", "volume": "1100"},
                {"ticker": "MSFT", "open": "201", "high": "202", "low": "200", "close": "201", "volume": "2100"},
            ],
            ("us_options_opra/day_aggs_v1", date(2025, 4, 1)): [
                {
                    "ticker": "O:AAPL250418C00190000",
                    "open": "5.10",
                    "high": "5.40",
                    "low": "4.80",
                    "close": "5.25",
                    "volume": "10",
                }
            ],
            ("us_options_opra/day_aggs_v1", date(2025, 4, 2)): [
                {
                    "ticker": "O:AAPL250418C00195000",
                    "open": "4.10",
                    "high": "4.40",
                    "low": "3.80",
                    "close": "4.25",
                    "volume": "12",
                }
            ],
        }

    def iter_csv_rows(self, dataset: str, trade_date: date):
        yield from self._rows[(dataset, trade_date)]


class _MissingStockFlatFilesClient(_FakeFlatFilesClient):
    def iter_csv_rows(self, dataset: str, trade_date: date):
        if dataset == "us_stocks_sip/day_aggs_v1":
            raise ExternalServiceError(f"Massive flat file not found for {trade_date.isoformat()}: {dataset}")
        yield from super().iter_csv_rows(dataset, trade_date)


class _DelayedFlatFilesClient(_FakeFlatFilesClient):
    def iter_csv_rows(self, dataset: str, trade_date: date):
        if trade_date == date(2025, 4, 1):
            time.sleep(0.05)
        yield from super().iter_csv_rows(dataset, trade_date)


class _ContextFlatFilesClient(_FakeFlatFilesClient):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeRestClient:
    def __init__(self) -> None:
        self.treasury_calls: list[date] = []
        self.dividend_calls: list[tuple[str, date]] = []

    def get_average_treasury_yield(self, start_date: date, end_date: date):
        assert start_date == end_date
        self.treasury_calls.append(start_date)
        return Decimal("0.041")

    def list_ex_dividend_dates(self, symbol: str, start_date: date, end_date: date):
        assert start_date == end_date
        self.dividend_calls.append((symbol, start_date))
        return {start_date} if symbol == "AAPL" else set()

    def close(self) -> None:
        return None


def test_sync_stock_day_streams_and_upserts_payload_batches() -> None:
    module = _load_sync_script_module()
    store = _store()
    flatfiles = _FakeFlatFilesClient()

    inserted = module._sync_stock_day(
        store,
        flatfiles,
        date(2025, 4, 1),
        symbols={"AAPL", "MSFT"},
        batch_size=1,
    )

    assert inserted == 2
    aapl = store.get_underlying_day_bars("AAPL", date(2025, 4, 1), date(2025, 4, 1))
    msft = store.get_underlying_day_bars("MSFT", date(2025, 4, 1), date(2025, 4, 1))
    assert len(aapl) == 1
    assert len(msft) == 1
    assert aapl[0].close_price == 100.0
    assert msft[0].close_price == 200.0


def test_sync_option_day_streams_and_upserts_payload_batches() -> None:
    module = _load_sync_script_module()
    store = _store()
    flatfiles = _FakeFlatFilesClient()

    inserted = module._sync_option_day(
        store,
        flatfiles,
        date(2025, 4, 1),
        symbols={"AAPL"},
        batch_size=2,
    )

    assert inserted == 1
    gateway = HistoricalOptionGateway(store, "AAPL")
    quote = gateway.get_quote("O:AAPL250418C00190000", date(2025, 4, 1))
    assert quote is not None
    assert quote.mid_price == 5.25


def test_sync_option_day_uses_record_fast_path_when_store_supports_it() -> None:
    module = _load_sync_script_module()
    flatfiles = _FakeFlatFilesClient()

    class _Store:
        def __init__(self) -> None:
            self.records: list[list[tuple[object, ...]]] = []

        def upsert_option_day_bar_records(self, batch):
            self.records.append(list(batch))
            return len(batch)

    store = _Store()

    inserted = module._sync_option_day(
        store,
        flatfiles,
        date(2025, 4, 1),
        symbols={"AAPL"},
        batch_size=2,
    )

    assert inserted == 1
    assert len(store.records) == 1
    assert store.records[0][0][1] == "O:AAPL250418C00190000"


def test_sync_stock_day_dry_run_counts_rows_without_store() -> None:
    module = _load_sync_script_module()
    flatfiles = _FakeFlatFilesClient()

    inserted = module._sync_stock_day(
        None,
        flatfiles,
        date(2025, 4, 1),
        symbols={"AAPL", "MSFT"},
        batch_size=1,
        dry_run=True,
    )

    assert inserted == 2


def test_sync_option_day_dry_run_counts_rows_without_store() -> None:
    module = _load_sync_script_module()
    flatfiles = _FakeFlatFilesClient()

    inserted = module._sync_option_day(
        None,
        flatfiles,
        date(2025, 4, 1),
        symbols={"AAPL"},
        batch_size=2,
        dry_run=True,
    )

    assert inserted == 1


def test_sync_trade_date_skips_missing_stock_file_and_keeps_option_rows() -> None:
    module = _load_sync_script_module()
    store = _store()
    flatfiles = _MissingStockFlatFilesClient()

    result = module._sync_trade_date(
        store,
        flatfiles,
        date(2025, 4, 1),
        symbols={"AAPL", "MSFT"},
        batch_size=2,
    )

    assert result.stock_count == 0
    assert result.option_count == 1
    assert result.stock_error is not None
    assert result.option_error is None

    gateway = HistoricalOptionGateway(store, "AAPL")
    quote = gateway.get_quote("O:AAPL250418C00190000", date(2025, 4, 1))
    assert quote is not None


def test_sync_trade_date_collects_imported_symbols_for_enrichment() -> None:
    module = _load_sync_script_module()
    flatfiles = _FakeFlatFilesClient()

    result = module._sync_trade_date(
        None,
        flatfiles,
        date(2025, 4, 1),
        symbols=None,
        batch_size=2,
        dry_run=True,
    )

    assert result.stock_symbols == ("AAPL", "MSFT")
    assert result.option_symbols == ("AAPL",)


def test_sync_trade_dates_supports_parallel_workers_and_keeps_date_order() -> None:
    module = _load_sync_script_module()
    flatfiles = _FakeFlatFilesClient()

    results = module._sync_trade_dates(
        None,
        flatfiles,
        [date(2025, 4, 1), date(2025, 4, 2)],
        symbols={"AAPL", "MSFT"},
        batch_size=2,
        dry_run=True,
        workers=2,
    )

    assert [item.trade_date for item in results] == [date(2025, 4, 1), date(2025, 4, 2)]
    assert [item.stock_count for item in results] == [2, 2]
    assert [item.option_count for item in results] == [1, 1]


def test_iter_sync_trade_dates_streams_completed_parallel_results() -> None:
    module = _load_sync_script_module()
    flatfiles = _DelayedFlatFilesClient()

    results = list(module._iter_sync_trade_dates(
        None,
        flatfiles,
        [date(2025, 4, 1), date(2025, 4, 2)],
        symbols={"AAPL", "MSFT"},
        batch_size=2,
        dry_run=True,
        workers=2,
    ))

    assert [item.trade_date for item in results] == [date(2025, 4, 2), date(2025, 4, 1)]


def test_maybe_enrich_trade_date_can_be_skipped() -> None:
    module = _load_sync_script_module()
    store = _store()
    rest_client = _FakeRestClient()
    result = module.TradeDateSyncResult(trade_date=date(2025, 4, 1), stock_count=2, option_count=1)

    module._maybe_enrich_trade_date(
        store,
        rest_client,
        result,
        symbols={"AAPL"},
        skip_rest_enrichment=True,
    )

    assert rest_client.treasury_calls == []
    assert rest_client.dividend_calls == []
    assert store.get_average_treasury_yield(date(2025, 4, 1), date(2025, 4, 1)) is None


def test_maybe_enrich_trade_date_writes_treasury_and_dividends_when_enabled() -> None:
    module = _load_sync_script_module()
    store = _store()
    rest_client = _FakeRestClient()
    result = module.TradeDateSyncResult(trade_date=date(2025, 4, 1), stock_count=2, option_count=1)

    module._maybe_enrich_trade_date(
        store,
        rest_client,
        result,
        symbols={"AAPL", "MSFT"},
        skip_rest_enrichment=False,
    )

    assert rest_client.treasury_calls == [date(2025, 4, 1)]
    assert rest_client.dividend_calls == [("AAPL", date(2025, 4, 1)), ("MSFT", date(2025, 4, 1))]
    assert store.get_average_treasury_yield(date(2025, 4, 1), date(2025, 4, 1)) == 0.041
    assert store.list_ex_dividend_dates("AAPL", date(2025, 4, 1), date(2025, 4, 1)) == {date(2025, 4, 1)}


def test_maybe_enrich_trade_date_uses_imported_symbols_when_allowlist_is_empty() -> None:
    module = _load_sync_script_module()
    store = _store()
    rest_client = _FakeRestClient()
    result = module.TradeDateSyncResult(
        trade_date=date(2025, 4, 1),
        stock_count=2,
        option_count=1,
        stock_symbols=("AAPL", "MSFT"),
        option_symbols=("AAPL",),
    )

    module._maybe_enrich_trade_date(
        store,
        rest_client,
        result,
        symbols=set(),
        skip_rest_enrichment=False,
    )

    assert rest_client.treasury_calls == [date(2025, 4, 1)]
    assert rest_client.dividend_calls == [("AAPL", date(2025, 4, 1)), ("MSFT", date(2025, 4, 1))]
    assert store.list_ex_dividend_dates("AAPL", date(2025, 4, 1), date(2025, 4, 1)) == {date(2025, 4, 1)}


def test_print_trade_date_result_includes_progress_prefix_and_flushes() -> None:
    module = _load_sync_script_module()
    result = module.TradeDateSyncResult(
        trade_date=date(2025, 4, 1),
        stock_count=2,
        option_count=1,
    )

    with patch("builtins.print") as mock_print:
        module._print_trade_date_result(
            result,
            dry_run=False,
            completed=3,
            total=10,
        )

    mock_print.assert_called_once()
    args, kwargs = mock_print.call_args
    assert args[0].startswith("[3/10] 2025-04-01: synced stock_rows=2 option_rows=1")
    assert kwargs["flush"] is True


def test_write_status_snapshot_preserves_existing_launcher_metadata() -> None:
    module = _load_sync_script_module()
    status_path = _status_file_path()
    try:
        _seed_status_file(status_path)

        module._write_status_snapshot(
            str(status_path),
            status="running",
            completed_trade_dates=3,
            total_trade_dates=10,
            last_completed_trade_date="2025-04-01",
        )

        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["started_at"] == "2026-03-29T14:02:55-05:00"
        assert payload["stdout_log_path"] == "stdout.log"
        assert payload["status"] == "running"
        assert payload["completed_trade_dates"] == 3
        assert payload["total_trade_dates"] == 10
        assert payload["last_completed_trade_date"] == "2025-04-01"
    finally:
        status_path.unlink(missing_ok=True)


def test_write_status_snapshot_preserves_unknown_launcher_fields() -> None:
    module = _load_sync_script_module()
    status_path = _status_file_path()
    try:
        _seed_status_file(
            status_path,
            stdout_log_path="stdout.log",
            log_path="combined.log",
            launcher_version="v2",
            custom_metadata={"source": "launcher", "attempt": 1},
        )

        module._write_status_snapshot(
            str(status_path),
            status="running",
            completed_trade_dates=2,
        )

        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["launcher_version"] == "v2"
        assert payload["custom_metadata"] == {"source": "launcher", "attempt": 1}
        assert payload["log_path"] == "combined.log"
        assert payload["status"] == "running"
        assert payload["completed_trade_dates"] == 2
    finally:
        status_path.unlink(missing_ok=True)


def test_write_status_snapshot_preserves_nested_launcher_metadata_with_runtime_nested_fields() -> None:
    module = _load_sync_script_module()
    status_path = _status_file_path()
    try:
        _seed_status_file(
            status_path,
            custom_metadata={"source": "launcher", "nested": {"attempt": 1, "mode": "bg"}},
        )

        module._write_status_snapshot(
            str(status_path),
            status="running",
            last_result={
                "trade_date": "2025-04-01",
                "stock_count": 2,
                "option_count": 1,
                "stock_error": None,
                "option_error": None,
                "stock_symbols": ["AAPL"],
                "option_symbols": ["AAPL"],
            },
        )

        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["custom_metadata"] == {"source": "launcher", "nested": {"attempt": 1, "mode": "bg"}}
        assert payload["last_result"]["trade_date"] == "2025-04-01"
        assert payload["last_result"]["stock_symbols"] == ["AAPL"]
    finally:
        status_path.unlink(missing_ok=True)


def test_write_status_snapshot_recovers_from_malformed_json() -> None:
    module = _load_sync_script_module()
    status_path = _status_file_path()
    try:
        status_path.write_text("{not-json", encoding="utf-8")

        module._write_status_snapshot(
            str(status_path),
            status="running",
            completed_trade_dates=1,
            completed_pct=50.0,
        )

        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "running"
        assert payload["completed_trade_dates"] == 1
        assert payload["completed_pct"] == 50.0
    finally:
        status_path.unlink(missing_ok=True)


def test_result_status_payload_serializes_symbols_and_errors() -> None:
    module = _load_sync_script_module()
    result = module.TradeDateSyncResult(
        trade_date=date(2025, 4, 1),
        stock_count=2,
        option_count=1,
        stock_error="missing stock file",
        option_error=None,
        stock_symbols=("AAPL", "MSFT"),
        option_symbols=("AAPL",),
    )

    payload = module._result_status_payload(result)

    assert payload == {
        "trade_date": "2025-04-01",
        "stock_count": 2,
        "option_count": 1,
        "stock_error": "missing stock file",
        "option_error": None,
        "stock_symbols": ["AAPL", "MSFT"],
        "option_symbols": ["AAPL"],
    }


def test_result_status_payload_serializes_dual_errors() -> None:
    module = _load_sync_script_module()
    result = module.TradeDateSyncResult(
        trade_date=date(2025, 4, 2),
        stock_count=0,
        option_count=0,
        stock_error="missing stock file",
        option_error="missing option file",
    )

    payload = module._result_status_payload(result)

    assert payload == {
        "trade_date": "2025-04-02",
        "stock_count": 0,
        "option_count": 0,
        "stock_error": "missing stock file",
        "option_error": "missing option file",
        "stock_symbols": [],
        "option_symbols": [],
    }


def test_write_status_snapshot_keeps_completed_at_null_until_completion() -> None:
    module = _load_sync_script_module()
    status_path = _status_file_path()
    try:
        _seed_status_file(status_path)

        module._write_status_snapshot(
            str(status_path),
            status="running",
            completed_trade_dates=0,
            completed_at=None,
        )
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "running"
        assert payload["completed_at"] is None

        module._write_status_snapshot(
            str(status_path),
            status="running",
            completed_trade_dates=1,
        )
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "running"
        assert payload["completed_trade_dates"] == 1
        assert payload["completed_at"] is None

        module._write_status_snapshot(
            str(status_path),
            status="completed",
            completed_trade_dates=1,
            completed_at="2026-03-29T15:00:00-05:00",
        )
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "completed"
        assert payload["completed_trade_dates"] == 1
        assert payload["completed_at"] == "2026-03-29T15:00:00-05:00"
    finally:
        status_path.unlink(missing_ok=True)


def test_write_status_snapshot_keeps_failed_at_null_until_failure() -> None:
    module = _load_sync_script_module()
    status_path = _status_file_path()
    try:
        _seed_status_file(status_path)

        module._write_status_snapshot(
            str(status_path),
            status="running",
            completed_trade_dates=0,
            failed_at=None,
        )
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "running"
        assert payload["failed_at"] is None

        module._write_status_snapshot(
            str(status_path),
            status="running",
            completed_trade_dates=1,
        )
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "running"
        assert payload["completed_trade_dates"] == 1
        assert payload["failed_at"] is None

        module._write_status_snapshot(
            str(status_path),
            status="failed",
            failed_at="2026-03-29T15:05:00-05:00",
            error="boom",
        )
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "failed"
        assert payload["failed_at"] == "2026-03-29T15:05:00-05:00"
        assert payload["error"] == "boom"
    finally:
        status_path.unlink(missing_ok=True)


def test_write_status_snapshot_completion_clears_prior_error() -> None:
    module = _load_sync_script_module()
    status_path = _status_file_path()
    try:
        _seed_status_file(status_path)

        module._write_status_snapshot(
            str(status_path),
            status="failed",
            error="boom",
            failed_at="2026-03-29T15:05:00-05:00",
        )
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "failed"
        assert payload["error"] == "boom"

        module._write_status_snapshot(
            str(status_path),
            status="completed",
            completed_at="2026-03-29T15:10:00-05:00",
            error=None,
        )
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "completed"
        assert payload["completed_at"] == "2026-03-29T15:10:00-05:00"
        assert payload["error"] is None
    finally:
        status_path.unlink(missing_ok=True)


def test_write_status_snapshot_failure_preserves_prior_completed_at() -> None:
    module = _load_sync_script_module()
    status_path = _status_file_path()
    try:
        _seed_status_file(status_path)

        module._write_status_snapshot(
            str(status_path),
            status="completed",
            completed_at="2026-03-29T15:10:00-05:00",
        )
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["completed_at"] == "2026-03-29T15:10:00-05:00"

        module._write_status_snapshot(
            str(status_path),
            status="failed",
            failed_at="2026-03-29T15:15:00-05:00",
            error="boom",
        )
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "failed"
        assert payload["failed_at"] == "2026-03-29T15:15:00-05:00"
        assert payload["completed_at"] == "2026-03-29T15:10:00-05:00"
    finally:
        status_path.unlink(missing_ok=True)


def test_write_status_snapshot_running_preserves_prior_last_result_without_replacement() -> None:
    module = _load_sync_script_module()
    status_path = _status_file_path()
    try:
        _seed_status_file(status_path)
        initial_result = module._result_status_payload(
            module.TradeDateSyncResult(
                trade_date=date(2025, 4, 1),
                stock_count=2,
                option_count=1,
                stock_symbols=("AAPL",),
            )
        )

        module._write_status_snapshot(
            str(status_path),
            status="running",
            last_result=initial_result,
        )
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["last_result"] == initial_result

        module._write_status_snapshot(
            str(status_path),
            status="running",
            completed_trade_dates=2,
        )
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "running"
        assert payload["completed_trade_dates"] == 2
        assert payload["last_result"] == initial_result
    finally:
        status_path.unlink(missing_ok=True)


def test_write_status_snapshot_preserves_launcher_command_field() -> None:
    module = _load_sync_script_module()
    status_path = _status_file_path()
    try:
        _seed_status_file(
            status_path,
            command="python scripts/sync_historical_market_data.py --start-date 2025-04-01 --end-date 2025-04-02",
        )

        module._write_status_snapshot(
            str(status_path),
            status="running",
            completed_trade_dates=1,
        )

        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["command"] == "python scripts/sync_historical_market_data.py --start-date 2025-04-01 --end-date 2025-04-02"
        assert payload["status"] == "running"
        assert payload["completed_trade_dates"] == 1
    finally:
        status_path.unlink(missing_ok=True)


def test_write_status_snapshot_completion_preserves_prior_failed_at_without_override() -> None:
    module = _load_sync_script_module()
    status_path = _status_file_path()
    try:
        _seed_status_file(status_path)

        module._write_status_snapshot(
            str(status_path),
            status="failed",
            failed_at="2026-03-29T15:05:00-05:00",
            error="boom",
        )
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["failed_at"] == "2026-03-29T15:05:00-05:00"

        module._write_status_snapshot(
            str(status_path),
            status="completed",
            completed_at="2026-03-29T15:10:00-05:00",
            error=None,
        )
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "completed"
        assert payload["completed_at"] == "2026-03-29T15:10:00-05:00"
        assert payload["failed_at"] == "2026-03-29T15:05:00-05:00"
    finally:
        status_path.unlink(missing_ok=True)


def test_write_status_snapshot_failure_preserves_prior_last_result() -> None:
    module = _load_sync_script_module()
    status_path = _status_file_path()
    try:
        _seed_status_file(status_path)
        last_result = module._result_status_payload(
            module.TradeDateSyncResult(
                trade_date=date(2025, 4, 1),
                stock_count=2,
                option_count=1,
                stock_symbols=("AAPL",),
            )
        )

        module._write_status_snapshot(
            str(status_path),
            status="running",
            last_result=last_result,
        )
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["last_result"] == last_result

        module._write_status_snapshot(
            str(status_path),
            status="failed",
            error="boom",
            failed_at="2026-03-29T15:05:00-05:00",
        )
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "failed"
        assert payload["error"] == "boom"
        assert payload["last_result"] == last_result
    finally:
        status_path.unlink(missing_ok=True)


def test_write_status_snapshot_overwrites_last_result_only_when_explicitly_supplied() -> None:
    module = _load_sync_script_module()
    status_path = _status_file_path()
    try:
        _seed_status_file(status_path)
        first_result = module._result_status_payload(
            module.TradeDateSyncResult(
                trade_date=date(2025, 4, 1),
                stock_count=2,
                option_count=1,
                stock_symbols=("AAPL",),
            )
        )
        second_result = module._result_status_payload(
            module.TradeDateSyncResult(
                trade_date=date(2025, 4, 2),
                stock_count=3,
                option_count=2,
                stock_symbols=("MSFT",),
            )
        )

        module._write_status_snapshot(
            str(status_path),
            status="running",
            last_result=first_result,
        )
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["last_result"] == first_result

        module._write_status_snapshot(
            str(status_path),
            status="running",
            completed_trade_dates=2,
        )
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["last_result"] == first_result

        module._write_status_snapshot(
            str(status_path),
            status="running",
            last_result=second_result,
        )
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["last_result"] == second_result
    finally:
        status_path.unlink(missing_ok=True)


def test_main_updates_status_file_to_completed() -> None:
    module = _load_sync_script_module()
    status_path = _status_file_path()
    results = iter([
        module.TradeDateSyncResult(
            trade_date=date(2025, 4, 1),
            stock_count=2,
            option_count=1,
        ),
        module.TradeDateSyncResult(
            trade_date=date(2025, 4, 2),
            stock_count=3,
            option_count=2,
        ),
    ])
    try:
        _seed_status_file(status_path)
        assert _run_main(
            module,
            status_path=status_path,
            start_date="2025-04-01",
            end_date="2025-04-02",
            trade_dates=[date(2025, 4, 1), date(2025, 4, 2)],
            results=results,
            include_status_flag=True,
        ) == 0

        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "completed"
        assert payload["completed_trade_dates"] == 2
        assert payload["completed_pct"] == 100.0
        assert payload["processed_trade_dates"] == 2
        assert payload["processed_pct"] == 100.0
        assert payload["completed_stock_rows"] == 5
        assert payload["completed_option_rows"] == 3
        assert payload["last_completed_trade_date"] == "2025-04-02"
        assert payload["last_result"]["trade_date"] == "2025-04-02"
        assert payload["window_coverage_status"] == "complete"
        assert payload["remaining_trade_dates"] == 0
        assert payload["started_at"] == "2026-03-29T14:02:55-05:00"
        assert payload["python_pid"] == 12345
        assert payload["stdout_log_path"] == "stdout.log"
        assert payload["stderr_log_path"] == "stderr.log"
    finally:
        status_path.unlink(missing_ok=True)


def test_main_updates_status_file_to_failed_on_exception() -> None:
    module = _load_sync_script_module()
    status_path = _status_file_path()
    try:
        _seed_status_file(status_path)
        try:
            _run_main(
                module,
                status_path=status_path,
                start_date="2025-04-01",
                end_date="2025-04-02",
                trade_dates=[date(2025, 4, 1), date(2025, 4, 2)],
                sync_side_effect=RuntimeError("boom"),
                include_status_flag=True,
            )
        except RuntimeError as exc:
            assert str(exc) == "boom"
        else:
            raise AssertionError("Expected RuntimeError from _iter_sync_trade_dates")

        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "failed"
        assert payload["error"] == "boom"
        assert payload["failed_at"] == payload["updated_at"]
        assert payload["started_at"] == "2026-03-29T14:02:55-05:00"
        assert payload["python_pid"] == 12345
        assert payload["stdout_log_path"] == "stdout.log"
        assert payload["stderr_log_path"] == "stderr.log"
    finally:
        status_path.unlink(missing_ok=True)


def test_main_honors_status_path_env_var_when_flag_is_omitted() -> None:
    module = _load_sync_script_module()
    status_path = _status_file_path()
    results = iter([
        module.TradeDateSyncResult(
            trade_date=date(2025, 4, 1),
            stock_count=2,
            option_count=1,
        )
    ])
    try:
        _seed_status_file(status_path)
        assert _run_main(
            module,
            status_path=status_path,
            trade_dates=[date(2025, 4, 1)],
            results=results,
            env={"BACKTESTFORECAST_IMPORT_STATUS_PATH": str(status_path)},
        ) == 0

        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "completed"
        assert payload["completed_trade_dates"] == 1
        assert payload["last_completed_trade_date"] == "2025-04-01"
        assert payload["status_path"] == str(status_path)
    finally:
        status_path.unlink(missing_ok=True)


def test_main_honors_historical_import_status_path_env_fallback() -> None:
    module = _load_sync_script_module()
    status_path = _status_file_path()
    results = iter([
        module.TradeDateSyncResult(
            trade_date=date(2025, 4, 1),
            stock_count=2,
            option_count=1,
        )
    ])
    try:
        _seed_status_file(status_path)
        assert _run_main(
            module,
            status_path=status_path,
            trade_dates=[date(2025, 4, 1)],
            results=results,
            env={"HISTORICAL_IMPORT_STATUS_PATH": str(status_path)},
        ) == 0

        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "completed"
        assert payload["completed_trade_dates"] == 1
        assert payload["last_completed_trade_date"] == "2025-04-01"
        assert payload["status_path"] == str(status_path)
    finally:
        status_path.unlink(missing_ok=True)


def test_main_keeps_completed_pct_zero_for_empty_trading_window() -> None:
    module = _load_sync_script_module()
    status_path = _status_file_path()
    try:
        _seed_status_file(status_path)
        assert _run_main(
            module,
            status_path=status_path,
            trade_dates=[],
            include_status_flag=True,
        ) == 0

        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "completed"
        assert payload["completed_trade_dates"] == 0
        assert payload["completed_pct"] == 0.0
        assert payload["processed_trade_dates"] == 0
        assert payload["processed_pct"] == 0.0
        assert payload["last_completed_trade_date"] is None
        assert payload["last_result"] is None
    finally:
        status_path.unlink(missing_ok=True)


def test_main_marks_completion_with_errors_when_trade_dates_remain_incomplete() -> None:
    module = _load_sync_script_module()
    status_path = _status_file_path()
    results = iter([
        module.TradeDateSyncResult(
            trade_date=date(2025, 4, 1),
            stock_count=2,
            option_count=0,
            stock_error=None,
            option_error="missing option flat file",
        )
    ])
    try:
        _seed_status_file(status_path)
        assert _run_main(
            module,
            status_path=status_path,
            trade_dates=[date(2025, 4, 1)],
            results=results,
            include_status_flag=True,
        ) == 0

        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "completed_with_errors"
        assert payload["completed_trade_dates"] == 0
        assert payload["processed_trade_dates"] == 1
        assert payload["remaining_trade_dates"] == 1
        assert payload["next_pending_trade_date"] == "2025-04-01"
        assert payload["window_coverage_status"] == "incomplete"
        assert payload["window_freshness_trade_date_lag"] == 1
        assert payload["error"].startswith("1 trade date(s) remain incomplete.")
        assert payload["trade_date_checkpoints"]["2025-04-01"]["option_error"] == "missing option flat file"
    finally:
        status_path.unlink(missing_ok=True)


def test_main_resume_skips_completed_trade_dates_and_retries_incomplete_dates() -> None:
    module = _load_sync_script_module()
    status_path = _status_file_path()
    results = iter([
        module.TradeDateSyncResult(
            trade_date=date(2025, 4, 2),
            stock_count=3,
            option_count=2,
            stock_error=None,
            option_error=None,
        )
    ])
    try:
        _seed_status_file(
            status_path,
            run_signature={
                "window_start": "2025-04-01",
                "window_end": "2025-04-02",
                "symbols": [],
                "dry_run": True,
                "skip_rest_enrichment": False,
            },
            trade_date_checkpoints={
                "2025-04-01": {
                    "stock_count": 2,
                    "option_count": 1,
                    "stock_error": None,
                    "option_error": None,
                },
                "2025-04-02": {
                    "stock_count": 3,
                    "option_count": 0,
                    "stock_error": None,
                    "option_error": "missing option flat file",
                },
            },
            completed_trade_dates=1,
            processed_trade_dates=2,
            completed_pct=50.0,
            processed_pct=100.0,
            completed_trade_dates_list=["2025-04-01"],
            processed_trade_dates_list=["2025-04-01", "2025-04-02"],
            remaining_trade_dates=1,
            next_pending_trade_date="2025-04-02",
            window_target_trade_date="2025-04-02",
            window_coverage_status="incomplete",
            window_freshness_trade_date_lag=1,
        )

        assert _run_main(
            module,
            status_path=status_path,
            start_date="2025-04-01",
            end_date="2025-04-02",
            trade_dates=[date(2025, 4, 1), date(2025, 4, 2)],
            results=results,
            include_status_flag=True,
            resume=True,
        ) == 0

        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "completed"
        assert payload["resume_requested"] is True
        assert payload["resume_applied"] is True
        assert payload["resumed_trade_dates"] == 1
        assert payload["completed_trade_dates"] == 2
        assert payload["processed_trade_dates"] == 2
        assert payload["remaining_trade_dates"] == 0
        assert payload["completed_stock_rows"] == 5
        assert payload["completed_option_rows"] == 3
        assert payload["trade_date_checkpoints"]["2025-04-02"]["option_error"] is None
    finally:
        status_path.unlink(missing_ok=True)


def test_main_resume_rejects_checkpoint_signature_mismatch() -> None:
    module = _load_sync_script_module()
    status_path = _status_file_path()
    try:
        _seed_status_file(
            status_path,
            run_signature={
                "window_start": "2025-04-03",
                "window_end": "2025-04-03",
                "symbols": ["SPY"],
                "dry_run": True,
                "skip_rest_enrichment": False,
            },
            trade_date_checkpoints={
                "2025-04-03": {
                    "stock_count": 2,
                    "option_count": 1,
                    "stock_error": None,
                    "option_error": None,
                }
            },
        )

        with pytest.raises(ValueError, match="different import window or symbol set"):
            _run_main(
                module,
                status_path=status_path,
                trade_dates=[date(2025, 4, 1)],
                results=iter(()),
                include_status_flag=True,
                resume=True,
            )
    finally:
        status_path.unlink(missing_ok=True)


def test_main_prefers_primary_status_path_env_var_over_secondary() -> None:
    module = _load_sync_script_module()
    primary_status_path = _status_file_path()
    secondary_status_path = _status_file_path()
    results = iter([
        module.TradeDateSyncResult(
            trade_date=date(2025, 4, 1),
            stock_count=2,
            option_count=1,
        )
    ])
    try:
        _seed_status_file(primary_status_path)
        _seed_status_file(secondary_status_path)
        assert _run_main(
            module,
            trade_dates=[date(2025, 4, 1)],
            results=results,
            env={
                "BACKTESTFORECAST_IMPORT_STATUS_PATH": str(primary_status_path),
                "HISTORICAL_IMPORT_STATUS_PATH": str(secondary_status_path),
            },
        ) == 0

        primary_payload = json.loads(primary_status_path.read_text(encoding="utf-8"))
        secondary_payload = json.loads(secondary_status_path.read_text(encoding="utf-8"))
        assert primary_payload["status"] == "completed"
        assert primary_payload["completed_trade_dates"] == 1
        assert secondary_payload["status_path"] == str(secondary_status_path)
        assert "completed_trade_dates" not in secondary_payload
    finally:
        primary_status_path.unlink(missing_ok=True)
        secondary_status_path.unlink(missing_ok=True)


def test_main_prefers_flag_status_path_over_env_vars() -> None:
    module = _load_sync_script_module()
    flag_status_path = _status_file_path()
    primary_status_path = _status_file_path()
    secondary_status_path = _status_file_path()
    results = iter([
        module.TradeDateSyncResult(
            trade_date=date(2025, 4, 1),
            stock_count=2,
            option_count=1,
        )
    ])
    try:
        _seed_status_file(flag_status_path)
        _seed_status_file(primary_status_path)
        _seed_status_file(secondary_status_path)
        assert _run_main(
            module,
            status_path=flag_status_path,
            trade_dates=[date(2025, 4, 1)],
            results=results,
            env={
                "BACKTESTFORECAST_IMPORT_STATUS_PATH": str(primary_status_path),
                "HISTORICAL_IMPORT_STATUS_PATH": str(secondary_status_path),
            },
            include_status_flag=True,
        ) == 0

        flag_payload = json.loads(flag_status_path.read_text(encoding="utf-8"))
        primary_payload = json.loads(primary_status_path.read_text(encoding="utf-8"))
        secondary_payload = json.loads(secondary_status_path.read_text(encoding="utf-8"))
        assert flag_payload["status"] == "completed"
        assert flag_payload["completed_trade_dates"] == 1
        assert "completed_trade_dates" not in primary_payload
        assert "completed_trade_dates" not in secondary_payload
    finally:
        flag_status_path.unlink(missing_ok=True)
        primary_status_path.unlink(missing_ok=True)
        secondary_status_path.unlink(missing_ok=True)
