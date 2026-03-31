from __future__ import annotations

from contextlib import ExitStack
import importlib.util
import json
import os
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from backtestforecast.db.base import Base
from backtestforecast.errors import ExternalServiceError
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore
from backtestforecast.market_data.types import ExDividendRecord
from backtestforecast.models import HistoricalExDividendDate


def _load_script_module():
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "backfill_ex_dividend_dates.py"
    sys.path.insert(0, str(script_path.parent))
    try:
        spec = importlib.util.spec_from_file_location("backfill_ex_dividend_dates", script_path)
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
    return Path(f"dividend-backfill-status-{uuid4().hex}.json")


def _seed_status_file(status_path: Path, **overrides: object) -> None:
    payload: dict[str, object] = {
        "started_at": "2026-03-30T15:10:00-05:00",
        "python_pid": 12345,
        "status_path": str(status_path),
    }
    payload.update(overrides)
    status_path.write_text(json.dumps(payload), encoding="utf-8")


class _FakeDividendClient:
    def __init__(
        self,
        dates_by_symbol: dict[str, set[date]] | None = None,
        *,
        optionable_symbols: list[str] | None = None,
    ) -> None:
        self._dates_by_symbol = {key.upper(): set(value) for key, value in (dates_by_symbol or {}).items()}
        self.calls: list[tuple[str, date, date]] = []
        self.universe_calls: list[tuple[date, bool]] = []
        self._optionable_symbols = [item.upper() for item in (optionable_symbols or [])]

    def list_ex_dividend_dates(self, symbol: str, start_date: date, end_date: date) -> set[date]:
        self.calls.append((symbol, start_date, end_date))
        return {
            item
            for item in self._dates_by_symbol.get(symbol.upper(), set())
            if start_date <= item <= end_date
        }

    def list_ex_dividend_records(self, symbol: str, start_date: date, end_date: date) -> list[ExDividendRecord]:
        return [
            ExDividendRecord(
                ex_dividend_date=item,
                provider_dividend_id=f"{symbol.upper()}-{item.isoformat()}",
                cash_amount=0.15,
                currency="USD",
                declaration_date=item,
                record_date=item,
                pay_date=item,
                frequency=4,
                distribution_type="recurring",
                historical_adjustment_factor=1.0,
                split_adjusted_cash_amount=0.15,
            )
            for item in sorted(self.list_ex_dividend_dates(symbol, start_date, end_date))
        ]

    def list_optionable_underlyings(self, *, as_of_date: date, include_expired: bool = True) -> list[str]:
        self.universe_calls.append((as_of_date, include_expired))
        if self._optionable_symbols:
            return sorted(self._optionable_symbols)
        return sorted(self._dates_by_symbol)

    def close(self) -> None:
        return None


class _FakeFlatFilesClient:
    def __init__(self, rows_by_date: dict[date, list[dict[str, str]]] | None = None) -> None:
        self._rows_by_date = rows_by_date or {}
        self.calls: list[tuple[str, date]] = []

    def iter_csv_rows(self, dataset: str, trade_date: date):
        self.calls.append((dataset, trade_date))
        yield from self._rows_by_date.get(trade_date, [])

    def close(self) -> None:
        return None


class _ErroringDividendClient(_FakeDividendClient):
    def __init__(
        self,
        *,
        failing_chunks: set[tuple[str, date, date]],
        dates_by_symbol: dict[str, set[date]] | None = None,
    ) -> None:
        super().__init__(dates_by_symbol)
        self._failing_chunks = failing_chunks

    def list_ex_dividend_dates(self, symbol: str, start_date: date, end_date: date) -> set[date]:
        self.calls.append((symbol, start_date, end_date))
        if (symbol.upper(), start_date, end_date) in self._failing_chunks:
            raise ExternalServiceError(f"upstream failed for {symbol} {start_date.isoformat()}..{end_date.isoformat()}")
        return {
            item
            for item in self._dates_by_symbol.get(symbol.upper(), set())
            if start_date <= item <= end_date
        }

    def list_ex_dividend_records(self, symbol: str, start_date: date, end_date: date) -> list[ExDividendRecord]:
        self.calls.append((symbol, start_date, end_date))
        if (symbol.upper(), start_date, end_date) in self._failing_chunks:
            raise ExternalServiceError(f"upstream failed for {symbol} {start_date.isoformat()}..{end_date.isoformat()}")
        return [
            ExDividendRecord(ex_dividend_date=item, provider_dividend_id=f"{symbol.upper()}-{item.isoformat()}")
            for item in sorted(
                {
                    item
                    for item in self._dates_by_symbol.get(symbol.upper(), set())
                    if start_date <= item <= end_date
                }
            )
        ]


def _run_main(
    module,
    *,
    store: HistoricalMarketDataStore | None = None,
    client: _FakeDividendClient | None = None,
    start_date: str = "2025-01-01",
    end_date: str = "2025-12-31",
    symbols: str = "",
    chunk_mode: str = "year",
    status_path: Path | None = None,
    resume: bool = False,
    dry_run: bool = False,
    status_write_every: int = 100,
    universe_source: str = "auto",
    optionable_as_of_date: str = "",
    config_symbols: list[str] | None = None,
    discovered_symbols: list[str] | None = None,
    env: dict[str, str] | None = None,
    flatfiles: _FakeFlatFilesClient | None = None,
) -> int:
    argv = [
        "backfill_ex_dividend_dates.py",
        "--start-date", start_date,
        "--end-date", end_date,
        "--chunk-mode", chunk_mode,
        "--universe-source", universe_source,
    ]
    if optionable_as_of_date:
        argv.extend(["--optionable-as-of-date", optionable_as_of_date])
    if symbols:
        argv.extend(["--symbols", symbols])
    if status_path is not None:
        argv.extend(["--status-path", str(status_path)])
    argv.extend(["--status-write-every", str(status_write_every)])
    if resume:
        argv.append("--resume")
    if dry_run:
        argv.append("--dry-run")

    with ExitStack() as stack:
        stack.enter_context(patch.object(sys, "argv", argv))
        if env is not None:
            stack.enter_context(patch.dict(os.environ, env, clear=False))
        stack.enter_context(
            patch.object(
                module,
                "get_settings",
                return_value=SimpleNamespace(
                    historical_data_start_year=2014,
                    historical_data_sync_symbols=config_symbols or [],
                ),
            )
        )
        if store is not None:
            stack.enter_context(patch.object(module, "HistoricalMarketDataStore", return_value=store))
        if client is None:
            client = _FakeDividendClient()
        stack.enter_context(patch.object(module, "MassiveClient", return_value=client))
        if flatfiles is None:
            flatfiles = _FakeFlatFilesClient()
        stack.enter_context(patch.object(module.MassiveFlatFilesClient, "from_settings", return_value=flatfiles))
        if discovered_symbols is not None:
            stack.enter_context(patch.object(module, "_discover_symbols", return_value=discovered_symbols))
        return module.main()


def test_iter_symbol_chunks_supports_year_and_quarter_modes() -> None:
    module = _load_script_module()

    yearly = list(
        module._iter_symbol_chunks(
            symbol="F",
            start_date=date(2014, 7, 1),
            end_date=date(2015, 3, 31),
            chunk_mode="year",
        )
    )
    quarterly = list(
        module._iter_symbol_chunks(
            symbol="F",
            start_date=date(2014, 7, 1),
            end_date=date(2015, 3, 31),
            chunk_mode="quarter",
        )
    )

    assert [(chunk.start_date, chunk.end_date) for chunk in yearly] == [
        (date(2014, 7, 1), date(2014, 12, 31)),
        (date(2015, 1, 1), date(2015, 3, 31)),
    ]
    assert [(chunk.start_date, chunk.end_date) for chunk in quarterly] == [
        (date(2014, 7, 1), date(2014, 9, 30)),
        (date(2014, 10, 1), date(2014, 12, 31)),
        (date(2015, 1, 1), date(2015, 3, 31)),
    ]


def test_should_write_progress_snapshot_batches_non_error_updates() -> None:
    module = _load_script_module()
    chunk = module.DividendBackfillChunk("F", date(2014, 7, 1), date(2014, 12, 31))
    result = module.ChunkSyncResult(chunk=chunk, dividend_count=1, stored_count=1, error=None)
    errored = module.ChunkSyncResult(chunk=chunk, dividend_count=0, stored_count=0, error="boom")

    assert module._should_write_progress_snapshot(processed_count=1, status_write_every=100, result=result) is False
    assert module._should_write_progress_snapshot(processed_count=100, status_write_every=100, result=result) is True
    assert module._should_write_progress_snapshot(processed_count=17, status_write_every=100, result=errored) is True
    assert module._should_write_progress_snapshot(processed_count=1, status_write_every=1, result=result) is True


def test_write_status_snapshot_retries_permission_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_script_module()
    status_path = _status_file_path()
    temp_path = status_path.with_suffix(status_path.suffix + ".tmp")
    original_replace = Path.replace
    attempts = {"count": 0}

    def flaky_replace(self: Path, target: Path):
        if self == temp_path and attempts["count"] < 2:
            attempts["count"] += 1
            raise PermissionError("locked")
        return original_replace(self, target)

    monkeypatch.setattr(Path, "replace", flaky_replace)

    try:
        assert module._write_status_snapshot(str(status_path), status="running", retry_delay_seconds=0) is True
        assert attempts["count"] == 2
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "running"
    finally:
        status_path.unlink(missing_ok=True)
        temp_path.unlink(missing_ok=True)


def test_write_status_snapshot_can_fail_softly() -> None:
    module = _load_script_module()
    status_path = _status_file_path()
    temp_path = status_path.with_suffix(status_path.suffix + ".tmp")

    try:
        with patch.object(Path, "replace", side_effect=PermissionError("locked")):
            assert (
                module._write_status_snapshot(
                    str(status_path),
                    allow_failure=True,
                    max_attempts=2,
                    retry_delay_seconds=0,
                    status="running",
                )
                is False
            )
    finally:
        status_path.unlink(missing_ok=True)
        temp_path.unlink(missing_ok=True)


def test_sync_chunk_upserts_symbol_range_rows() -> None:
    module = _load_script_module()
    store = _store()
    client = _FakeDividendClient(
        {
            "F": {
                date(2014, 7, 15),
                date(2015, 2, 13),
            }
        }
    )
    chunk = module.DividendBackfillChunk("F", date(2014, 7, 1), date(2014, 12, 31))

    result = module._sync_chunk(store, client, chunk, dry_run=False)

    assert result.error is None
    assert result.dividend_count == 1
    assert result.stored_count == 1
    assert store.list_ex_dividend_dates("F", date(2014, 7, 1), date(2014, 12, 31)) == {date(2014, 7, 15)}
    with store._session(readonly=True) as session:
        row = session.query(HistoricalExDividendDate).filter_by(symbol="F", ex_dividend_date=date(2014, 7, 15)).one()
    assert float(row.cash_amount) == pytest.approx(0.15)
    assert row.currency == "USD"
    assert row.frequency == 4
    assert row.distribution_type == "recurring"


def test_sync_chunk_preserves_same_day_dividends_with_distinct_provider_ids() -> None:
    module = _load_script_module()
    store = _store()

    class _DuplicateDayClient(_FakeDividendClient):
        def list_ex_dividend_records(self, symbol: str, start_date: date, end_date: date) -> list[ExDividendRecord]:
            self.calls.append((symbol, start_date, end_date))
            return [
                ExDividendRecord(
                    ex_dividend_date=date(2016, 1, 27),
                    provider_dividend_id="div-recurring",
                    cash_amount=0.15,
                    distribution_type="recurring",
                ),
                ExDividendRecord(
                    ex_dividend_date=date(2016, 1, 27),
                    provider_dividend_id="div-supplemental",
                    cash_amount=0.25,
                    distribution_type="supplemental",
                ),
            ]

    chunk = module.DividendBackfillChunk("F", date(2016, 1, 1), date(2016, 12, 31))
    result = module._sync_chunk(store, _DuplicateDayClient(), chunk, dry_run=False)

    assert result.error is None
    assert result.dividend_count == 2
    assert result.stored_count == 2
    assert store.list_ex_dividend_dates("F", date(2016, 1, 1), date(2016, 12, 31)) == {date(2016, 1, 27)}
    with store._session(readonly=True) as session:
        rows = list(
            session.query(HistoricalExDividendDate)
            .filter_by(symbol="F", ex_dividend_date=date(2016, 1, 27))
            .order_by(HistoricalExDividendDate.provider_dividend_id)
        )
    assert [row.provider_dividend_id for row in rows] == ["div-recurring", "div-supplemental"]


def test_main_backfills_quarter_chunks_and_updates_status() -> None:
    module = _load_script_module()
    store = _store()
    client = _FakeDividendClient(
        {
            "F": {
                date(2014, 7, 15),
                date(2014, 10, 15),
                date(2015, 1, 15),
            }
        }
    )
    status_path = _status_file_path()

    try:
        exit_code = _run_main(
            module,
            store=store,
            client=client,
            start_date="2014-07-01",
            end_date="2015-03-31",
            symbols="F",
            chunk_mode="quarter",
            status_path=status_path,
        )

        assert exit_code == 0
        assert client.calls == [
            ("F", date(2014, 7, 1), date(2014, 9, 30)),
            ("F", date(2014, 10, 1), date(2014, 12, 31)),
            ("F", date(2015, 1, 1), date(2015, 3, 31)),
        ]
        assert store.list_ex_dividend_dates("F", date(2014, 7, 1), date(2015, 3, 31)) == {
            date(2014, 7, 15),
            date(2014, 10, 15),
            date(2015, 1, 15),
        }

        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "completed"
        assert payload["chunk_mode"] == "quarter"
        assert payload["resolved_symbols"] == ["F"]
        assert payload["total_chunks"] == 3
        assert payload["completed_chunks"] == 3
        assert payload["completed_dividend_rows"] == 3
        assert payload["completed_stored_rows"] == 3
        assert payload["next_pending_chunk"] is None
    finally:
        status_path.unlink(missing_ok=True)


def test_main_discovers_symbols_when_no_explicit_or_configured_symbols_exist() -> None:
    module = _load_script_module()
    client = _FakeDividendClient(
        {
            "AAPL": {date(2025, 2, 7)},
            "MSFT": {date(2025, 2, 20)},
        }
    )

    exit_code = _run_main(
        module,
        client=client,
        start_date="2025-01-01",
        end_date="2025-12-31",
        dry_run=True,
        discovered_symbols=["AAPL", "MSFT"],
    )

    assert exit_code == 0
    assert client.calls == [
        ("AAPL", date(2025, 1, 1), date(2025, 12, 31)),
        ("MSFT", date(2025, 1, 1), date(2025, 12, 31)),
    ]


def test_main_can_resolve_symbols_from_massive_optionable_universe() -> None:
    module = _load_script_module()
    client = _FakeDividendClient(
        {
            "AAPL": {date(2025, 2, 7)},
            "MSFT": {date(2025, 2, 20)},
        },
        optionable_symbols=["MSFT", "AAPL"],
    )

    exit_code = _run_main(
        module,
        client=client,
        start_date="2025-01-01",
        end_date="2025-12-31",
        dry_run=True,
        universe_source="massive-optionable",
        discovered_symbols=["SHOULD_NOT_BE_USED"],
    )

    assert exit_code == 0
    assert client.universe_calls == [(date(2025, 12, 31), True)]
    assert client.calls == [
        ("AAPL", date(2025, 1, 1), date(2025, 12, 31)),
        ("MSFT", date(2025, 1, 1), date(2025, 12, 31)),
    ]


def test_main_can_resolve_symbols_from_flatfile_optionable_universe() -> None:
    module = _load_script_module()
    client = _FakeDividendClient(
        {
            "AAPL": {date(2025, 2, 7)},
            "MSFT": {date(2025, 2, 20)},
        }
    )
    flatfiles = _FakeFlatFilesClient(
        {
            date(2026, 3, 27): [
                {"ticker": "O:AAPL260417C00180000"},
                {"ticker": "O:MSFT260417P00350000"},
                {"ticker": "O:AAPL260417P00170000"},
                {"ticker": "INVALID"},
            ]
        }
    )

    exit_code = _run_main(
        module,
        client=client,
        flatfiles=flatfiles,
        start_date="2025-01-01",
        end_date="2025-12-31",
        dry_run=True,
        universe_source="flatfile-optionable",
        optionable_as_of_date="2026-03-27",
        discovered_symbols=["SHOULD_NOT_BE_USED"],
    )

    assert exit_code == 0
    assert flatfiles.calls == [(module.option_day_dataset(), date(2026, 3, 27))]
    assert client.calls == [
        ("AAPL", date(2025, 1, 1), date(2025, 12, 31)),
        ("MSFT", date(2025, 1, 1), date(2025, 12, 31)),
    ]


def test_main_resume_skips_completed_chunks_and_retries_incomplete_chunks() -> None:
    module = _load_script_module()
    store = _store()
    client = _FakeDividendClient(
        {
            "F": {
                date(2014, 10, 15),
            }
        }
    )
    status_path = _status_file_path()

    try:
        _seed_status_file(
            status_path,
            run_signature={
                "window_start": "2014-07-01",
                "window_end": "2014-12-31",
                "symbols": ["F"],
                "universe_source": "auto",
                "optionable_as_of_date": None,
                "chunk_mode": "quarter",
                "dry_run": False,
            },
            chunk_checkpoints={
                "F|2014-07-01|2014-09-30": {
                    "symbol": "F",
                    "chunk_start": "2014-07-01",
                    "chunk_end": "2014-09-30",
                    "dividend_count": 1,
                    "stored_count": 1,
                    "error": None,
                },
                "F|2014-10-01|2014-12-31": {
                    "symbol": "F",
                    "chunk_start": "2014-10-01",
                    "chunk_end": "2014-12-31",
                    "dividend_count": 0,
                    "stored_count": 0,
                    "error": "upstream failed previously",
                },
            },
        )

        exit_code = _run_main(
            module,
            store=store,
            client=client,
            start_date="2014-07-01",
            end_date="2014-12-31",
            symbols="F",
            chunk_mode="quarter",
            status_path=status_path,
            resume=True,
        )

        assert exit_code == 0
        assert client.calls == [
            ("F", date(2014, 10, 1), date(2014, 12, 31)),
        ]
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["resume_requested"] is True
        assert payload["resume_applied"] is True
        assert payload["resumed_chunks"] == 1
        assert payload["status"] == "completed"
        assert payload["completed_chunks"] == 2
        assert payload["completed_stored_rows"] == 2
    finally:
        status_path.unlink(missing_ok=True)


def test_main_marks_completion_with_errors_when_chunk_fails() -> None:
    module = _load_script_module()
    status_path = _status_file_path()
    client = _ErroringDividendClient(
        failing_chunks={("F", date(2014, 10, 1), date(2014, 12, 31))},
        dates_by_symbol={"F": {date(2014, 7, 15)}},
    )

    try:
        exit_code = _run_main(
            module,
            client=client,
            start_date="2014-07-01",
            end_date="2014-12-31",
            symbols="F",
            chunk_mode="quarter",
            status_path=status_path,
            dry_run=True,
        )

        assert exit_code == 0
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "completed_with_errors"
        assert payload["remaining_chunks"] == 1
        assert "rerun with --resume" in payload["error"]
    finally:
        status_path.unlink(missing_ok=True)


def test_main_resume_rejects_signature_mismatch() -> None:
    module = _load_script_module()
    status_path = _status_file_path()

    try:
        _seed_status_file(
            status_path,
            run_signature={
                "window_start": "2014-07-01",
                "window_end": "2014-12-31",
                "symbols": ["AAPL"],
                "universe_source": "auto",
                "optionable_as_of_date": None,
                "chunk_mode": "year",
                "dry_run": False,
            },
        )

        with pytest.raises(ValueError, match="different dividend backfill window or symbol set"):
            _run_main(
                module,
                client=_FakeDividendClient(),
                start_date="2014-07-01",
                end_date="2014-12-31",
                symbols="F",
                chunk_mode="year",
                status_path=status_path,
                resume=True,
            )
    finally:
        status_path.unlink(missing_ok=True)


def test_main_rejects_non_positive_status_write_every() -> None:
    module = _load_script_module()

    with pytest.raises(ValueError, match="status-write-every"):
        _run_main(
            module,
            client=_FakeDividendClient(),
            start_date="2025-01-01",
            end_date="2025-12-31",
            symbols="F",
            dry_run=True,
            status_write_every=0,
        )
