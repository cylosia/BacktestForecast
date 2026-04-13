from __future__ import annotations

from contextlib import ExitStack
import importlib.util
import json
import os
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from backtestforecast.db.base import Base
from backtestforecast.errors import ExternalServiceError
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore
from backtestforecast.market_data.types import EarningsEventRecord
from backtestforecast.models import HistoricalEarningsEvent, HistoricalUnderlyingDayBar


def _load_script_module():
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "backfill_earnings_events.py"
    sys.path.insert(0, str(script_path.parent))
    try:
        spec = importlib.util.spec_from_file_location("backfill_earnings_events", script_path)
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
    return Path(f"earnings-backfill-status-{uuid4().hex}.json")


class _FakeEarningsClient:
    def __init__(
        self,
        records_by_symbol: dict[str, list[EarningsEventRecord]] | None = None,
        *,
        optionable_symbols: list[str] | None = None,
        failing_chunks: set[tuple[str, date, date]] | None = None,
    ) -> None:
        self._records_by_symbol = {key.upper(): list(value) for key, value in (records_by_symbol or {}).items()}
        self._optionable_symbols = [item.upper() for item in (optionable_symbols or [])]
        self._failing_chunks = failing_chunks or set()
        self.calls: list[tuple[str, date, date]] = []
        self.universe_calls: list[tuple[date, bool]] = []

    def list_earnings_event_records(self, symbol: str, start_date: date, end_date: date) -> list[EarningsEventRecord]:
        normalized = symbol.upper()
        self.calls.append((normalized, start_date, end_date))
        if (normalized, start_date, end_date) in self._failing_chunks:
            raise ExternalServiceError(f"upstream failed for {normalized} {start_date.isoformat()}..{end_date.isoformat()}")
        return [
            record
            for record in self._records_by_symbol.get(normalized, [])
            if start_date <= record.event_date <= end_date
        ]

    def list_optionable_underlyings(self, *, as_of_date: date, include_expired: bool = True) -> list[str]:
        self.universe_calls.append((as_of_date, include_expired))
        return sorted(self._optionable_symbols)

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


def _run_main(
    module,
    *,
    store: HistoricalMarketDataStore | None = None,
    client: _FakeEarningsClient | None = None,
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
        "backfill_earnings_events.py",
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
        stack.enter_context(patch_object(sys, "argv", argv))
        if env is not None:
            stack.enter_context(patch_dict(os.environ, env))
        stack.enter_context(
            patch_object(
                module,
                "get_settings",
                lambda: SimpleNamespace(
                    historical_data_start_year=2014,
                    historical_data_sync_symbols=config_symbols or [],
                ),
            )
        )
        if store is not None:
            stack.enter_context(patch_object(module, "HistoricalMarketDataStore", lambda *args, **kwargs: store))
        if client is None:
            client = _FakeEarningsClient()
        stack.enter_context(patch_object(module, "MassiveClient", lambda *args, **kwargs: client))
        if flatfiles is None:
            flatfiles = _FakeFlatFilesClient()
        stack.enter_context(patch_object(module.MassiveFlatFilesClient, "from_settings", lambda *args, **kwargs: flatfiles))
        if discovered_symbols is not None:
            stack.enter_context(patch_object(module, "_discover_symbols", lambda **kwargs: discovered_symbols))
        return module.main()


class patch_object:
    def __init__(self, obj, attr, value):
        self.obj = obj
        self.attr = attr
        self.value = value
        self.original = None

    def __enter__(self):
        self.original = getattr(self.obj, self.attr)
        setattr(self.obj, self.attr, self.value)
        return self.value

    def __exit__(self, exc_type, exc, tb):
        setattr(self.obj, self.attr, self.original)
        return False


class patch_dict:
    def __init__(self, mapping, updates):
        self.mapping = mapping
        self.updates = updates
        self.original: dict[str, object | None] = {}

    def __enter__(self):
        for key, value in self.updates.items():
            self.original[key] = self.mapping.get(key)
            self.mapping[key] = value
        return self.mapping

    def __exit__(self, exc_type, exc, tb):
        for key, original in self.original.items():
            if original is None and key in self.mapping:
                self.mapping.pop(key, None)
            else:
                self.mapping[key] = original  # type: ignore[index]
        return False


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


def test_sync_chunk_upserts_symbol_range_rows() -> None:
    module = _load_script_module()
    store = _store()
    client = _FakeEarningsClient(
        {
            "AAPL": [
                EarningsEventRecord(
                    event_date=date(2025, 4, 1),
                    event_type="earnings_announcement_date",
                    provider_event_id="AAPL-earn-2025-04-01",
                ),
                EarningsEventRecord(
                    event_date=date(2025, 4, 1),
                    event_type="earnings_conference_call",
                    provider_event_id="AAPL-call-2025-04-01",
                ),
            ]
        }
    )
    chunk = module.EarningsBackfillChunk("AAPL", date(2025, 4, 1), date(2025, 4, 30))

    result = module._sync_chunk(store, client, chunk, dry_run=False)

    assert result.earnings_count == 2
    assert result.stored_count == 2
    assert result.error is None
    assert store.list_earnings_event_dates("AAPL", date(2025, 4, 1), date(2025, 4, 30)) == {date(2025, 4, 1)}
    with store._session(readonly=True) as session:
        rows = (
            session.query(HistoricalEarningsEvent)
            .filter_by(symbol="AAPL", event_date=date(2025, 4, 1))
            .order_by(HistoricalEarningsEvent.event_type)
            .all()
        )
    assert [row.provider_event_id for row in rows] == [
        "AAPL-earn-2025-04-01",
        "AAPL-call-2025-04-01",
    ]


def test_main_auto_discovers_symbols_and_writes_completed_status() -> None:
    module = _load_script_module()
    store = _store()
    status_path = _status_file_path()
    client = _FakeEarningsClient(
        {
            "AAPL": [
                EarningsEventRecord(
                    event_date=date(2025, 4, 1),
                    event_type="earnings_announcement_date",
                    provider_event_id="AAPL-earn-2025-04-01",
                )
            ],
            "MSFT": [
                EarningsEventRecord(
                    event_date=date(2025, 4, 2),
                    event_type="earnings_announcement_date",
                    provider_event_id="MSFT-earn-2025-04-02",
                )
            ],
        }
    )
    store.upsert_underlying_day_bars(
        [
            HistoricalUnderlyingDayBar(
                symbol="AAPL",
                trade_date=date(2025, 4, 1),
                open_price=Decimal("100"),
                high_price=Decimal("101"),
                low_price=Decimal("99"),
                close_price=Decimal("100"),
                volume=Decimal("1000"),
                source_file_date=date(2025, 4, 1),
            ),
            HistoricalUnderlyingDayBar(
                symbol="MSFT",
                trade_date=date(2025, 4, 1),
                open_price=Decimal("200"),
                high_price=Decimal("201"),
                low_price=Decimal("199"),
                close_price=Decimal("200"),
                volume=Decimal("1000"),
                source_file_date=date(2025, 4, 1),
            ),
        ]
    )

    try:
        rc = _run_main(
            module,
            store=store,
            client=client,
            start_date="2025-04-01",
            end_date="2025-04-30",
            status_path=status_path,
            status_write_every=1,
            config_symbols=[],
        )
        assert rc == 0
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        assert payload["status"] == "completed"
        assert payload["resolved_symbols"] == ["AAPL", "MSFT"]
        assert payload["completed_earnings_rows"] == 2
        assert payload["completed_stored_rows"] == 2
        assert store.list_earnings_event_dates("AAPL", date(2025, 4, 1), date(2025, 4, 30)) == {date(2025, 4, 1)}
        assert store.list_earnings_event_dates("MSFT", date(2025, 4, 1), date(2025, 4, 30)) == {date(2025, 4, 2)}
    finally:
        status_path.unlink(missing_ok=True)


def test_resolve_symbols_falls_back_when_store_lookup_times_out() -> None:
    module = _load_script_module()
    client = _FakeEarningsClient()

    class _TimingOutStore:
        def list_imported_symbols_for_window(self, start_date: date, end_date: date):
            raise OperationalError("select 1", {}, Exception("statement timeout"))

    with patch_object(module, "_discover_symbols", lambda **kwargs: ["AAPL", "MSFT"]):
        resolved = module._resolve_symbols(
            start_date=date(2025, 4, 1),
            end_date=date(2025, 4, 30),
            explicit_symbols=set(),
            universe_source="auto",
            client=client,
            store=_TimingOutStore(),
            optionable_as_of_date=None,
        )

    assert resolved == ["AAPL", "MSFT"]


def test_load_symbols_file_supports_newlines_and_csv() -> None:
    module = _load_script_module()
    symbols_path = Path(f"earnings-symbols-{uuid4().hex}.txt")
    try:
        symbols_path.write_text("AAPL, msft\n\nspy\nAAPL\n", encoding="utf-8")
        assert module._load_symbols_file(str(symbols_path)) == {"AAPL", "MSFT", "SPY"}
    finally:
        symbols_path.unlink(missing_ok=True)
