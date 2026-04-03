from __future__ import annotations

from datetime import date
from pathlib import Path
import uuid

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from backtestforecast.db.base import Base
from backtestforecast.market_data.contract_catalog import OptionContractCatalogStore
from backtestforecast.market_data.service import MassiveOptionGateway
from backtestforecast.market_data.types import OptionContractRecord
from backtestforecast.models import HistoricalOptionContractCatalogSnapshot


def _make_store(db_path: Path, *, snapshot_model=None):
    engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    kwargs = {}
    if snapshot_model is not None:
        kwargs["snapshot_model"] = snapshot_model
    return OptionContractCatalogStore(session_factory=factory, readonly_session_factory=factory, **kwargs)


def _temp_db_path() -> Path:
    tmpdir = Path.cwd() / ".pytest-tmp-contract-catalog"
    tmpdir.mkdir(exist_ok=True)
    return tmpdir / f"{uuid.uuid4().hex}.sqlite"


def test_contract_catalog_round_trip_exact_query():
    store = _make_store(_temp_db_path())
    contracts = [
        OptionContractRecord("O:AAPL250404C00200000", "call", date(2025, 4, 4), 200.0, 100.0),
        OptionContractRecord("O:AAPL250404C00205000", "call", date(2025, 4, 4), 205.0, 100.0),
    ]

    store.upsert_contracts(
        symbol="AAPL",
        as_of_date=date(2025, 4, 1),
        contract_type="call",
        expiration_date=date(2025, 4, 4),
        strike_price_gte=190.0,
        strike_price_lte=210.0,
        contracts=contracts,
    )

    cached = store.get_contracts(
        symbol="AAPL",
        as_of_date=date(2025, 4, 1),
        contract_type="call",
        expiration_date=date(2025, 4, 4),
        strike_price_gte=190.0,
        strike_price_lte=210.0,
    )

    assert cached == contracts


def test_contract_catalog_filters_banded_lookup_from_full_snapshot():
    store = _make_store(_temp_db_path())
    contracts = [
        OptionContractRecord("O:AAPL250404C00190000", "call", date(2025, 4, 4), 190.0, 100.0),
        OptionContractRecord("O:AAPL250404C00200000", "call", date(2025, 4, 4), 200.0, 100.0),
        OptionContractRecord("O:AAPL250404C00210000", "call", date(2025, 4, 4), 210.0, 100.0),
    ]

    store.upsert_contracts(
        symbol="AAPL",
        as_of_date=date(2025, 4, 1),
        contract_type="call",
        expiration_date=date(2025, 4, 4),
        strike_price_gte=None,
        strike_price_lte=None,
        contracts=contracts,
    )

    cached = store.get_contracts(
        symbol="AAPL",
        as_of_date=date(2025, 4, 1),
        contract_type="call",
        expiration_date=date(2025, 4, 4),
        strike_price_gte=195.0,
        strike_price_lte=205.0,
    )

    assert cached == [contracts[1]]


def test_gateway_uses_durable_catalog_before_provider():
    store = _make_store(_temp_db_path())
    cached_contracts = [
        OptionContractRecord("O:AAPL250404C00200000", "call", date(2025, 4, 4), 200.0, 100.0),
    ]
    store.upsert_contracts(
        symbol="AAPL",
        as_of_date=date(2025, 4, 1),
        contract_type="call",
        expiration_date=date(2025, 4, 4),
        strike_price_gte=170.0,
        strike_price_lte=230.0,
        contracts=cached_contracts,
    )

    class _Client:
        def list_option_contracts_for_expiration(self, **kwargs):
            raise AssertionError("provider should not be called when durable catalog has the snapshot")

    gateway = MassiveOptionGateway(_Client(), "AAPL", contract_catalog=store)
    result = gateway._list_contracts_for_exact_expiration(
        entry_date=date(2025, 4, 1),
        contract_type="call",
        expiration_date=date(2025, 4, 4),
        strike_price_gte=170.0,
        strike_price_lte=230.0,
    )

    assert result == cached_contracts


def test_historical_gateway_uses_durable_catalog_before_raw_store():
    from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway

    store = _make_store(
        _temp_db_path(),
        snapshot_model=HistoricalOptionContractCatalogSnapshot,
    )
    cached_contracts = [
        OptionContractRecord("O:AAPL250404C00200000", "call", date(2025, 4, 4), 200.0, 100.0),
    ]
    store.upsert_contracts(
        symbol="AAPL",
        as_of_date=date(2025, 4, 1),
        contract_type="call",
        expiration_date=date(2025, 4, 4),
        strike_price_gte=170.0,
        strike_price_lte=230.0,
        contracts=cached_contracts,
    )

    raw_store = type(
        "_RawStore",
        (),
        {
            "list_option_contracts_for_expiration": staticmethod(
                lambda **kwargs: (_ for _ in ()).throw(AssertionError("raw store should not be called"))
            ),
        },
    )()

    gateway = HistoricalOptionGateway(raw_store, "AAPL", contract_catalog=store)
    result = gateway.list_contracts_for_expiration(
        entry_date=date(2025, 4, 1),
        contract_type="call",
        expiration_date=date(2025, 4, 4),
        strike_price_gte=170.0,
        strike_price_lte=230.0,
    )

    assert result == cached_contracts


def test_gateway_filters_from_warm_full_expiration_cache_before_provider():
    contracts = [
        OptionContractRecord("O:AAPL250404C00190000", "call", date(2025, 4, 4), 190.0, 100.0),
        OptionContractRecord("O:AAPL250404C00200000", "call", date(2025, 4, 4), 200.0, 100.0),
        OptionContractRecord("O:AAPL250404C00210000", "call", date(2025, 4, 4), 210.0, 100.0),
    ]

    class _Client:
        def __init__(self) -> None:
            self.calls = 0

        def list_option_contracts_for_expiration(self, **kwargs):
            self.calls += 1
            return contracts

    client = _Client()
    gateway = MassiveOptionGateway(client, "AAPL")

    warmed = gateway._list_contracts_for_exact_expiration(
        entry_date=date(2025, 4, 1),
        contract_type="call",
        expiration_date=date(2025, 4, 4),
    )
    filtered = gateway._list_contracts_for_exact_expiration(
        entry_date=date(2025, 4, 1),
        contract_type="call",
        expiration_date=date(2025, 4, 4),
        strike_price_gte=195.0,
        strike_price_lte=205.0,
    )

    assert warmed == contracts
    assert filtered == [contracts[1]]
    assert client.calls == 1


def test_historical_gateway_filters_from_warm_full_expiration_cache_before_store():
    from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway

    contracts = [
        OptionContractRecord("O:AAPL250404C00190000", "call", date(2025, 4, 4), 190.0, 100.0),
        OptionContractRecord("O:AAPL250404C00200000", "call", date(2025, 4, 4), 200.0, 100.0),
        OptionContractRecord("O:AAPL250404C00210000", "call", date(2025, 4, 4), 210.0, 100.0),
    ]

    class _RawStore:
        def __init__(self) -> None:
            self.calls = 0

        def list_option_contracts_for_expiration(self, **kwargs):
            self.calls += 1
            return contracts

    raw_store = _RawStore()
    gateway = HistoricalOptionGateway(raw_store, "AAPL")

    warmed = gateway.list_contracts_for_expiration(
        entry_date=date(2025, 4, 1),
        contract_type="call",
        expiration_date=date(2025, 4, 4),
    )
    filtered = gateway.list_contracts_for_expiration(
        entry_date=date(2025, 4, 1),
        contract_type="call",
        expiration_date=date(2025, 4, 4),
        strike_price_gte=195.0,
        strike_price_lte=205.0,
    )

    assert warmed == contracts
    assert filtered == [contracts[1]]
    assert raw_store.calls == 1
