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


def _make_store(db_path: Path):
    engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    return OptionContractCatalogStore(session_factory=factory, readonly_session_factory=factory)


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
