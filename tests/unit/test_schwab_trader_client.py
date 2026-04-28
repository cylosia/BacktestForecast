from __future__ import annotations

import json
import time
from contextlib import suppress
from pathlib import Path
from urllib.parse import parse_qs
from uuid import uuid4

import httpx
import pytest

from backtestforecast.errors import ConfigurationError, ValidationError
from backtestforecast.integrations.schwab_trader import (
    SchwabAccountKind,
    SchwabFileTokenStore,
    SchwabOrderLeg,
    SchwabToken,
    SchwabTraderClient,
)


def _test_token_path() -> Path:
    path = Path("logs") / "test_schwab_tokens" / f"{uuid4().hex}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _unlink(path: Path) -> None:
    with suppress(FileNotFoundError):
        path.unlink()


def test_build_authorization_url_encodes_required_oauth_values() -> None:
    client = SchwabTraderClient(
        app_key="app-key",
        app_secret="secret",
        redirect_uri="https://localhost/callback",
        auth_base_url="https://auth.test",
        access_token="token",
    )

    try:
        url = client.build_authorization_url(state="state-1", scope="readonly")
    finally:
        client.close()

    assert url.startswith("https://auth.test/v1/oauth/authorize?")
    query = parse_qs(url.split("?", 1)[1])
    assert query["response_type"] == ["code"]
    assert query["client_id"] == ["app-key"]
    assert query["redirect_uri"] == ["https://localhost/callback"]
    assert query["state"] == ["state-1"]
    assert query["scope"] == ["readonly"]


def test_token_store_round_trips_token_without_losing_refresh_token() -> None:
    token_path = _test_token_path()
    store = SchwabFileTokenStore(token_path)
    token = SchwabToken(access_token="access", refresh_token="refresh", expires_at=1234.0, scope="scope")

    try:
        store.save(token)

        loaded = store.load()
        assert loaded == token
    finally:
        _unlink(token_path)


def test_exchange_authorization_code_posts_form_with_basic_auth_and_saves_token() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "access_token": "new-access",
                "refresh_token": "new-refresh",
                "expires_in": 1800,
                "token_type": "Bearer",
            },
        )

    token_path = _test_token_path()
    store = SchwabFileTokenStore(token_path)
    client = SchwabTraderClient(
        app_key="app-key",
        app_secret="app-secret",
        redirect_uri="https://localhost/callback",
        auth_base_url="https://auth.test",
        access_token="bootstrap",
        token_store=store,
        transport=httpx.MockTransport(handler),
    )

    try:
        try:
            token = client.exchange_authorization_code("code-123")
        finally:
            client.close()

        assert token.access_token == "new-access"
        assert token.refresh_token == "new-refresh"
        assert store.load() is not None
        assert str(requests[0].url) == "https://auth.test/v1/oauth/token"
        assert requests[0].headers["Authorization"].startswith("Basic ")
        assert requests[0].headers["Content-Type"] == "application/x-www-form-urlencoded"
        form = parse_qs(requests[0].content.decode())
        assert form["grant_type"] == ["authorization_code"]
        assert form["code"] == ["code-123"]
        assert form["redirect_uri"] == ["https://localhost/callback"]
    finally:
        _unlink(token_path)


def test_expired_file_token_refreshes_before_authenticated_request() -> None:
    token_path = _test_token_path()
    store = SchwabFileTokenStore(token_path)
    store.save(SchwabToken(access_token="expired", refresh_token="refresh", expires_at=time.time() - 1))
    auth_headers: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/oauth/token":
            return httpx.Response(200, json={"access_token": "fresh", "expires_in": 1800})
        auth_headers.append(request.headers["Authorization"])
        return httpx.Response(200, json={"AAPL": {"quote": {"lastPrice": 200.0}}})

    client = SchwabTraderClient(
        app_key="app-key",
        app_secret="app-secret",
        access_token="ignored",
        token_store=store,
        base_url="https://api.test",
        auth_base_url="https://api.test",
        transport=httpx.MockTransport(handler),
    )

    try:
        try:
            payload = client.get_quotes("AAPL")
        finally:
            client.close()

        assert payload == {"AAPL": {"quote": {"lastPrice": 200.0}}}
        assert auth_headers == ["Bearer fresh"]
        loaded = store.load()
        assert loaded is not None
        assert loaded.access_token == "fresh"
    finally:
        _unlink(token_path)


def test_get_account_numbers_returns_list_payload() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/trader/v1/accounts/accountNumbers"
        assert request.headers["Authorization"] == "Bearer access"
        return httpx.Response(200, json=[{"accountNumber": "123", "hashValue": "hash"}])

    client = SchwabTraderClient(access_token="access", base_url="https://api.test", transport=httpx.MockTransport(handler))
    try:
        payload = client.get_account_numbers()
    finally:
        client.close()

    assert payload == [{"accountNumber": "123", "hashValue": "hash"}]


def test_get_quotes_uses_marketdata_quotes_endpoint() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/marketdata/v1/quotes"
        assert request.url.params["symbols"] == "AAPL,MSFT"
        assert request.url.params["fields"] == "quote,reference"
        return httpx.Response(200, json={"AAPL": {}, "MSFT": {}})

    client = SchwabTraderClient(access_token="access", base_url="https://api.test", transport=httpx.MockTransport(handler))
    try:
        payload = client.get_quotes(["aapl", "msft"], fields="quote,reference")
    finally:
        client.close()

    assert set(payload) == {"AAPL", "MSFT"}


def test_get_option_chain_passes_schwab_query_parameters() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/marketdata/v1/chains"
        assert request.url.params["symbol"] == "AAPL"
        assert request.url.params["contractType"] == "CALL"
        assert request.url.params["fromDate"] == "2026-05-01"
        return httpx.Response(200, json={"symbol": "AAPL"})

    client = SchwabTraderClient(access_token="access", base_url="https://api.test", transport=httpx.MockTransport(handler))
    try:
        payload = client.get_option_chain("aapl", contractType="CALL", fromDate="2026-05-01")
    finally:
        client.close()

    assert payload == {"symbol": "AAPL"}


def test_order_builders_create_equity_option_and_spread_payloads() -> None:
    client = SchwabTraderClient(access_token="access")
    try:
        equity = client.build_equity_order(symbol="aapl", quantity=3, instruction="buy")
        option = client.build_option_order(
            option_symbol="AAPL 260501C00100000",
            quantity=1,
            instruction="BUY_TO_OPEN",
            order_type="LIMIT",
            limit_price=1.25,
        )
        spread = client.build_option_spread_order(
            legs=[
                SchwabOrderLeg(symbol="AAPL 260501C00100000", quantity=1, instruction="SELL_TO_OPEN"),
                {"symbol": "AAPL 260508C00100000", "quantity": 1, "instruction": "BUY_TO_OPEN"},
            ],
            price=0.75,
        )
    finally:
        client.close()

    assert equity["orderType"] == "MARKET"
    assert equity["orderLegCollection"][0]["instrument"]["assetType"] == "EQUITY"
    assert option["price"] == "1.25"
    assert option["orderLegCollection"][0]["instrument"]["assetType"] == "OPTION"
    assert spread["orderType"] == "NET_DEBIT"
    assert spread["price"] == "0.75"
    assert len(spread["orderLegCollection"]) == 2


def test_limit_order_requires_price() -> None:
    client = SchwabTraderClient(access_token="access")
    try:
        with pytest.raises(ValidationError):
            client.build_equity_order(symbol="AAPL", quantity=1, instruction="BUY", order_type="LIMIT")
    finally:
        client.close()


def test_place_order_defaults_to_preview_endpoint() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/trader/v1/accounts/hash/previewOrder"
        assert json.loads(request.content) == {"orderType": "MARKET"}
        return httpx.Response(200, json={"previewId": "preview"})

    client = SchwabTraderClient(
        access_token="access",
        base_url="https://api.test",
        transport=httpx.MockTransport(handler),
    )
    try:
        payload = client.place_order({"orderType": "MARKET"}, "hash")
    finally:
        client.close()

    assert payload == {"previewId": "preview"}


def test_live_place_order_requires_config_and_explicit_confirmation() -> None:
    client = SchwabTraderClient(access_token="access", trading_enabled=False)
    try:
        with pytest.raises(ConfigurationError):
            client.place_order({"orderType": "MARKET"}, "hash", preview_only=False, confirm_order_placement=True)
    finally:
        client.close()


def test_live_place_order_posts_when_enabled_and_confirmed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/trader/v1/accounts/hash/orders"
        return httpx.Response(201, headers={"Location": "https://api.test/order/123"})

    client = SchwabTraderClient(
        access_token="access",
        trading_enabled=True,
        base_url="https://api.test",
        transport=httpx.MockTransport(handler),
    )
    try:
        payload = client.place_order(
            {"orderType": "MARKET"},
            "hash",
            preview_only=False,
            confirm_order_placement=True,
        )
    finally:
        client.close()

    assert payload == {
        "status_code": 201,
        "location": "https://api.test/order/123",
        "order": None,
    }


def test_from_settings_accepts_paper_account_kind_without_requiring_credentials() -> None:
    client = SchwabTraderClient.from_settings(account_kind=SchwabAccountKind.PAPER)
    try:
        assert client.account_kind is SchwabAccountKind.PAPER
    finally:
        client.close()
