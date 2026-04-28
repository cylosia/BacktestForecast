from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlencode

import httpx
import structlog

from backtestforecast.config import get_settings
from backtestforecast.errors import ConfigurationError, ExternalServiceError, ValidationError

logger = structlog.get_logger("schwab_trader")

DEFAULT_SCHWAB_BASE_URL = "https://api.schwabapi.com"
TOKEN_EXPIRY_SKEW_SECONDS = 60.0


class SchwabAccountKind(StrEnum):
    LIVE = "live"
    PAPER = "paper"


@dataclass(frozen=True, slots=True)
class SchwabToken:
    access_token: str
    refresh_token: str | None = None
    token_type: str = "Bearer"  # noqa: S105 - OAuth token type, not a secret.
    expires_at: float | None = None
    scope: str | None = None
    id_token: str | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_oauth_payload(cls, payload: dict[str, Any], *, issued_at: float | None = None) -> SchwabToken:
        access_token = str(payload.get("access_token") or "").strip()
        if not access_token:
            raise ExternalServiceError("Schwab token response did not include an access_token.")

        now = time.time() if issued_at is None else issued_at
        expires_at: float | None = None
        expires_in = payload.get("expires_in")
        if expires_in is not None:
            try:
                expires_at = now + float(expires_in)
            except (TypeError, ValueError):
                logger.warning("schwab_token_invalid_expires_in", expires_in=expires_in)

        return cls(
            access_token=access_token,
            refresh_token=_optional_str(payload.get("refresh_token")),
            token_type=str(payload.get("token_type") or "Bearer"),
            expires_at=expires_at,
            scope=_optional_str(payload.get("scope")),
            id_token=_optional_str(payload.get("id_token")),
            raw_payload=dict(payload),
        )

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> SchwabToken:
        access_token = str(payload.get("access_token") or "").strip()
        if not access_token:
            raise ConfigurationError("Schwab token file is missing access_token.")
        expires_at = payload.get("expires_at")
        return cls(
            access_token=access_token,
            refresh_token=_optional_str(payload.get("refresh_token")),
            token_type=str(payload.get("token_type") or "Bearer"),
            expires_at=float(expires_at) if expires_at is not None else None,
            scope=_optional_str(payload.get("scope")),
            id_token=_optional_str(payload.get("id_token")),
            raw_payload=dict(payload.get("raw_payload") or {}),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "token_type": self.token_type,
            "expires_at": self.expires_at,
            "scope": self.scope,
            "id_token": self.id_token,
            "raw_payload": self.raw_payload,
        }

    def is_expired(self, *, skew_seconds: float = TOKEN_EXPIRY_SKEW_SECONDS) -> bool:
        if self.expires_at is None:
            return False
        return time.time() >= (self.expires_at - skew_seconds)


@dataclass(frozen=True, slots=True)
class SchwabOrderLeg:
    symbol: str
    quantity: float
    instruction: str
    asset_type: str = "OPTION"

    def to_payload(self) -> dict[str, Any]:
        quantity = _positive_quantity(self.quantity)
        return {
            "instruction": self.instruction.upper(),
            "quantity": quantity,
            "instrument": {
                "symbol": self.symbol.strip().upper(),
                "assetType": self.asset_type.upper(),
            },
        }


class SchwabFileTokenStore:
    """Small file-backed token store.

    Store live and paper-money tokens in separate files. The public Trader API
    uses the same host; the separation is operational, not a different endpoint.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser()

    def load(self) -> SchwabToken | None:
        if not self.path.exists():
            return None
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ConfigurationError(f"Schwab token file is not valid JSON: {self.path}") from exc
        if not isinstance(payload, dict):
            raise ConfigurationError(f"Schwab token file must contain a JSON object: {self.path}")
        return SchwabToken.from_dict(payload)

    def save(self, token: SchwabToken) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_name(f"{self.path.name}.tmp")
        temp_path.write_text(json.dumps(token.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
        temp_path.replace(self.path)


class SchwabTraderClient:
    """Sync client for Schwab Trader API calls used by thinkorswim-enabled accounts."""

    def __init__(
        self,
        *,
        app_key: str | None = None,
        app_secret: str | None = None,
        redirect_uri: str | None = None,
        access_token: str | None = None,
        token: SchwabToken | None = None,
        token_store: SchwabFileTokenStore | None = None,
        account_kind: SchwabAccountKind | str = SchwabAccountKind.LIVE,
        base_url: str | None = None,
        auth_base_url: str | None = None,
        timeout_seconds: float | None = None,
        trading_enabled: bool | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        settings = get_settings()
        self.app_key = app_key or settings.schwab_app_key
        self.app_secret = app_secret or settings.schwab_app_secret
        self.redirect_uri = redirect_uri or settings.schwab_redirect_uri
        self.account_kind = SchwabAccountKind(account_kind)
        self.base_url = (base_url or settings.schwab_base_url or DEFAULT_SCHWAB_BASE_URL).rstrip("/")
        self.auth_base_url = (auth_base_url or settings.schwab_auth_base_url or self.base_url).rstrip("/")
        self.timeout_seconds = timeout_seconds if timeout_seconds is not None else settings.schwab_timeout_seconds
        self.trading_enabled = settings.schwab_trading_enabled if trading_enabled is None else trading_enabled
        self._access_token = access_token
        self._token = token
        self._token_store = token_store
        if self._token is None and self._token_store is not None:
            self._token = self._token_store.load()
        self._http = httpx.Client(timeout=self.timeout_seconds, transport=transport, trust_env=False)

    @classmethod
    def from_settings(cls, *, account_kind: SchwabAccountKind | str = SchwabAccountKind.LIVE) -> SchwabTraderClient:
        settings = get_settings()
        normalized_kind = SchwabAccountKind(account_kind)
        token_path = (
            settings.schwab_token_path_paper
            if normalized_kind is SchwabAccountKind.PAPER
            else settings.schwab_token_path_live
        )
        token_store = SchwabFileTokenStore(token_path) if token_path else None
        return cls(account_kind=normalized_kind, token_store=token_store)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> SchwabTraderClient:
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.close()

    @property
    def default_account_hash(self) -> str | None:
        settings = get_settings()
        if self.account_kind is SchwabAccountKind.PAPER:
            return cast(str | None, settings.schwab_account_hash_paper)
        return cast(str | None, settings.schwab_account_hash_live)

    def build_authorization_url(self, *, state: str | None = None, scope: str | None = None) -> str:
        if not self.app_key:
            raise ConfigurationError("SCHWAB_APP_KEY is required to build the Schwab OAuth URL.")
        if not self.redirect_uri:
            raise ConfigurationError("SCHWAB_REDIRECT_URI is required to build the Schwab OAuth URL.")
        params = {
            "response_type": "code",
            "client_id": self.app_key,
            "redirect_uri": self.redirect_uri,
        }
        if state:
            params["state"] = state
        if scope:
            params["scope"] = scope
        return f"{self.auth_base_url}/v1/oauth/authorize?{urlencode(params)}"

    def exchange_authorization_code(self, code: str) -> SchwabToken:
        if not self.redirect_uri:
            raise ConfigurationError("SCHWAB_REDIRECT_URI is required to exchange a Schwab authorization code.")
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_uri,
        }
        payload = self._request_token(data)
        token = SchwabToken.from_oauth_payload(payload)
        self._token = token
        if self._token_store is not None:
            self._token_store.save(token)
        return token

    def refresh_access_token(self, refresh_token: str | None = None) -> SchwabToken:
        refresh_token = refresh_token or (self._token.refresh_token if self._token is not None else None)
        if not refresh_token:
            raise ConfigurationError("A Schwab refresh_token is required to refresh the access token.")
        payload = self._request_token({"grant_type": "refresh_token", "refresh_token": refresh_token})
        if "refresh_token" not in payload:
            payload["refresh_token"] = refresh_token
        token = SchwabToken.from_oauth_payload(payload)
        self._token = token
        if self._token_store is not None:
            self._token_store.save(token)
        return token

    def get_account_numbers(self) -> list[dict[str, Any]]:
        payload = self._request("GET", "/trader/v1/accounts/accountNumbers")
        if not isinstance(payload, list):
            raise ExternalServiceError("Schwab accountNumbers response was not a list.")
        return [item for item in payload if isinstance(item, dict)]

    def get_accounts(self, *, fields: str | None = None) -> Any:
        params = {"fields": fields} if fields else None
        return self._request("GET", "/trader/v1/accounts", params=params)

    def get_account(self, account_hash: str | None = None, *, fields: str | None = None) -> Any:
        account_hash = self._resolve_account_hash(account_hash)
        params = {"fields": fields} if fields else None
        return self._request("GET", f"/trader/v1/accounts/{account_hash}", params=params)

    def get_orders(
        self,
        account_hash: str | None = None,
        *,
        start_time: str | None = None,
        end_time: str | None = None,
        status: str | None = None,
        max_results: int | None = None,
    ) -> Any:
        account_hash = self._resolve_account_hash(account_hash)
        params: dict[str, Any] = {}
        if start_time:
            params["fromEnteredTime"] = start_time
        if end_time:
            params["toEnteredTime"] = end_time
        if status:
            params["status"] = status
        if max_results is not None:
            params["maxResults"] = max_results
        return self._request("GET", f"/trader/v1/accounts/{account_hash}/orders", params=params or None)

    def get_order(self, order_id: str | int, account_hash: str | None = None) -> Any:
        account_hash = self._resolve_account_hash(account_hash)
        return self._request("GET", f"/trader/v1/accounts/{account_hash}/orders/{order_id}")

    def cancel_order(self, order_id: str | int, account_hash: str | None = None) -> None:
        account_hash = self._resolve_account_hash(account_hash)
        self._request(
            "DELETE",
            f"/trader/v1/accounts/{account_hash}/orders/{order_id}",
            expected_status={200, 202, 204},
        )

    def get_quotes(
        self,
        symbols: list[str] | tuple[str, ...] | set[str] | str,
        *,
        fields: str | None = None,
        indicative: bool | None = None,
    ) -> Any:
        normalized_symbols = _normalize_symbols(symbols)
        params: dict[str, Any] = {"symbols": ",".join(normalized_symbols)}
        if fields:
            params["fields"] = fields
        if indicative is not None:
            params["indicative"] = str(indicative).lower()
        return self._request("GET", "/marketdata/v1/quotes", params=params)

    def get_option_chain(self, symbol: str, **params: Any) -> Any:
        request_params = {"symbol": symbol.strip().upper()}
        request_params.update({key: value for key, value in params.items() if value is not None})
        return self._request("GET", "/marketdata/v1/chains", params=request_params)

    def preview_order(self, order_spec: dict[str, Any], account_hash: str | None = None) -> Any:
        account_hash = self._resolve_account_hash(account_hash)
        return self._request(
            "POST",
            f"/trader/v1/accounts/{account_hash}/previewOrder",
            json_body=order_spec,
            expected_status={200, 201},
        )

    def place_order(
        self,
        order_spec: dict[str, Any],
        account_hash: str | None = None,
        *,
        preview_only: bool = True,
        confirm_order_placement: bool = False,
    ) -> Any:
        if preview_only:
            return self.preview_order(order_spec, account_hash)
        if not self.trading_enabled:
            raise ConfigurationError("Set SCHWAB_TRADING_ENABLED=true before live Schwab order placement is allowed.")
        if not confirm_order_placement:
            raise ValidationError("Set confirm_order_placement=True to place a live Schwab order.")

        account_hash = self._resolve_account_hash(account_hash)
        response = self._request(
            "POST",
            f"/trader/v1/accounts/{account_hash}/orders",
            json_body=order_spec,
            expected_status={200, 201},
            return_response=True,
        )
        if not isinstance(response, httpx.Response):
            raise ExternalServiceError("Schwab order placement did not return an HTTP response.")
        return {
            "status_code": response.status_code,
            "location": response.headers.get("Location"),
            "order": _safe_json(response),
        }

    def build_equity_order(
        self,
        *,
        symbol: str,
        quantity: float,
        instruction: str,
        order_type: str = "MARKET",
        limit_price: float | None = None,
        duration: str = "DAY",
        session: str = "NORMAL",
    ) -> dict[str, Any]:
        return _build_single_leg_order(
            symbol=symbol,
            quantity=quantity,
            instruction=instruction,
            asset_type="EQUITY",
            order_type=order_type,
            limit_price=limit_price,
            duration=duration,
            session=session,
        )

    def build_option_order(
        self,
        *,
        option_symbol: str,
        quantity: float,
        instruction: str,
        order_type: str = "LIMIT",
        limit_price: float | None = None,
        duration: str = "DAY",
        session: str = "NORMAL",
    ) -> dict[str, Any]:
        return _build_single_leg_order(
            symbol=option_symbol,
            quantity=quantity,
            instruction=instruction,
            asset_type="OPTION",
            order_type=order_type,
            limit_price=limit_price,
            duration=duration,
            session=session,
        )

    def build_option_spread_order(
        self,
        *,
        legs: list[SchwabOrderLeg | dict[str, Any]],
        order_type: str = "NET_DEBIT",
        price: float | None = None,
        duration: str = "DAY",
        session: str = "NORMAL",
        complex_order_strategy_type: str = "CUSTOM",
    ) -> dict[str, Any]:
        if not legs:
            raise ValidationError("Option spread order requires at least one leg.")
        normalized_legs = [_normalize_order_leg(leg) for leg in legs]
        order: dict[str, Any] = {
            "orderType": order_type.upper(),
            "session": session.upper(),
            "duration": duration.upper(),
            "orderStrategyType": "SINGLE",
            "complexOrderStrategyType": complex_order_strategy_type.upper(),
            "orderLegCollection": normalized_legs,
        }
        if price is not None:
            order["price"] = _format_price(price)
        return order

    def _request_token(self, data: dict[str, str]) -> dict[str, Any]:
        self._require_oauth_credentials()
        payload = self._request(
            "POST",
            "/v1/oauth/token",
            data=data,
            headers={
                "Authorization": _basic_auth_header(self.app_key or "", self.app_secret or ""),
                "Content-Type": "application/x-www-form-urlencoded",
            },
            token_required=False,
            expected_status={200},
            base_url=self.auth_base_url,
        )
        if not isinstance(payload, dict):
            raise ExternalServiceError("Schwab token response was not a JSON object.")
        return payload

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        token_required: bool = True,
        expected_status: set[int] | None = None,
        base_url: str | None = None,
        return_response: bool = False,
    ) -> Any:
        expected = expected_status or {200}
        url = _build_url(base_url or self.base_url, path)
        request_headers = {"Accept": "application/json"}
        if token_required:
            token = self._get_access_token()
            request_headers["Authorization"] = f"Bearer {token}"
        if headers:
            request_headers.update(headers)

        try:
            response = self._http.request(
                method,
                url,
                params=params,
                json=json_body,
                data=data,
                headers=request_headers,
            )
        except httpx.HTTPError as exc:
            raise ExternalServiceError(f"Schwab API request failed: {exc}") from exc

        if response.status_code not in expected:
            self._raise_for_status(response)
        if return_response:
            return response
        if response.status_code == 204 or not response.content:
            return None
        return _safe_json(response)

    def _get_access_token(self) -> str:
        if self._token is not None:
            if self._token.is_expired():
                self.refresh_access_token()
            return self._token.access_token
        if self._access_token:
            return self._access_token
        raise ConfigurationError("Schwab access token is required. Configure a token file or pass access_token.")

    def _resolve_account_hash(self, account_hash: str | None) -> str:
        resolved = (account_hash or self.default_account_hash or "").strip()
        if not resolved:
            raise ConfigurationError("A Schwab account hash is required. Use accountNumbers to map account IDs.")
        return resolved

    def _require_oauth_credentials(self) -> None:
        if not self.app_key:
            raise ConfigurationError("SCHWAB_APP_KEY is required for Schwab OAuth.")
        if not self.app_secret:
            raise ConfigurationError("SCHWAB_APP_SECRET is required for Schwab OAuth.")

    def _raise_for_status(self, response: httpx.Response) -> None:
        detail = response.text[:500]
        if response.status_code in {401, 403}:
            raise ExternalServiceError("Schwab rejected the request. Verify token, account access, and entitlements.")
        if response.status_code == 404:
            raise ExternalServiceError("Schwab endpoint or resource was not found.")
        if response.status_code == 429:
            raise ExternalServiceError("Schwab rate limit reached. Retry later.")
        raise ExternalServiceError(f"Schwab API returned HTTP {response.status_code}: {detail}")


def _build_single_leg_order(
    *,
    symbol: str,
    quantity: float,
    instruction: str,
    asset_type: str,
    order_type: str,
    limit_price: float | None,
    duration: str,
    session: str,
) -> dict[str, Any]:
    normalized_type = order_type.upper()
    order: dict[str, Any] = {
        "orderType": normalized_type,
        "session": session.upper(),
        "duration": duration.upper(),
        "orderStrategyType": "SINGLE",
        "orderLegCollection": [
            SchwabOrderLeg(
                symbol=symbol,
                quantity=quantity,
                instruction=instruction,
                asset_type=asset_type,
            ).to_payload()
        ],
    }
    if limit_price is not None:
        order["price"] = _format_price(limit_price)
    elif normalized_type in {"LIMIT", "NET_DEBIT", "NET_CREDIT", "STOP_LIMIT"}:
        raise ValidationError(f"{normalized_type} orders require a limit_price.")
    return order


def _normalize_order_leg(leg: SchwabOrderLeg | dict[str, Any]) -> dict[str, Any]:
    if isinstance(leg, SchwabOrderLeg):
        return leg.to_payload()
    try:
        return SchwabOrderLeg(
            symbol=str(leg["symbol"]),
            quantity=float(leg["quantity"]),
            instruction=str(leg["instruction"]),
            asset_type=str(leg.get("asset_type") or leg.get("assetType") or "OPTION"),
        ).to_payload()
    except KeyError as exc:
        raise ValidationError(f"Order leg is missing required field: {exc.args[0]}") from exc


def _normalize_symbols(symbols: list[str] | tuple[str, ...] | set[str] | str) -> list[str]:
    raw_symbols = symbols.split(",") if isinstance(symbols, str) else list(symbols)
    normalized = [symbol.strip().upper() for symbol in raw_symbols if symbol and symbol.strip()]
    if not normalized:
        raise ValidationError("At least one symbol is required.")
    return normalized


def _positive_quantity(value: float) -> float:
    quantity = float(value)
    if quantity <= 0:
        raise ValidationError(f"Quantity must be positive, got {quantity}.")
    return int(quantity) if quantity.is_integer() else quantity


def _format_price(value: float) -> str:
    price = float(value)
    if price <= 0:
        raise ValidationError(f"Order price must be positive, got {price}.")
    return f"{price:.2f}"


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _basic_auth_header(app_key: str, app_secret: str) -> str:
    token = base64.b64encode(f"{app_key}:{app_secret}".encode()).decode("ascii")
    return f"Basic {token}"


def _build_url(base_url: str, path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"


def _safe_json(response: httpx.Response) -> Any:
    if not response.content:
        return None
    try:
        return response.json()
    except ValueError:
        return response.text


__all__ = [
    "SchwabAccountKind",
    "SchwabFileTokenStore",
    "SchwabOrderLeg",
    "SchwabToken",
    "SchwabTraderClient",
]
