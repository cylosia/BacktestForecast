from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _bootstrap import bootstrap_repo

bootstrap_repo(load_api_env=True)

from backtestforecast.integrations.schwab_trader import SchwabAccountKind, SchwabTraderClient


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Schwab Trader API / thinkorswim brokerage utility. Order placement is safe-by-default.",
    )
    parser.add_argument("--kind", choices=[kind.value for kind in SchwabAccountKind], default=SchwabAccountKind.LIVE.value)
    subparsers = parser.add_subparsers(dest="command", required=True)

    auth_url = subparsers.add_parser("auth-url", help="Print the Schwab OAuth authorization URL.")
    auth_url.add_argument("--state")
    auth_url.add_argument("--scope")
    auth_url.set_defaults(func=_auth_url)

    exchange_code = subparsers.add_parser("exchange-code", help="Exchange an OAuth callback code and save the token.")
    exchange_code.add_argument("--code", required=True)
    exchange_code.set_defaults(func=_exchange_code)

    refresh_token = subparsers.add_parser("refresh-token", help="Refresh and save the configured token.")
    refresh_token.set_defaults(func=_refresh_token)

    account_numbers = subparsers.add_parser("account-numbers", help="List account numbers and Schwab account hashes.")
    account_numbers.set_defaults(func=_account_numbers)

    accounts = subparsers.add_parser("accounts", help="Fetch account summaries.")
    accounts.add_argument("--fields", help="Optional Schwab fields, e.g. positions.")
    accounts.set_defaults(func=_accounts)

    account = subparsers.add_parser("account", help="Fetch one account by hash.")
    account.add_argument("--account-hash")
    account.add_argument("--fields", help="Optional Schwab fields, e.g. positions.")
    account.set_defaults(func=_account)

    orders = subparsers.add_parser("orders", help="Fetch account orders.")
    orders.add_argument("--account-hash")
    orders.add_argument("--start-time", help="Schwab fromEnteredTime value.")
    orders.add_argument("--end-time", help="Schwab toEnteredTime value.")
    orders.add_argument("--status")
    orders.add_argument("--max-results", type=int)
    orders.set_defaults(func=_orders)

    quotes = subparsers.add_parser("quotes", help="Fetch stock/ETF/option quotes.")
    quotes.add_argument("--symbols", required=True, help="Comma-separated symbols.")
    quotes.add_argument("--fields", help="Optional Schwab quote fields, e.g. quote,reference.")
    quotes.add_argument("--indicative", action="store_true")
    quotes.set_defaults(func=_quotes)

    option_chain = subparsers.add_parser("option-chain", help="Fetch an option chain.")
    option_chain.add_argument("--symbol", required=True)
    option_chain.add_argument("--contract-type", dest="contractType")
    option_chain.add_argument("--strike-count", dest="strikeCount", type=int)
    option_chain.add_argument("--include-underlying-quote", dest="includeUnderlyingQuote", action="store_true")
    option_chain.add_argument("--strategy")
    option_chain.add_argument("--interval")
    option_chain.add_argument("--strike", type=float)
    option_chain.add_argument("--range", dest="range")
    option_chain.add_argument("--from-date", dest="fromDate")
    option_chain.add_argument("--to-date", dest="toDate")
    option_chain.add_argument("--days-to-expiration", dest="daysToExpiration")
    option_chain.add_argument("--exp-month", dest="expMonth")
    option_chain.add_argument("--option-type", dest="optionType")
    option_chain.add_argument("--entitlement")
    option_chain.set_defaults(func=_option_chain)

    _add_equity_order_parser(subparsers, "preview-equity-order", _preview_equity_order)
    _add_equity_order_parser(
        subparsers,
        "place-equity-order",
        _place_equity_order,
        require_confirmation=True,
    )
    _add_option_order_parser(subparsers, "preview-option-order", _preview_option_order)
    _add_option_order_parser(
        subparsers,
        "place-option-order",
        _place_option_order,
        require_confirmation=True,
    )
    _add_spread_order_parser(subparsers, "preview-option-spread", _preview_option_spread)
    _add_spread_order_parser(
        subparsers,
        "place-option-spread",
        _place_option_spread,
        require_confirmation=True,
    )

    args = parser.parse_args()
    result = args.func(args)
    _print_json(result)
    return 0


def _client(args: argparse.Namespace) -> SchwabTraderClient:
    return SchwabTraderClient.from_settings(account_kind=args.kind)


def _auth_url(args: argparse.Namespace) -> dict[str, str]:
    with _client(args) as client:
        return {"authorization_url": client.build_authorization_url(state=args.state, scope=args.scope)}


def _exchange_code(args: argparse.Namespace) -> dict[str, Any]:
    with _client(args) as client:
        token = client.exchange_authorization_code(args.code)
    return _token_summary(token.expires_at, bool(token.refresh_token))


def _refresh_token(args: argparse.Namespace) -> dict[str, Any]:
    with _client(args) as client:
        token = client.refresh_access_token()
    return _token_summary(token.expires_at, bool(token.refresh_token))


def _account_numbers(args: argparse.Namespace) -> Any:
    with _client(args) as client:
        return client.get_account_numbers()


def _accounts(args: argparse.Namespace) -> Any:
    with _client(args) as client:
        return client.get_accounts(fields=args.fields)


def _account(args: argparse.Namespace) -> Any:
    with _client(args) as client:
        return client.get_account(args.account_hash, fields=args.fields)


def _orders(args: argparse.Namespace) -> Any:
    with _client(args) as client:
        return client.get_orders(
            args.account_hash,
            start_time=args.start_time,
            end_time=args.end_time,
            status=args.status,
            max_results=args.max_results,
        )


def _quotes(args: argparse.Namespace) -> Any:
    with _client(args) as client:
        return client.get_quotes(args.symbols, fields=args.fields, indicative=args.indicative or None)


def _option_chain(args: argparse.Namespace) -> Any:
    params = {
        key: value
        for key, value in vars(args).items()
        if key
        in {
            "contractType",
            "strikeCount",
            "includeUnderlyingQuote",
            "strategy",
            "interval",
            "strike",
            "range",
            "fromDate",
            "toDate",
            "daysToExpiration",
            "expMonth",
            "optionType",
            "entitlement",
        }
        and value not in (None, False)
    }
    with _client(args) as client:
        return client.get_option_chain(args.symbol, **params)


def _preview_equity_order(args: argparse.Namespace) -> Any:
    with _client(args) as client:
        return client.preview_order(_build_equity_order(client, args), args.account_hash)


def _place_equity_order(args: argparse.Namespace) -> Any:
    with _client(args) as client:
        return client.place_order(
            _build_equity_order(client, args),
            args.account_hash,
            preview_only=False,
            confirm_order_placement=args.confirm_order_placement,
        )


def _preview_option_order(args: argparse.Namespace) -> Any:
    with _client(args) as client:
        return client.preview_order(_build_option_order(client, args), args.account_hash)


def _place_option_order(args: argparse.Namespace) -> Any:
    with _client(args) as client:
        return client.place_order(
            _build_option_order(client, args),
            args.account_hash,
            preview_only=False,
            confirm_order_placement=args.confirm_order_placement,
        )


def _preview_option_spread(args: argparse.Namespace) -> Any:
    with _client(args) as client:
        return client.preview_order(_build_spread_order(client, args), args.account_hash)


def _place_option_spread(args: argparse.Namespace) -> Any:
    with _client(args) as client:
        return client.place_order(
            _build_spread_order(client, args),
            args.account_hash,
            preview_only=False,
            confirm_order_placement=args.confirm_order_placement,
        )


def _add_equity_order_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    name: str,
    func: Any,
    *,
    require_confirmation: bool = False,
) -> None:
    parser = subparsers.add_parser(name)
    parser.add_argument("--account-hash")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--quantity", required=True, type=float)
    parser.add_argument("--instruction", required=True, help="BUY, SELL, BUY_TO_COVER, SELL_SHORT.")
    parser.add_argument("--order-type", default="MARKET")
    parser.add_argument("--limit-price", type=float)
    parser.add_argument("--duration", default="DAY")
    parser.add_argument("--session", default="NORMAL")
    _add_confirmation(parser, require_confirmation)
    parser.set_defaults(func=func)


def _add_option_order_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    name: str,
    func: Any,
    *,
    require_confirmation: bool = False,
) -> None:
    parser = subparsers.add_parser(name)
    parser.add_argument("--account-hash")
    parser.add_argument("--option-symbol", required=True)
    parser.add_argument("--quantity", required=True, type=float)
    parser.add_argument("--instruction", required=True, help="BUY_TO_OPEN, SELL_TO_OPEN, BUY_TO_CLOSE, SELL_TO_CLOSE.")
    parser.add_argument("--order-type", default="LIMIT")
    parser.add_argument("--limit-price", type=float)
    parser.add_argument("--duration", default="DAY")
    parser.add_argument("--session", default="NORMAL")
    _add_confirmation(parser, require_confirmation)
    parser.set_defaults(func=func)


def _add_spread_order_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    name: str,
    func: Any,
    *,
    require_confirmation: bool = False,
) -> None:
    parser = subparsers.add_parser(name)
    parser.add_argument("--account-hash")
    parser.add_argument(
        "--legs-json",
        required=True,
        help='JSON list, e.g. [{"symbol":"XYZ 260501C00100000","quantity":1,"instruction":"SELL_TO_OPEN"}].',
    )
    parser.add_argument("--order-type", default="NET_DEBIT")
    parser.add_argument("--price", type=float)
    parser.add_argument("--duration", default="DAY")
    parser.add_argument("--session", default="NORMAL")
    parser.add_argument("--complex-order-strategy-type", default="CUSTOM")
    _add_confirmation(parser, require_confirmation)
    parser.set_defaults(func=func)


def _add_confirmation(parser: argparse.ArgumentParser, required: bool) -> None:
    if required:
        parser.add_argument(
            "--confirm-order-placement",
            action="store_true",
            help="Required in addition to SCHWAB_TRADING_ENABLED=true for live placement.",
        )


def _build_equity_order(client: SchwabTraderClient, args: argparse.Namespace) -> dict[str, Any]:
    return client.build_equity_order(
        symbol=args.symbol,
        quantity=args.quantity,
        instruction=args.instruction,
        order_type=args.order_type,
        limit_price=args.limit_price,
        duration=args.duration,
        session=args.session,
    )


def _build_option_order(client: SchwabTraderClient, args: argparse.Namespace) -> dict[str, Any]:
    return client.build_option_order(
        option_symbol=args.option_symbol,
        quantity=args.quantity,
        instruction=args.instruction,
        order_type=args.order_type,
        limit_price=args.limit_price,
        duration=args.duration,
        session=args.session,
    )


def _build_spread_order(client: SchwabTraderClient, args: argparse.Namespace) -> dict[str, Any]:
    try:
        legs = json.loads(args.legs_json)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"--legs-json is not valid JSON: {exc}") from exc
    if not isinstance(legs, list):
        raise SystemExit("--legs-json must be a JSON list.")
    return client.build_option_spread_order(
        legs=legs,
        order_type=args.order_type,
        price=args.price,
        duration=args.duration,
        session=args.session,
        complex_order_strategy_type=args.complex_order_strategy_type,
    )


def _token_summary(expires_at: float | None, has_refresh_token: bool) -> dict[str, Any]:
    return {
        "saved": True,
        "has_refresh_token": has_refresh_token,
        "expires_at": expires_at,
    }


def _print_json(payload: Any) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main())
