from __future__ import annotations

import argparse
import json
from decimal import Decimal

from _bootstrap import bootstrap_repo

bootstrap_repo(load_api_env=True)

from backtestforecast.integrations.massive_status import fetch_massive_status
from backtestforecast.integrations.massive_client import MassiveClient
from backtestforecast.market_data.prewarm import prewarm_long_option_backtest
from backtestforecast.market_data.service import MarketDataService
from backtestforecast.schemas.backtests import CreateBacktestRunRequest, StrategyType


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prewarm the durable option contract catalog for historical long-option backtests.")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--strategy", choices=[StrategyType.LONG_CALL.value, StrategyType.LONG_PUT.value], required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--target-dte", type=int, required=True)
    parser.add_argument("--dte-tolerance-days", type=int, default=3)
    parser.add_argument("--max-holding-days", type=int, default=7)
    parser.add_argument("--account-size", default="100000")
    parser.add_argument("--risk-per-trade-pct", default="100")
    parser.add_argument("--commission-per-contract", default="0.65")
    parser.add_argument("--max-dates", type=int, default=None)
    parser.add_argument("--include-quotes", action="store_true")
    parser.add_argument(
        "--atm-offset-steps",
        type=int,
        default=None,
        help="Convenience override for long_call_strike/long_put_strike with mode=atm_offset_steps.",
    )
    parser.add_argument(
        "--strategy-overrides-json",
        default=None,
        help='Optional JSON object, for example {"long_call_strike":{"mode":"atm_offset_steps","value":0}}',
    )
    parser.add_argument(
        "--ignore-provider-status",
        action="store_true",
        help="Run even if Massive options REST is currently degraded.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    strategy_overrides = json.loads(args.strategy_overrides_json) if args.strategy_overrides_json else None
    if args.atm_offset_steps is not None:
        key = "long_call_strike" if args.strategy == StrategyType.LONG_CALL.value else "long_put_strike"
        strategy_overrides = {
            **(strategy_overrides or {}),
            key: {"mode": "atm_offset_steps", "value": args.atm_offset_steps},
        }
    if not args.ignore_provider_status:
        status = fetch_massive_status()
        if status.options_rest_degraded:
            print(json.dumps(status.to_dict(), indent=2))
            raise SystemExit(
                "Massive options REST is currently degraded. "
                "Use --ignore-provider-status to force a live prewarm run anyway."
            )
    request = CreateBacktestRunRequest(
        symbol=args.symbol,
        strategy_type=StrategyType(args.strategy),
        start_date=args.start_date,
        end_date=args.end_date,
        target_dte=args.target_dte,
        dte_tolerance_days=args.dte_tolerance_days,
        max_holding_days=args.max_holding_days,
        account_size=Decimal(args.account_size),
        risk_per_trade_pct=Decimal(args.risk_per_trade_pct),
        commission_per_contract=Decimal(args.commission_per_contract),
        entry_rules=[],
        strategy_overrides=strategy_overrides,
    )

    with MassiveClient() as client:
        service = MarketDataService(client)
        try:
            summary = prewarm_long_option_backtest(
                request,
                market_data_service=service,
                include_quotes=args.include_quotes,
                max_dates=args.max_dates,
            )
        finally:
            service.close()

    print(json.dumps(summary.to_dict(), indent=2))
    if summary.errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
