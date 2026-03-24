from __future__ import annotations

from dataclasses import dataclass

from backtestforecast.backtests.margin import short_straddle_strangle_margin
from backtestforecast.backtests.strategies.base import StrategyDefinition
from backtestforecast.backtests.strategies.common import (
    choose_common_atm_strike,
    choose_primary_expiration,
    contracts_for_expiration,
    get_overrides,
    require_contract_for_strike,
    resolve_strike,
    synthetic_ticker,
    valid_entry_mids,
)
from backtestforecast.backtests.types import (
    BacktestConfig,
    OpenMultiLegPosition,
    OpenOptionLeg,
    OptionDataGateway,
)
from backtestforecast.errors import DataUnavailableError
from backtestforecast.market_data.types import DailyBar


@dataclass(frozen=True, slots=True)
class ShortVolatilityStrategy(StrategyDefinition):
    strategy_type: str
    margin_warning_message: str | None = (
        "Short volatility positions have theoretically unlimited loss and require margin."
    )

    def build_position(
        self,
        config: BacktestConfig,
        bar: DailyBar,
        bar_index: int,
        option_gateway: OptionDataGateway,
    ) -> OpenMultiLegPosition | None:
        overrides = get_overrides(config.strategy_overrides)
        calls = option_gateway.list_contracts(bar.trade_date, "call", config.target_dte, config.dte_tolerance_days)
        puts = option_gateway.list_contracts(bar.trade_date, "put", config.target_dte, config.dte_tolerance_days)
        common_expirations = sorted({c.expiration_date for c in calls} & {c.expiration_date for c in puts})
        if not common_expirations:
            raise DataUnavailableError("No common call/put expiration available.")
        expiration = choose_primary_expiration(
            [c for c in calls if c.expiration_date in common_expirations],
            bar.trade_date,
            config.target_dte,
        )
        cc = contracts_for_expiration(calls, expiration)
        pc = contracts_for_expiration(puts, expiration)
        dte = (expiration - bar.trade_date).days

        if self.strategy_type == "short_straddle":
            strike = choose_common_atm_strike(cc, pc, bar.close_price)
            call_c = require_contract_for_strike(cc, strike)
            put_c = require_contract_for_strike(pc, strike)
        else:
            _iv_cache = getattr(option_gateway, '_iv_cache', None)
            call_strike = resolve_strike(
                [c.strike_price for c in cc],
                bar.close_price,
                "call",
                overrides.short_call_strike,
                dte,
                contracts=cc,
                option_gateway=option_gateway,
                trade_date=bar.trade_date,
                iv_cache=_iv_cache,
                risk_free_rate=config.resolve_risk_free_rate(bar.trade_date),
            )
            put_strike = resolve_strike(
                [c.strike_price for c in pc],
                bar.close_price,
                "put",
                overrides.short_put_strike,
                dte,
                contracts=pc,
                option_gateway=option_gateway,
                trade_date=bar.trade_date,
                iv_cache=_iv_cache,
                risk_free_rate=config.resolve_risk_free_rate(bar.trade_date),
            )
            call_c = require_contract_for_strike(cc, call_strike)
            put_c = require_contract_for_strike(pc, put_strike)

        cq = option_gateway.get_quote(call_c.ticker, bar.trade_date)
        pq = option_gateway.get_quote(put_c.ticker, bar.trade_date)
        if cq is None or pq is None:
            return None
        if not valid_entry_mids(cq.mid_price, pq.mid_price):
            return None

        credit = (cq.mid_price + pq.mid_price) * 100.0
        capital_required = short_straddle_strangle_margin(
            bar.close_price,
            call_c.strike_price,
            put_c.strike_price,
            cq.mid_price,
            pq.mid_price,
        )

        return OpenMultiLegPosition(
            display_ticker=synthetic_ticker([call_c.ticker, put_c.ticker]),
            strategy_type=self.strategy_type,
            underlying_symbol=config.symbol,
            entry_date=bar.trade_date,
            entry_index=bar_index,
            quantity=1,
            dte_at_open=(expiration - bar.trade_date).days,
            option_legs=[
                OpenOptionLeg(
                    call_c.ticker, "call", -1, call_c.strike_price, expiration, 1, cq.mid_price, cq.mid_price
                ),
                OpenOptionLeg(put_c.ticker, "put", -1, put_c.strike_price, expiration, 1, pq.mid_price, pq.mid_price),
            ],
            scheduled_exit_date=expiration,
            capital_required_per_unit=capital_required,
            max_loss_per_unit=None,
            max_profit_per_unit=credit,
            detail_json={
                "legs": [
                    {
                        "asset_type": "option",
                        "ticker": call_c.ticker,
                        "side": "short",
                        "contract_type": "call",
                        "strike_price": call_c.strike_price,
                        "expiration_date": expiration.isoformat(),
                        "quantity_per_unit": 1,
                        "entry_mid": cq.mid_price,
                    },
                    {
                        "asset_type": "option",
                        "ticker": put_c.ticker,
                        "side": "short",
                        "contract_type": "put",
                        "strike_price": put_c.strike_price,
                        "expiration_date": expiration.isoformat(),
                        "quantity_per_unit": 1,
                        "entry_mid": pq.mid_price,
                    },
                ],
                "assumptions": [
                    "Short volatility is modeled as selling both a call and put at/near ATM.",
                    "Margin requirement is estimated using naked option margin for each leg.",
                ],
                "entry_package_market_value": -credit,
                "capital_required_per_unit": capital_required,
                "max_loss_per_unit": None,
                "max_profit_per_unit": credit,
            },
        )


SHORT_STRADDLE_STRATEGY = ShortVolatilityStrategy(strategy_type="short_straddle")
SHORT_STRANGLE_STRATEGY = ShortVolatilityStrategy(strategy_type="short_strangle")
