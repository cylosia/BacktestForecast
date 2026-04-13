# UVXY Long Put Post-2018 Rule Book

Scope: `UVXY` long puts after the leverage change, starting `2018-02-28`.

Objective: maximize ROI for the post-change product while keeping the rule set grounded in the completed one-contract grid runs.

Test basis:
- Window: `2018-02-28..2020-04-30`
- Structure: `1` contract, long put
- Grid: `delta 5..95 step 5`, `DTE 1..30`, `take profit 20..200 step 20`
- Ranking filter: `status=ok`, `trade_count >= 40`, `decided_trades >= 40`
- Ranking order: `roi_pct_per_day desc`, then `total_net_pnl desc`, then lower `max_drawdown_pct`

Source artifacts:
- [all-regime split comparison summary](/C:/Users/Administrator/BacktestForecast/logs/uvxy_long_put_2015_05_2018_02_27_vs_2018_02_28_2020_04_30_all_regime_split_comparison_summary.json)
- [post-change bearish/not-high-iv grid](/C:/Users/Administrator/BacktestForecast/logs/uvxy_long_put_2018_02_28_2020_04_30_bearish_not_high_iv_delta_dte_take_profit_20_200_step20_grid_one_contract.csv)
- [post-change bearish/low-iv grid](/C:/Users/Administrator/BacktestForecast/logs/uvxy_long_put_2018_02_28_2020_04_30_bearish_low_iv_delta_dte_take_profit_20_200_step20_grid_one_contract.csv)

## Hard Rules

1. Only trade `UVXY` long puts in a `bearish` directional regime.
2. Do not trade `bullish` or `neutral` regimes.
3. Do not require `trending` as an entry gate.
4. Size every trade at `1` contract.
5. Only enter when an exact next-calendar-day put expiration exists.
6. Use end-of-day execution assumptions only.

Reasoning:
- All post-change winners that survived the minimum-trade filter were bearish.
- No bullish or neutral family produced a qualifying row.
- Post-change `bearish+trending` and `bearish+low_iv+trending` produced no row with `>= 40` trades.
- Exact `1 DTE` is calendar-day based in this backtest stack, so regular-week entries are effectively Thursday-only.
- Holiday-shortened weeks can create valid Wednesday entries when the weekly expiration shifts to Thursday.

## Verified Standalone Rule Sets

### Rule Set A: Broad Post-Change ROI Rule

Use this when you want the strongest post-change rule under the broader bearish filter.

- Required regimes: `bearish`
- Blocked regimes: `high_iv`
- Contract: nearest-expiry put with `20 delta`
- `DTE`: `1`
- Take profit: `20%`
- Exit fallback: hold to expiration if the take-profit does not fire
- Entry-day gate: only when a next-day expiration exists

Observed post-change results:
- `trade_count=65`
- `win_rate=29.2308%`
- `roi_pct_per_day=51.3091`
- `total_net_pnl=401.75`
- `max_drawdown_pct=0.0927%`
- `average_purchase_price=0.1205`

Notes:
- `20%` through `200%` take-profit all tied for this winner in the tested grid.
- `20%` is the simplest operational choice because it matched the best result while locking gains earliest.

### Rule Set B: Higher-Efficiency Low-IV Rule

Use this when you want the highest post-change `roi_pct_per_day` among robust bearish subsets.

- Required regimes: `bearish` and `low_iv`
- Contract: nearest-expiry put with `15 delta`
- `DTE`: `1`
- Take profit: `20%`
- Exit fallback: hold to expiration if the take-profit does not fire
- Entry-day gate: only when a next-day expiration exists

Observed post-change results:
- `trade_count=53`
- `win_rate=24.5283%`
- `roi_pct_per_day=58.9712`
- `total_net_pnl=266.55`
- `max_drawdown_pct=0.0793%`
- `average_purchase_price=0.0853`

Trade-off versus Rule Set A:
- Better `roi_pct_per_day`
- Lower total PnL
- Fewer trades

## Recommended Decision Tree

This section is an inference from the standalone sweep winners above. It is the best practical rule book from the completed evidence, but it has not yet been backtested as one combined hierarchical strategy.

1. If no exact next-day put expiration exists, do not trade.
2. Else if regime is `bearish` and `low_iv`, buy `1` `15 delta` put with `1 DTE`.
3. Else if regime is `bearish` and not `high_iv`, buy `1` `20 delta` put with `1 DTE`.
4. Else do not trade.

Take-profit for both branches:
- Set `20%`
- If not hit, hold to expiration

Why this hierarchy:
- `bearish+low_iv` was the most efficient post-change subset.
- The broader `bearish and not high_iv` rule was the strongest higher-sample fallback.
- Adding `trending` made the post-change sample too small to trust.

## What Not To Do

- Do not trade long puts in `bullish` regimes.
- Do not trade long puts in `neutral` regimes.
- Do not force a `trending` overlay post-change.
- Do not spend time optimizing take-profit above `20%` for the `1 DTE` winners. In the tested post-change winners, the `20..200` range tied.

## Execution Assumptions

These rules inherit the current backtest mechanics:

- Entry is modeled on the entry-date end-of-day bar.
- Historical option fills are based on the option day-bar close proxy.
- A `1 DTE` Friday expiration trade is effectively entered on Thursday close and exits on Friday if still open.
- In regular weeks, that means the executable entry day is usually Thursday.
- In holiday-shortened weeks, Wednesday can become the executable `1 DTE` entry day if expiration shifts to Thursday.
- Monday, Tuesday, and most Wednesday signals are intentionally ignored by this rule book because the nearest listed expiration is not exactly one calendar day away.
- No intraday timing edge is assumed.

## Confidence And Limits

- Confidence is highest for Rule Set A, because it has the broadest robust post-change sample.
- Rule Set B is more selective and more efficient, but lower sample.
- The decision tree above is an inference from separate sweeps, not a directly replayed combined strategy.
- The backtests are one-contract, close-based, and do not include live fill frictions beyond the engine's existing assumptions.
