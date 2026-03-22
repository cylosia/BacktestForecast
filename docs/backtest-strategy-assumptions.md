# Backtest Strategy Assumptions

This document describes the default modeling assumptions used by the current multi-strategy backtesting engine.

## Global assumptions

- Entries are evaluated on daily closes.
- Option pricing uses the latest same-day quote mid when available.
- If a same-day quote is missing before expiration, the prior mid is carried forward and a run warning is emitted.
- If a same-day quote is missing on or after expiration, intrinsic value is used.
- Runs still allow only one active strategy package at a time.
- `commission_per_contract` applies only to option contracts. Stock commissions are assumed to be zero in this slice.
- `entry_mid` and `exit_mid` on multi-leg trades represent the normalized net package value per 100-share option multiplier.
- Sizing uses both available cash/collateral and `risk_per_trade_pct` against estimated max loss.
- Credit strategies reserve capital against defined max loss or cash-secured collateral, not against premium received.

## Strategy assumptions

### Long Call
- Uses a single call expiration nearest `target_dte`.
- Strike is nearest-to-spot with a small OTM tie bias.
- Max loss is the premium paid.

### Long Put
- Uses a single put expiration nearest `target_dte`.
- Strike is nearest-to-spot with a small OTM tie bias.
- Max loss is the premium paid.

### Covered Call
- Buys 100 shares per contract and sells one OTM call.
- The short call strike is the nearest listed strike at or above spot.
- Capital sizing is constrained by share cost, not just option premium.
- The combined covered-call package is exited at the call expiration, `max_holding_days`, or backtest end.

### Cash-Secured Put
- Sells one OTM put per package.
- The short put strike is the nearest listed strike at or below spot.
- Capital sizing assumes full strike collateral.
- Standalone cash-secured put backtests realize P&L at close/expiration and do not convert to shares.

### Bull Call Debit Spread
- Long call at the nearest listed call strike at or above spot.
- Short call at the next higher listed strike.
- Max loss is net debit. Max profit is spread width minus debit.

### Bear Put Debit Spread
- Long put at the nearest listed put strike at or below spot.
- Short put at the next lower listed strike.
- Max loss is net debit. Max profit is spread width minus debit.

### Bull Put Credit Spread
- Short put at the nearest listed put strike at or below spot.
- Long put at the next lower listed strike.
- Max loss is spread width minus net credit.

### Bear Call Credit Spread
- Short call at the nearest listed call strike at or above spot.
- Long call at the next higher listed strike.
- Max loss is spread width minus net credit.

### Iron Condor
- Short strikes are one listed OTM strike on each side of spot.
- Long wings are one additional listed strike beyond the shorts.
- The widest side determines capital at risk.

### Long Straddle
- Buys one ATM call and one ATM put with the same expiration and same strike when available.
- Max loss is total debit paid.

### Long Strangle
- Buys one listed OTM call and one listed OTM put with the same expiration.
- Max loss is total debit paid.

### Calendar Spread
- Supports both call and put calendars. Default is call calendar unless `strategy_overrides.calendar_contract_type="put"` is supplied.
- Short leg uses the expiration nearest `target_dte`.
- Long leg uses the next later expiration at least 14 calendar days farther out when available.
- Package exits at the near-leg expiration, `max_holding_days`, or backtest end.

### Butterfly
- Modeled as a long call butterfly in this slice.
- Center strike is the listed strike nearest spot.
- Wings use the immediately adjacent listed strikes above and below the center.
- If strike spacing is asymmetric, the narrower wing determines max profit potential.

### Wheel Strategy
- Entry rules gate both short-put entry and covered-call sales.
- Phase 1 sells a cash-secured put using full strike collateral for sizing.
- If the put expires ITM, assignment converts the phase into long shares.
- Share inventory is tracked separately from option phases.
- Covered calls are sold only when shares are held and entry rules are satisfied.
- If a covered call expires ITM, shares are considered called away and the stock leg is closed.
- Remaining shares at the end of the run are liquidated on the final available bar.

### Poor Man's Covered Call (PMCC)
- Buys a deep-ITM far-dated call (2 strikes below spot) and sells an OTM near-dated call.
- Two legs with different expirations.
- Capital required is the net debit paid. Max loss is the debit.
- If opened for a net credit, margin is calculated as naked call margin on the short leg.

### Diagonal Spread
- Sells a near-dated call at a resolved strike, buys a far-dated call one strike increment lower.
- Two legs with different expirations.
- Risk profile mirrors PMCC: debit case uses debit as capital, credit case uses naked call margin.

### Double Diagonal
- Four legs across two expirations: sells near-dated call and put (OTM), buys far-dated call and put further OTM.
- Capital is net debit if opened for debit; otherwise short straddle/strangle margin.
- Max loss is the debit paid (if debit); uncapped if opened for credit.

### Ratio Call Backspread
- Sells 1 lower-strike call and buys 2 higher-strike calls (same expiration, 1:2 ratio).
- Max loss is the risk between strikes plus net debit, realised if the underlying finishes at the long strike at expiration.
- Unlimited upside profit potential from the extra long call.

### Ratio Put Backspread
- Sells 1 higher-strike put and buys 2 lower-strike puts (same expiration, 1:2 ratio).
- Mirror image of ratio call backspread.
- Max loss is the risk between strikes plus net debit. Unlimited downside profit potential.

### Collar
- Buys 100 shares, sells 1 OTM call, buys 1 OTM put (same expiration).
- Two option legs plus one stock leg. Fully defined risk.
- Max loss is (spot minus put strike) times 100 plus net option cost.
- Max profit is (call strike minus spot) times 100 minus net option cost.

### Short Straddle
- Sells an ATM call and an ATM put at the same strike.
- Max loss is unlimited in both directions. Max profit is the total credit received.
- Capital is calculated using short straddle/strangle margin (Reg T).

### Short Strangle
- Sells an OTM call and an OTM put at separately resolved strikes.
- Risk profile same as short straddle but with a wider breakeven range.
- Capital uses short straddle/strangle margin.

### Covered Strangle
- Buys 100 shares, sells 1 OTM call, sells 1 OTM put (same expiration).
- More aggressive than a collar: exposed to downside below the put strike with additional assignment risk.
- Capital uses covered strangle margin (stock cost plus put collateral with overlap discount).

### Synthetic Put
- Shorts 100 shares and buys 1 ATM call.
- One option leg plus one stock leg. Behaves like a long put.
- Profits as the stock falls. Losses are capped by the call protection.

### Reverse Conversion
- Shorts 100 shares, buys 1 ATM call, sells 1 ATM put (same strike).
- Two option legs plus one stock leg. Arbitrage-style structure.
- P&L is determined by net option cost versus stock-to-strike carry.

### Jade Lizard
- Sells 1 OTM put, sells 1 OTM call, buys 1 higher-strike call (bear call spread plus naked put).
- Three legs, same expiration. Only entered if total credit is positive.
- Upside risk can be zero when credit exceeds call spread width.
- Unlimited downside risk below the put strike.

### Iron Butterfly
- Sells ATM call and ATM put (same center strike), buys OTM call wing and OTM put wing.
- Four legs, same expiration. Fully defined risk.
- Max loss is the wider wing width minus credit received. Max profit is the credit.
- Rejected if opened for a net debit.

### Custom 2/3/4/5/6/8 Leg
- Accepts user-defined leg specifications: asset type (stock or option), contract type, side, strike offset from ATM, expiration offset (0/1/2), and quantity ratio.
- Supports up to 3 different expirations and mixed stock/option legs.
- Capital is estimated by pairing short legs with matching longs as credit spreads; unpaired shorts use naked option margin.
- Max loss and max profit are not calculated (both `None`).

### Naked Call
- Sells a single call at a resolved strike. Unlimited theoretical loss.
- Capital uses Reg T naked option margin formula.
- Users should interpret results with caution — see known limitations.

### Naked Put
- Sells a single put at a resolved strike. Loss is capped at strike-to-zero.
- Capital uses Reg T naked option margin formula.
- Users should interpret results with caution — see known limitations.

## Indicator/rule assumptions

### RSI
- Standard Wilder RSI.

### SMA/EMA crossover
- Entry triggers only on the crossover event day, not on persistent trend continuation.

### MACD
- Entry triggers only on MACD line crossing the signal line that day.

### Bollinger Bands
- Rule compares close to the requested lower/middle/upper band.

### IV Rank / IV Percentile
- Historical IV is estimated from same-day near-ATM option mids using Black-Scholes inversion with zero rates/dividends in this slice.
- The estimate uses the nearest common call/put strike for the selected expiration when available.

### Volume spikes
- Current volume is compared to the average of the prior `lookback_period` sessions.

### Support / resistance
- Support and resistance use rolling closes only, not swing-high/swing-low fractals or intraday highs/lows.
- Breakout/breakdown compares today’s close with the prior rolling level plus/minus tolerance.

### Avoid earnings
- Uses the existing Massive corporate-events integration when available.

## Unit conventions for `entry_mid` / `exit_mid`

The `entry_mid` and `exit_mid` values stored in `detail_json` use different unit
conventions depending on the engine:

| Engine          | Convention                                   |
|-----------------|----------------------------------------------|
| Generic engine  | Per-unit / per-100-share option multiplier   |
| Wheel engine    | Per-share option premium (not multiplied)    |

Check the `unit_convention` field in the run's `detail_json` to determine which
convention applies. Downstream consumers that compare P&L across strategies must
normalise to a common unit before aggregation.

## Covered strangle margin conservatism

The covered strangle margin calculation uses the sum of the individual leg margin
requirements. This is more conservative than typical broker margin, which
recognises the natural offset between legs and applies a lower combined
requirement. Backtest results may therefore overstate capital requirements
relative to live broker accounts.

## CAGR short-period limitation

CAGR (Compound Annual Growth Rate) is not computed for backtests shorter than
60 calendar days. For runs under this threshold the `cagr` field in the summary
is returned as `null`. Short-period CAGR values are highly sensitive to start/end
timing and are misleading when annualised.
