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
- Modeled as a call calendar in this slice.
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
