# Backtest Metrics Semantics

## CAGR Null Semantics for Short Periods

### Behavior

CAGR (Compound Annual Growth Rate) returns **null** for backtests shorter than approximately 60 trading days.

### Rationale

Annualizing a very short return is misleading. For example:

- A 1-week backtest returning 2% would annualize to an absurd ~180% CAGR
- A 1-day backtest with a small gain or loss would produce extreme, meaningless annualized figures

Short periods have high variance; extrapolating them to an annual rate is statistically unsound and would mislead users.

### UI Guidance

When CAGR is null:

- **Display:** Show "—" or "N/A" instead of a number
- **Tooltip/help:** Explain that CAGR requires at least ~60 trading days to be meaningful
- **Avoid:** Do not show "0%", "∞", or any numeric placeholder that could be misinterpreted
