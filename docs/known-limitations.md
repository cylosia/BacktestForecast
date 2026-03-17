# Known limitations

- Market-data failover is still single-provider in practice; retries reduce pain but do not provide true redundancy.
- Webhook dedupe is application-level, not backed by a dedicated unique event table yet.
- Integration tests use SQLite for portability; production remains PostgreSQL-first.
- The scanner still relies on deterministic ranking heuristics rather than a learned ranking model.
- The strategy catalog is served from an in-process Python module; a more dynamic catalog with user-configurable parameters may follow.
- `slippage_pct` was previously not wired to the API and defaulted to 0%. It is now configurable via the backtest request payload. Backtests run before this change used no slippage adjustment; historical results are not retroactively corrected.
- Naked option positions (naked calls/puts) are sized by margin requirement only. This understates the theoretical risk, which is unlimited for naked calls and strike-minus-zero for naked puts. Users should interpret naked-option backtest results with caution and apply their own risk limits.
- `RISK_FREE_RATE` is a static environment variable (default 0.045). Sharpe and Sortino ratio calculations use this value without date sensitivity. When the prevailing risk-free rate changes, the env var must be updated manually and backtests re-run to reflect current conditions.
- `market_date_today()` uses a hybrid holiday set: a static fallback covering 2025-2027 plus dynamic holidays fetched weekly from the Massive `/v1/marketstatus/upcoming` endpoint and cached in Redis. If the Massive API or Redis is unavailable, the static set is used alone. The static set should still be extended periodically as a safety net.
- Sortino ratio uses a sample-corrected denominator (N-1) for internal consistency with the Sharpe ratio calculation. Some academic references use a population denominator (N); results may differ slightly from external tools that use the population formula.
