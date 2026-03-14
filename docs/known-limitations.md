# Known limitations

- Market-data failover is still single-provider in practice; retries reduce pain but do not provide true redundancy.
- Webhook dedupe is application-level, not backed by a dedicated unique event table yet.
- Integration tests use SQLite for portability; production remains PostgreSQL-first.
- The scanner still relies on deterministic ranking heuristics rather than a learned ranking model.
- The strategy catalog is served from an in-process Python module; a more dynamic catalog with user-configurable parameters may follow.
- `slippage_pct` was previously not wired to the API and defaulted to 0%. It is now configurable via the backtest request payload. Backtests run before this change used no slippage adjustment; historical results are not retroactively corrected.
- Naked option positions (naked calls/puts) are sized by margin requirement only. This understates the theoretical risk, which is unlimited for naked calls and strike-minus-zero for naked puts. Users should interpret naked-option backtest results with caution and apply their own risk limits.
