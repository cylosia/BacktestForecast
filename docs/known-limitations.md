# Known limitations

- Market-data failover is still single-provider in practice; retries reduce pain but do not provide true redundancy.
- Webhook dedupe is application-level, not backed by a dedicated unique event table yet.
- Integration tests use SQLite for portability; production remains PostgreSQL-first.
- The scanner still relies on deterministic ranking heuristics rather than a learned ranking model.
- The strategy catalog is served from an in-process Python module; a more dynamic catalog with user-configurable parameters may follow.
