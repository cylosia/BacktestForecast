# Alembic Migrations

This directory contains the Alembic migration scripts for the BacktestForecast database schema.

## Baseline Migration

The schema is defined in a single baseline migration (`20260315_0001_baseline.py`) that
creates all 12 tables, indexes, CHECK constraints, and the `set_updated_at()` trigger
function with per-table triggers.

This baseline was consolidated on 2026-03-15 from 36 incremental migrations
(`0001`–`0036`) that were created during initial development. Since the application
had not yet been deployed to production, no stamping or archival was necessary — the
old migrations were simply replaced.

## Adding New Migrations

All future migrations should branch from `20260315_0001`:

```bash
alembic revision --autogenerate -m "describe your change"
```

Verify the generated output matches your ORM model changes, then test both directions:

```bash
# Forward
alembic upgrade head

# Backward (on a throwaway database)
alembic downgrade base
```

## Tables

| Table | Description |
|-------|-------------|
| `users` | Clerk-authenticated users with Stripe billing |
| `backtest_runs` | Async backtest job lifecycle and results |
| `backtest_trades` | Individual trades from completed backtests |
| `backtest_equity_points` | Daily equity curve points per backtest |
| `backtest_templates` | Saved backtest configurations |
| `scanner_jobs` | Market scanning job lifecycle |
| `scanner_recommendations` | Ranked results from completed scans |
| `export_jobs` | PDF/CSV export lifecycle and storage |
| `audit_events` | Append-only event log for compliance |
| `nightly_pipeline_runs` | Automated nightly screening pipeline |
| `daily_recommendations` | Ranked picks from the nightly pipeline |
| `symbol_analyses` | Deep single-symbol analysis jobs |

## Trigger

The `set_updated_at()` PostgreSQL trigger function is applied to all tables with an
`updated_at` column. It fires `BEFORE UPDATE` and sets `updated_at = NOW()`,
ensuring accuracy even for direct SQL updates that bypass the ORM's `onupdate`.
