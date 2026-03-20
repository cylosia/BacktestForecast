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

## Migration Branch Note

The migration graph contains one branch that diverges from `20260318_0026`
and merges back at `20260319_0034`:

```
20260318_0026 ─┬── 20260319_0026 → … → 20260319_0033 ──┐
               └── 20260318_0027 → 0024_heartbeat ──────┘→ 20260319_0034 → … → HEAD
```

- **Main chain:** `0026` → `20260319_0026` → … → `0033`
- **Branch:** `0026` → `20260318_0027` (GIN indexes) → `0024_heartbeat` (stub)
- **Merge:** `20260319_0034` (merges `0033` + `0024_heartbeat`)

Running `alembic upgrade head` traverses both paths to the merge point
automatically. Always use `alembic upgrade head` (not a specific revision)
in deployment scripts.

## Additional Tables (Post-Baseline)

| Table | Migration | Description |
|-------|-----------|-------------|
| `stripe_events` | 0002 | Stripe webhook event deduplication |
| `outbox_messages` | 0012 | Transactional outbox for reliable Celery dispatch |
| `sweep_jobs` | 0006 | Parameter sweep job lifecycle |
| `sweep_results` | 0006 | Ranked results from parameter sweeps |
| `task_results` | 0037 | Structured task outcome tracking |

## Trigger

The `set_updated_at()` PostgreSQL trigger function is applied to all tables with an
`updated_at` column. It fires `BEFORE UPDATE` and sets `updated_at = NOW()`,
ensuring accuracy even for direct SQL updates that bypass the ORM's `onupdate`.

## Migration Squash Procedure

With 47 migrations accumulated during rapid development, fresh deployments
take longer than necessary. To squash into a new consolidated baseline:

```bash
# 1. Verify current state on a throwaway database
alembic upgrade head
alembic check  # must report "No new upgrade operations detected"

# 2. Dump the current schema (after all migrations applied)
pg_dump --schema-only --no-owner --no-privileges backtestforecast > schema_snapshot.sql

# 3. Create a new baseline migration
alembic revision -m "consolidated_baseline_v2"

# 4. Replace upgrade() with the full CREATE TABLE statements from schema_snapshot.sql
#    Include all tables, indexes, constraints, triggers, and the set_updated_at() function.
#    Set down_revision = None.

# 5. Archive old migration files
mkdir -p alembic/versions/_archived
mv alembic/versions/2026031*.py alembic/versions/_archived/

# 6. Stamp existing databases (they already have the schema)
alembic stamp <new_revision_id>

# 7. Verify both paths work
#    Fresh database: alembic upgrade head
#    Existing database: alembic current (should show new revision)
```

**IMPORTANT**: Only squash when:
- All environments (dev, staging, production) are at the same revision
- No pending migrations are in flight
- You have a tested rollback plan
