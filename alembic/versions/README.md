# Alembic Migrations

This directory contains all Alembic migration scripts for the BacktestForecast database schema.

## Migration Consolidation Plan

As of March 2026 there are 26 migration files in this directory. For **new environments**
(fresh databases), running the full migration chain from `0001` through `0026` is slow and
fragile — intermediate migrations may reference columns or constraints that later migrations
alter or drop.

### Recommended approach

1. **Create a baseline migration** that captures the current `head` schema as a single
   `CREATE TABLE` / `CREATE INDEX` script. Use `alembic revision --autogenerate` against
   a freshly-migrated database, then manually clean the generated output.

2. **Stamp existing databases** with the baseline revision so they skip it:
   ```
   alembic stamp <baseline_revision>
   ```

3. **Keep the old migrations** in a sub-directory (e.g. `_archived/`) for audit trail
   purposes but exclude them from the active chain by removing their `down_revision`
   links.

4. All **future migrations** should branch from the new baseline revision.

### When to execute

This consolidation should be performed once the schema has stabilised after launch and
before onboarding any new environments (CI runners, staging clones, etc.) that would
otherwise need to replay the full chain.

### Important

- Do **not** drop old migrations from version control — they serve as an audit record.
- Verify the consolidated baseline against `alembic check` to ensure no model drift.
- Test both `upgrade head` and `downgrade base` on a throwaway database before merging.

## Migration-specific notes

### Migration 0009 — NULL `subject_id` deduplication

Migration 0009 adds a unique constraint on audit events. As part of the upgrade
it **deletes duplicate rows where `subject_id` IS NULL** so the constraint can be
applied cleanly. This is intentional data loss for rows that were already
duplicates with no subject association. If you need to preserve those rows for
forensic purposes, export them before running `alembic upgrade` past this
revision.

### Migration 0024 — JSON → JSONB conversion of `regime_labels`

Migration 0024 changes the `regime_labels` column from `JSON` to `JSONB`. On
PostgreSQL this is an in-place rewrite (`ALTER COLUMN ... TYPE JSONB USING
column::jsonb`). Key considerations:

- The `ALTER` acquires an `ACCESS EXCLUSIVE` lock on the table for the duration
  of the rewrite. For large tables, schedule the migration during a maintenance
  window.
- Any application code that relied on key-ordering guarantees of JSON (insertion
  order) should be reviewed, as JSONB does not preserve key order.
- Downgrade reverts to `JSON`, which is a safe cast direction.
