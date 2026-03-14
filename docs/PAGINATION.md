# Pagination

## Cursor Format for Daily Picks

### Composite Cursor

Daily picks pagination uses a composite cursor format:

```
{iso_timestamp}|{uuid}
```

Example: `2025-03-14T16:30:00.000Z|a1b2c3d4-e5f6-7890-abcd-ef1234567890`

### Rationale

Simple timestamp-only cursors can **skip records** when multiple picks share the same timestamp:

- Several picks may be created in the same second (e.g., batch processing)
- A cursor of `2025-03-14T16:30:00.000Z` is ambiguous: does "next" mean after this exact moment, or after this record?
- Paginating with `WHERE created_at > cursor` would skip all other records with that identical timestamp

By appending the UUID, the cursor uniquely identifies a single record. The next page uses:

- `created_at > timestamp` OR
- `(created_at = timestamp AND id > uuid)` (for tie-breaking)

This ensures no records are skipped when timestamps collide.
