# Concurrency: Optimistic Locking with `updated_at`

## Template Update Pattern

Templates use an optimistic concurrency control pattern to prevent lost
updates when multiple clients edit the same template concurrently.

### How it works

1. The client reads a template, receiving its `updated_at` timestamp.
2. When submitting an update, the client sends the timestamp back as
   `expected_updated_at`.
3. The server compares `expected_updated_at` against the stored
   `updated_at` value using a **2 ms tolerance window**.
4. If the values match (within tolerance), the update proceeds and
   `updated_at` is refreshed.
5. If they don't match, the server returns `409 Conflict`, indicating
   another client modified the template since it was last read.

### Why 2 ms tolerance?

Some databases and serialization layers truncate or round microsecond
precision differently:

- PostgreSQL stores `timestamptz` with microsecond precision, but JSON
  serialization via `datetime.isoformat()` may truncate trailing zeros.
- JavaScript's `Date` object only has millisecond precision. A
  `2026-03-14T12:00:00.123456+00:00` timestamp round-trips through the
  frontend as `2026-03-14T12:00:00.123Z`, losing 456 microseconds.
- Different JSON libraries may serialize/deserialize with varying
  precision (e.g. `ujson` vs `orjson` vs `json`).

The 2 ms window absorbs these rounding differences while remaining tight
enough to catch genuine concurrent modifications (which would typically
differ by seconds or more).

### Implementation reference

```python
tolerance = timedelta(milliseconds=2)
if abs(stored.updated_at - expected_updated_at) > tolerance:
    raise ConflictError("Template was modified by another request.")
```

### Trade-offs

- **Pro**: No database-level row locks, no pessimistic locking overhead.
- **Pro**: Works naturally with REST APIs and stateless backends.
- **Con**: Clients must handle `409 Conflict` by re-fetching and retrying.
- **Con**: The 2 ms window theoretically allows a race if two updates
  happen within 2 ms of each other, but this is extremely unlikely in
  practice given network latency.

## Statement Timeouts

PostgreSQL `statement_timeout` is configured per session type to bound
the maximum duration of any single SQL statement:

| Session type            | Timeout  | Rationale                                                       |
|-------------------------|----------|-----------------------------------------------------------------|
| API sessions            | 30 s     | User-facing requests should fail fast.                          |
| Worker sessions         | 300 s    | Background jobs (backtests, scans, sweeps) need more headroom.  |
| Scan candidate timeout  | 120 s    | Must be strictly less than the worker `statement_timeout` so an individual candidate times out before the whole session is killed. |

If you increase the worker `statement_timeout`, verify that
`soft_time_limit` on the corresponding Celery task is still greater
than `statement_timeout` to allow graceful cleanup.

## HTTP Connection Reuse

MassiveClient uses httpx connection pooling with `max_connections=20`,
`max_keepalive_connections=10`, and `keepalive_expiry=30`. Each Celery
task still creates its own client instance, but the connection pool
within each instance reuses connections for multiple API calls within
the same task.
