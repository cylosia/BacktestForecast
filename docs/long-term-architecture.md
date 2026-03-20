# Long-Term Architectural Improvements

Status of each improvement from the March 2026 production audit.

## LT-15: Rate Limiting Improvements

**Status: Incremental improvements applied**

### What's done
- Redis + in-memory fallback rate limiter with Lua-based atomics
- Fail-closed mode with halved limits when Redis is unavailable
- Separate buckets for metrics, DLQ, auth-fail, and per-feature limits
- **New**: Near-limit warning metric (`rate_limit_near_threshold_total`) fires
  at 80%+ utilization for early abuse detection

### Future work
- Consider Redis Cluster or a dedicated rate-limiting sidecar (e.g., Envoy)
  for multi-region deployments
- Add sliding-window algorithm (currently fixed-window via INCR + EXPIRE)
- Add rate limit dashboards with per-user breakdowns


## LT-16: CQRS for Read-Heavy Endpoints

**Status: Foundation in place**

### What's done
- `get_lightweight_for_user` avoids loading trades/equity (read-optimized)
- `get_run_status` returns only scalar fields (lightweight polling)
- Separate worker session factory with longer statement timeout
- **New**: `database_read_replica_url` config option and `get_readonly_db()`
  session factory.  When set, read-heavy endpoints can use a read replica.

### How to enable
```env
DATABASE_READ_REPLICA_URL=postgresql+psycopg://user:pass@replica:5432/db?sslmode=require
```

### Future work
- Route list/compare/recommendations endpoints through `get_readonly_db()`
- Add materialized views for scan recommendation aggregates
- Consider Redis caching for hot list endpoints


## LT-17: Streaming DB Writes for Large Scan Results

**Status: Adequately addressed by alternative approach**

### What's done
- Candidate cap lowered from 2000 to 1000
- Memory trimming every 200 candidates: low-ranked candidates have heavy
  fields (trades, equity_curve) cleared
- Only `max_recommendations` (typically 10–30) are flushed to DB at the end

### Why not streaming writes
Flushing mid-scan would require:
1. Temporary storage tables or a two-phase ranking approach
2. Transaction management across flushes (can't use CAS on partial data)
3. Handling rollback of partial flushes on scan failure

The trimming approach achieves bounded memory without this complexity.


## LT-18: SSE for Job Status (Polling → Push)

**Status: Fully scaffolded, operational**

### What's done
- Redis Pub/Sub for job status changes (`events.py`)
- SSE endpoint with per-user and per-process connection limits
- Heartbeat (15s), timeout (300s), graceful reconnection
- Fallback to DB persistence when Redis is unavailable
- Connection slot tracking via Redis keys with TTL

### Future work
- Frontend integration (currently uses polling; SSE endpoints are available
  but the Next.js app hasn't switched to EventSource yet)
- Add SSE dashboard panel in Grafana


## LT-19: Distributed Tracing

**Status: Partial — header propagation in place**

### What's done
- `traceparent` header propagated from API → Celery task headers
- Celery `before_start` hook extracts `traceparent` and binds to structlog
- Massive API client forwards `traceparent` on outgoing requests
- `request_id` header propagated across all layers
- Sentry integration for error tracking (when configured)

### Future work
- Add OpenTelemetry SDK for structured spans (not just header propagation)
- Instrument SQLAlchemy queries, Redis calls, and HTTP clients
- Export traces to Jaeger/Tempo for end-to-end visibility
- Add trace-aware log correlation (trace_id in structured logs)


## LT-20: Feature Flag Service

**Status: Robust env-var-based system with observability**

### What's done
- 4-layer evaluation: kill switch → allow-list → tier targeting → % rollout
- Deterministic CRC32 bucketing for consistent user experience
- Settings snapshot for atomic evaluation (no mid-check config race)
- **New**: `feature_flag_evaluations_total` Prometheus counter with labels
  for `feature` and `result` (enabled, disabled, tier_excluded,
  rollout_excluded)

### Future work
- Consider LaunchDarkly, Unleash, or Flipt for a dedicated service with:
  - UI for non-engineering flag management
  - Audit trail for flag changes
  - Real-time flag updates without process restart
  - A/B testing integration
- The current env-var approach works well for the current team size;
  switch when flag changes need to be faster than a deploy cycle
