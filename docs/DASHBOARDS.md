# Grafana Dashboard Guide

Operators should create the following dashboards in Grafana to monitor BacktestForecast. Use Prometheus as the data source.

## Sweep Job Resource Consumption

Track sweep job throughput, task execution, and duration.

| Panel | Type | Query | Notes |
|-------|------|-------|-------|
| Sweep jobs by status | Timeseries | `sum by (status) (rate(sweep_jobs_total[5m]))` | Legend: `{{ status }}` |
| Sweep task rate | Timeseries | `sum by (status) (rate(celery_tasks_total{task_name="sweeps.run"}[5m]))` | Legend: `{{ status }}` |
| Sweep task throughput | Stat | `increase(celery_tasks_total{task_name="sweeps.run"}[1h])` | Completions per hour |

## SSE Connection Metrics

Track Server-Sent Events connection usage and per-user limits.

| Panel | Type | Query | Notes |
|-------|------|-------|-------|
| Active SSE connections | Stat / Gauge | `active_sse_connections` | Current total |
| SSE connections over time | Timeseries | `active_sse_connections` | Trend |
| Per-user SSE limits | Timeseries | `sse_slots_used` or equivalent per-user gauge (if instrumented) | Check `use-sse.ts` / API for metric names |

## Option Cache Memory

Track in-memory option gateway cache size.

| Panel | Type | Query | Notes |
|-------|------|-------|-------|
| Option cache entries | Stat / Gauge | `option_cache_entries` | Current count across all API processes |
| Option cache over time | Timeseries | `option_cache_entries` | Trend; high values may indicate memory pressure |
