# BacktestForecast API Error Codes

All error responses follow the envelope format:

```json
{
  "error": {
    "code": "error_code_here",
    "message": "Human-readable description",
    "request_id": "optional-request-id"
  }
}
```

## Authentication Errors

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `authentication_error` | 401 | Missing or invalid Bearer token / session cookie |
| `token_too_large` | 401 | JWT exceeds 8192 bytes |

## Authorization Errors

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `feature_locked` | 403 | Feature requires a higher plan tier |
| `quota_exceeded` | 429 | Monthly quota reached for the current plan |

## Validation Errors

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `request_validation_error` | 422 | Request payload did not match expected schema |
| `validation_error` | 422 | Business logic validation failure |

## Resource Errors

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `not_found` | 404 | Requested resource does not exist or is not owned by the user |
| `conflict` | 409 | Action conflicts with current state (e.g., deleting a running job) |

## Rate Limiting

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `rate_limited` | 429 | Too many requests; see `Retry-After` header |

## Server Errors

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `internal_server_error` | 500 | Unexpected server error |
| `external_service_error` | 502 | Upstream API (Stripe, Massive) unavailable |
| `configuration_error` | 500 | Server misconfiguration (details redacted) |

## Job-Specific Error Codes

| Code | Context | Description |
|------|---------|-------------|
| `enqueue_failed` | All job types | Failed to dispatch task to Celery broker |
| `entitlement_revoked` | All job types | User's subscription was downgraded during execution |
| `time_limit_exceeded` | All job types | Task exceeded its soft time limit |
| `max_retries_exceeded` | All job types | Task failed after exhausting all retries |
| `quota_exceeded` | Backtests | Monthly backtest quota reached |
| `sweep_empty` | Sweeps | No sweep combinations completed successfully |
| `sweep_execution_error` | Sweeps | Unexpected error during sweep execution |
| `export_generation_failed` | Exports | Export file generation failed |
| `unsupported_format` | Exports | Requested export format is not supported |
| `user_not_found` | All job types | User account was deleted during execution |
| `stale_running` | Reaper | Job was stuck in running state and auto-failed |
| `subscription_revoked` | Billing | Subscription cancelled; in-flight jobs stopped |

## API Versioning

The API uses URL-based versioning with the `/v1` prefix. All endpoints except
`/health/*`, `/metrics`, `/admin/*`, and `/` are under `/v1`.

Breaking changes will use a new version prefix (e.g., `/v2`). Non-breaking
additions (new fields, new endpoints) may be added to `/v1` without version bump.
