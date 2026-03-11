# Failure-mode review

| Area | Failure mode | Current behavior | Recommended operator action |
|---|---|---|---|
| Auth | Clerk signing key misconfigured | API rejects authenticated calls | Re-verify Clerk issuer/JWKS env and redeploy |
| Billing | Stripe webhook replay | Duplicate events are ignored by audit-backed dedupe | Verify webhook delivery logs; no manual action usually needed |
| Billing | Unknown Stripe price id | Falls back to subscription metadata tier, otherwise Free | Confirm price ids match env; inspect webhook payload |
| Market data | Massive 429 / 5xx | Client retries with backoff, then returns provider error | Throttle launches, keep read-only surfaces available |
| Redis | Redis unavailable | Rate limiter degrades to in-memory fallback; ready endpoint marks degraded | Restore Redis quickly; avoid horizontal scale until healthy |
| Worker | Scan task exceptions | Celery retries transient failures; failed jobs retain warnings/error_code | Inspect failed job payload and task logs |
| Exports | CSV formula injection | Strings are sanitized before export | No action needed; keep regression test in CI |
| Database | DB unavailable | Ready endpoint returns 503 | Fail traffic away and restore DB before reopening writes |
