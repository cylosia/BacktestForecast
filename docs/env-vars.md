# Environment Variables Reference

All environment variables recognised by BacktestForecast. Variables marked **required** must be set in the indicated environments; all others have sensible defaults.

## Application

| Variable | Description | Default | Required in |
|---|---|---|---|
| `APP_NAME` | Display name for the API service | `BacktestForecast API` | — |
| `APP_ENV` | Runtime environment (`development`, `test`, `staging`, `production`) | `development` | All |
| `APP_PUBLIC_URL` | Public URL of the Next.js frontend | `http://localhost:3000` | Staging, Production |
| `API_PUBLIC_URL` | Public URL of the FastAPI backend | `http://localhost:8000` | Staging, Production |
| `API_PORT` | Port the API server listens on | `8000` | — |
| `WEB_PORT` | Port the web dev server listens on | `3000` | — |
| `LOG_LEVEL` | Python log level (`DEBUG`/`INFO`/`WARNING`/`ERROR`/`CRITICAL`) | `INFO` | — |
| `LOG_JSON` | Emit structured JSON logs (must be `true` in production) | `false` | Production |

## Database

| Variable | Description | Default | Required in |
|---|---|---|---|
| `DATABASE_URL` | SQLAlchemy connection string for PostgreSQL | `postgresql+psycopg://backtestforecast:...@localhost:5432/backtestforecast` | All |
| `DB_POOL_SIZE` | Connection pool size | `5` | — |
| `DB_POOL_MAX_OVERFLOW` | Maximum overflow connections beyond pool size | `10` | — |
| `DB_POOL_RECYCLE` | Seconds before a pooled connection is recycled | `1800` | — |
| `DB_POOL_TIMEOUT` | Seconds to wait for a connection from the pool before raising | `10` | — |

## Redis

| Variable | Description | Default | Required in |
|---|---|---|---|
| `REDIS_URL` | Redis URL used by Celery broker and as the default for all Redis operations | `redis://localhost:6379/0` | All |
| `REDIS_PASSWORD` | Redis password; injected into `REDIS_URL` if the URL lacks credentials | — | Production |
| `REDIS_CACHE_URL` | Separate Redis URL for rate limiting, SSE, and caching (defaults to `REDIS_URL`) | `REDIS_URL` | — |

## Auth (Clerk)

| Variable | Description | Default | Required in |
|---|---|---|---|
| `CLERK_SECRET_KEY` | Clerk backend API secret key | — | Production |
| `CLERK_ISSUER` | Expected JWT issuer for token verification | — | Production |
| `CLERK_AUDIENCE` | Expected JWT audience | — | Production |
| `CLERK_JWKS_URL` | URL to Clerk's JWKS endpoint (derived from `CLERK_ISSUER` if unset) | — | Production (or `CLERK_JWT_KEY`) |
| `CLERK_JWT_KEY` | PEM-encoded public key for local JWT verification | — | Production (or `CLERK_JWKS_URL`) |
| `CLERK_AUTHORIZED_PARTIES` | Comma-separated list of allowed `azp` claim values | `http://localhost:3000` | Production |

## Billing (Stripe)

| Variable | Description | Default | Required in |
|---|---|---|---|
| `STRIPE_SECRET_KEY` | Stripe API secret key | — | When billing is enabled |
| `STRIPE_WEBHOOK_SECRET` | Stripe webhook signing secret | — | When billing is enabled |
| `STRIPE_PRO_MONTHLY_PRICE_ID` | Stripe Price ID for Pro monthly plan | — | When billing is enabled |
| `STRIPE_PRO_YEARLY_PRICE_ID` | Stripe Price ID for Pro yearly plan | — | When billing is enabled |
| `STRIPE_PREMIUM_MONTHLY_PRICE_ID` | Stripe Price ID for Premium monthly plan | — | When billing is enabled |
| `STRIPE_PREMIUM_YEARLY_PRICE_ID` | Stripe Price ID for Premium yearly plan | — | When billing is enabled |

## External APIs

| Variable | Description | Default | Required in |
|---|---|---|---|
| `MASSIVE_API_KEY` | API key for the Massive market-data provider | — | Production |
| `MASSIVE_BASE_URL` | Base URL for the Massive API | `https://api.massive.com` | — |
| `MASSIVE_TIMEOUT_SECONDS` | HTTP timeout for Massive API calls | `30` | — |
| `MASSIVE_MAX_RETRIES` | Maximum retries for transient Massive API failures | `2` | — |
| `MASSIVE_RETRY_BACKOFF_SECONDS` | Backoff between retries | `0.5` | — |
| `EARNINGS_API_KEY` | API key for the earnings calendar provider | — | — |

## CORS / Security

| Variable | Description | Default | Required in |
|---|---|---|---|
| `WEB_CORS_ORIGINS_RAW` | Comma-separated allowed CORS origins | `http://localhost:3000` | Production |
| `API_ALLOWED_HOSTS_RAW` | Comma-separated allowed Host header values | `localhost,127.0.0.1` | Production |
| `REQUEST_MAX_BODY_BYTES` | Maximum request body size in bytes | `1048576` | — |
| `TRUSTED_PROXY_CIDRS` | Comma-separated CIDRs for trusted reverse proxies | RFC 1918 ranges | — |
| `IP_HASH_SALT` | Salt for hashing client IPs in logs/rate limiting (>= 16 chars) | Placeholder (must change in production) | Production |

### IP_HASH_SALT

**Required in production.** HMAC key used to hash client IP addresses for
audit trail storage. The default value is insecure and must be replaced with
a unique random secret.

Generate a value: `python -c "import secrets; print(secrets.token_hex(32))"`

Failure to set this in production will generate a startup warning.

## Rate Limiting

| Variable | Default | Description |
|----------|---------|-------------|
| `RATE_LIMIT_PREFIX` | `bff:rate-limit` | Redis key prefix for rate limit counters |
| `RATE_LIMIT_FAIL_CLOSED` | `true` | If true, immediately return `503 Service Unavailable` when Redis is unavailable or a Redis rate-limit operation fails |
| `RATE_LIMIT_DEGRADED_MEMORY_FALLBACK` | `false` | Optional degraded mode for `RATE_LIMIT_FAIL_CLOSED=false`: use per-process in-memory counters with a halved effective limit when Redis rate limiting is unavailable |
| `RATE_LIMIT_MEMORY_MAX_KEYS` | `10000` | Max in-memory rate limit keys when Redis is down |
| `RATE_LIMIT_WINDOW_SECONDS` | `60` | Default sliding window for rate limits |
| `BACKTEST_CREATE_RATE_LIMIT` | `10` | Max backtest creations per window |
| `SCAN_CREATE_RATE_LIMIT` | `6` | Max scan creations per window |
| `SWEEP_CREATE_RATE_LIMIT` | `3` | Max sweep creations per window |
| `EXPORT_CREATE_RATE_LIMIT` | `20` | Max export creations per window |
| `SSE_RATE_LIMIT` | `30` | Max SSE connection attempts per window |

When `RATE_LIMIT_FAIL_CLOSED=true`, the API returns 503 if Redis is unreachable. When `false` (default), it falls back to in-memory counting which is per-process only.

| Variable | Description | Default | Required in |
|---|---|---|---|
| `BILLING_CREATE_RATE_LIMIT` | Max billing requests per window | `10` | — |
| `TEMPLATE_MUTATE_RATE_LIMIT` | Max template mutation requests per window | `20` | — |
| `ANALYSIS_CREATE_RATE_LIMIT` | Max deep analysis requests per window | `10` | — |
| `ANALYSIS_RATE_LIMIT_WINDOW_SECONDS` | Window for analysis rate limit | `3600` | — |
| `FORECAST_RATE_LIMIT` | Max forecast requests per window | `6` | — |
| `DAILY_PICKS_RATE_LIMIT` | Max daily picks requests per window | `30` | — |
| `SSE_REDIS_MAX_CONNECTIONS` | Maximum Redis connections for SSE pub/sub pool | `50` | — |
| `SSE_REDIS_SOCKET_TIMEOUT` | Socket timeout (seconds) for SSE Redis connections | `10.0` | — |
| `SSE_REDIS_CONNECT_TIMEOUT` | Connect timeout (seconds) for SSE Redis connections | `5.0` | — |

## Pipeline / Backtesting

| Variable | Description | Default | Required in |
|---|---|---|---|
| `PIPELINE_MAX_WORKERS` | Thread pool size for nightly pipeline execution | `20` | — |
| `PIPELINE_DEFAULT_SYMBOLS_CSV` | Comma-separated override for default nightly pipeline symbols | Built-in list of ~100 tickers | — |
| `SCAN_TIMEOUT_SECONDS` | Maximum wall-clock time for a single scan job | `540` | — |
| `RISK_FREE_RATE` | Annual risk-free rate used in Sharpe/Sortino calculations | `0.045` | — |
| `MAX_BACKTEST_WINDOW_DAYS` | Maximum backtest date range in days | `1825` | — |
| `MAX_SCANNER_WINDOW_DAYS` | Maximum scanner date range in days | `730` | — |

## Observability

| Variable | Description | Default | Required in |
|---|---|---|---|
| `METRICS_TOKEN` | Bearer token required to scrape `/metrics` endpoint | — | Production |

## S3 / Object Storage

| Variable | Description | Default | Required in |
|---|---|---|---|
| `S3_BUCKET` | S3 bucket name for export file storage | — | When exports are enabled |
| `S3_REGION` | AWS region | — | When exports are enabled |
| `S3_ENDPOINT_URL` | Custom S3 endpoint (for MinIO / R2 / etc.) | — | — |
| `AWS_ACCESS_KEY_ID` | AWS access key | — | When exports are enabled |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | — | When exports are enabled |

## Web (Next.js)

| Variable | Description | Default | Required in |
|---|---|---|---|
| `NEXT_PUBLIC_APP_URL` | Public URL of the web app (used in links/redirects) | `http://localhost:3000` | Production |
| `NEXT_PUBLIC_API_BASE_URL` | API base URL used by the frontend client | `http://localhost:8000` | Production |
| `NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY` | Clerk publishable key for frontend auth | — | Production |
| `NEXT_PUBLIC_CLERK_SIGN_IN_URL` | Path to the sign-in page | `/sign-in` | — |
| `NEXT_PUBLIC_CLERK_SIGN_IN_FALLBACK_REDIRECT_URL` | Redirect after sign-in | `/app/dashboard` | — |
| `NEXT_PUBLIC_CLERK_SIGN_UP_FALLBACK_REDIRECT_URL` | Redirect after sign-up | `/app/dashboard` | — |
| `CSP_REPORT_URI` | Endpoint for Content-Security-Policy violation reports | — | — |

## CI/CD

| Variable | Description | Default | Required in |
|---|---|---|---|
| `API_PRODUCTION_URL` | GitHub Actions variable — production API URL for post-deploy smoke tests | — | CD workflow |
| `ROLLBACK_TAG` | GitHub Actions variable — image tag to roll back to on smoke failure | — | CD workflow |
| `ENABLE_E2E_TESTS` | Set to `true` to enable Playwright E2E tests in CI | — | CI workflow |
| `CLERK_TESTING_TOKEN` | GitHub Actions secret — Clerk testing token for E2E auth flows | — | CI workflow (E2E) |
