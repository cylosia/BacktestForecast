# Server-Sent Events (SSE) Streaming

## Current status

The API path now preserves SSE streaming end-to-end:

- `RequestContextMiddleware` is pure ASGI
- `RequestBodyLimitMiddleware` is pure ASGI
- `ApiSecurityHeadersMiddleware` is pure ASGI
- `PrometheusMiddleware` is pure ASGI
- the Next.js proxy route forwards the backend event stream with `cache: "no-store"`

That means the historical `BaseHTTPMiddleware` buffering limitation no longer applies to the active API/Next SSE path in this repository.

## Reverse proxy guidance

For reverse proxies (for example nginx or Caddy), still disable proxy buffering for SSE responses:

```
X-Accel-Buffering: no
```

This ensures the proxy does not add buffering in front of the already-streaming API response.

## Connection Limits

- **Per process**: 200 simultaneous SSE connections (`SSE_MAX_CONNECTIONS_PROCESS`)
- **Per user**: 10 connections (`SSE_MAX_CONNECTIONS_PER_USER`), enforced via Redis or in-process fallback when Redis is unavailable

With N uvicorn workers, the effective server-wide limit is N × 200.

## Timeout and Heartbeats

- **Stream timeout**: 300 seconds
- **Heartbeat interval**: 15 seconds — the server sends a comment or empty event to keep the connection alive during quiet periods

## Redis Pool Management and Health Checks

The SSE layer uses a lazily-initialised shared async Redis connection pool for per-user slot counting and Pub/Sub. Pool health is validated via `ping()` at most once every 60 seconds; on failure, the pool is recreated. All checks and mutations are done under a lock to prevent races between concurrent coroutines. The pool is closed on application shutdown via `shutdown_async_redis()`.
