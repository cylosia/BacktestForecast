> **Current Status:** SSE endpoints are fully implemented on the backend but the frontend uses polling (`usePolling` hook) exclusively. SSE exists as an upgrade path to reduce polling overhead.

# Server-Sent Events (SSE) Middleware Buffering

## Limitation

Starlette's `BaseHTTPMiddleware` **buffers the entire response** before sending it to the client. This breaks streaming responses such as Server-Sent Events (SSE), because:

- SSE relies on incremental delivery of chunks
- The middleware reads the full response body into memory before forwarding
- Clients receive no data until the stream completes (or times out)

## Affected Middleware

Four middleware classes use `BaseHTTPMiddleware` and therefore buffer responses:

1. `RequestContextMiddleware` (observability/logging)
2. `RequestBodyLimitMiddleware` (security/http)
3. `ApiSecurityHeadersMiddleware` (security/http)
4. `PrometheusMiddleware` (observability/metrics)

## Workaround: Reverse Proxy

For reverse proxies (e.g., nginx, Caddy), set:

```
X-Accel-Buffering: no
```

This disables buffering on the proxy side so that SSE chunks are forwarded to the client as they arrive. The API response itself may still be buffered by the middleware, but the proxy will not add additional buffering.

## Long-Term Fix

Convert affected middleware to **pure ASGI middleware** that does not wrap the response in a way that reads the entire body. Pure ASGI middleware receives the response as a streaming callable and can forward chunks without buffering.

## Connection Limits

- **Per process**: 200 simultaneous SSE connections (`SSE_MAX_CONNECTIONS_PROCESS`)
- **Per user**: 10 connections (`SSE_MAX_CONNECTIONS_PER_USER`), enforced via Redis or in-process fallback when Redis is unavailable

With N uvicorn workers, the effective server-wide limit is N × 200.

## Timeout and Heartbeats

- **Stream timeout**: 300 seconds
- **Heartbeat interval**: 15 seconds — the server sends a comment or empty event to keep the connection alive during quiet periods

## Redis Pool Management and Health Checks

The SSE layer uses a lazily-initialised shared async Redis connection pool for per-user slot counting and Pub/Sub. Pool health is validated via `ping()` at most once every 60 seconds; on failure, the pool is recreated. All checks and mutations are done under a lock to prevent races between concurrent coroutines. The pool is closed on application shutdown via `shutdown_async_redis()`.
