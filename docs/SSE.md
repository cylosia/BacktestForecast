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
