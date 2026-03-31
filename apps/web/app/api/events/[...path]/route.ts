import { auth } from "@clerk/nextjs/server";
import { NextRequest } from "next/server";

import { isAllowedSseProxyOrigin } from "@/lib/api/sse-origin";

const API_BASE = (process.env.API_INTERNAL_BASE_URL ?? process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000").replace(/\/+$/, "");

function isAllowedOrigin(req: NextRequest): boolean {
  // This proxy is only used by browser EventSource requests from the web app.
  // Require explicit Origin/Referer evidence so a future auth or cookie change
  // cannot silently widen who may open authenticated streams through this route.
  return isAllowedSseProxyOrigin(req.headers);
}

export async function GET(req: NextRequest, { params }: { params: Promise<{ path: string[] }> }) {
  if (!isAllowedOrigin(req)) {
    return new Response("Forbidden", { status: 403 });
  }

  const { getToken } = await auth();
  const token = await getToken();
  if (!token) {
    return new Response("Unauthorized", { status: 401 });
  }

  const { path } = await params;
  const ALLOWED_RESOURCE_TYPES = new Set([
    "backtests",
    "multi_symbol_backtests",
    "multi_step_backtests",
    "scans",
    "exports",
    "analyses",
    "sweeps",
  ]);
  if (!path.length || !ALLOWED_RESOURCE_TYPES.has(path[0])) {
    return new Response("Invalid resource type", { status: 400 });
  }
  if (path.length !== 2) {
    return new Response("Invalid event path", { status: 400 });
  }
  if (path.some(segment => segment === "." || segment === ".." || segment.includes("/"))) {
    return new Response("Invalid path segment", { status: 400 });
  }
  const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  if (!UUID_RE.test(path[1])) {
    return new Response("Invalid resource ID", { status: 400 });
  }
  const backendPath = `/v1/events/${path[0]}/${path[1]}`;
  const backendUrl = `${API_BASE}${backendPath}`;

  try {
    const backendResponse = await fetch(backendUrl, {
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "text/event-stream",
      },
      cache: "no-store",
      signal: req.signal,
    });

    if (!backendResponse.ok) {
      const safe = backendResponse.status === 401 ? "Unauthorized"
        : backendResponse.status === 403 ? "Forbidden"
        : backendResponse.status === 404 ? "Not found"
        : "Upstream error";
      return new Response(safe, { status: backendResponse.status });
    }

    if (!backendResponse.body) {
      return new Response("No stream body", { status: 502 });
    }

    return new Response(backendResponse.body, {
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache, no-transform",
        Connection: "keep-alive",
        "X-Accel-Buffering": "no",
        "X-Content-Type-Options": "nosniff",
        "Content-Security-Policy": "default-src 'none'",
        "X-Robots-Tag": "noindex",
      },
    });
  } catch (err) {
    if (err instanceof DOMException && err.name === "AbortError") {
      return new Response("Client disconnected", { status: 499 });
    }
    return new Response("SSE proxy error", { status: 502 });
  }
}

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
