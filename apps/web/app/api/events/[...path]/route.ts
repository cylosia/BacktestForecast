import { auth } from "@clerk/nextjs/server";
import { NextRequest } from "next/server";

const API_BASE = (process.env.API_INTERNAL_BASE_URL ?? process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000").replace(/\/+$/, "");

export async function GET(req: NextRequest, { params }: { params: Promise<{ path: string[] }> }) {
  const { getToken } = await auth();
  const token = await getToken();
  if (!token) {
    return new Response("Unauthorized", { status: 401 });
  }

  const { path } = await params;
  const backendPath = `/v1/events/${path.join("/")}`;
  const backendUrl = `${API_BASE}${backendPath}`;

  try {
    const backendResponse = await fetch(backendUrl, {
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "text/event-stream",
      },
      signal: req.signal,
    });

    if (!backendResponse.ok) {
      return new Response(backendResponse.statusText, { status: backendResponse.status });
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
