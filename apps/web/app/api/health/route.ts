import { NextResponse } from "next/server";

const API_BASE = process.env.API_INTERNAL_BASE_URL ?? process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";

export async function GET() {
  let backendStatus = "unknown";
  try {
    const resp = await fetch(`${API_BASE.replace(/\/+$/, "")}/health/live`, {
      signal: AbortSignal.timeout(3000),
      cache: "no-store",
    });
    backendStatus = resp.ok ? "up" : "degraded";
  } catch {
    backendStatus = "down";
  }
  const status = backendStatus === "up" ? "ok" : "degraded";
  return NextResponse.json({ status, service: "web", backend: backendStatus });
}
