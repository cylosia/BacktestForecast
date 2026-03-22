import { NextResponse } from "next/server";

import { API_HEALTH_LIVE_PATH, WEB_PACKAGE_VERSION, WEB_SERVICE_NAME } from "@/lib/build-info";

const API_BASE = process.env.API_INTERNAL_BASE_URL ?? process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";

export async function GET() {
  let backendStatus = "unknown";
  try {
    const resp = await fetch(`${API_BASE.replace(/\/+$/, "")}${API_HEALTH_LIVE_PATH}`, {
      signal: AbortSignal.timeout(3000),
      cache: "no-store",
    });
    backendStatus = resp.ok ? "up" : "degraded";
  } catch {
    backendStatus = "down";
  }
  const status = backendStatus === "up" ? "ok" : "degraded";
  return NextResponse.json({ status, service: WEB_SERVICE_NAME, version: WEB_PACKAGE_VERSION, backend: backendStatus });
}
