import { env } from "@/lib/env";
import type { ApiErrorPayload } from "@/lib/backtests/types";

export class ApiError extends Error {
  status: number;
  code?: string;
  requestId?: string;

  constructor(message: string, status: number, code?: string, requestId?: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
    this.requestId = requestId;
  }
}

function buildHeaders(token: string, init?: RequestInit): HeadersInit {
  const custom = (init?.headers ?? {}) as Record<string, string>;
  const { Authorization: _a, "Content-Type": _ct, ...safe } = custom;
  return {
    ...(init?.body instanceof FormData ? {} : { "Content-Type": "application/json" }),
    Authorization: `Bearer ${token}`,
    ...safe,
  };
}

async function parseApiError(response: Response): Promise<never> {
  let payload: ApiErrorPayload | undefined;

  try {
    payload = (await response.json()) as ApiErrorPayload;
  } catch {
    payload = undefined;
  }

  throw new ApiError(
    payload?.error?.message ?? "The request could not be completed.",
    response.status,
    payload?.error?.code,
    payload?.error?.request_id,
  );
}

export async function apiRequest<T>(path: string, token: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${env.apiBaseUrl}${path}`, {
    ...init,
    headers: buildHeaders(token, init),
    cache: init?.cache ?? "no-store",
  });

  if (!response.ok) {
    await parseApiError(response);
  }

  if (response.status === 204) {
    return undefined as T;
  }

  return (await response.json()) as T;
}

export async function apiDownload(path: string, token: string, init?: RequestInit): Promise<Response> {
  const response = await fetch(`${env.apiBaseUrl}${path}`, {
    ...init,
    headers: buildHeaders(token, init),
    cache: "no-store",
  });

  if (!response.ok) {
    await parseApiError(response);
  }

  return response;
}
