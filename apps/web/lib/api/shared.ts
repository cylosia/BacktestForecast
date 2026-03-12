import { env } from "@/lib/env";
import type { ApiErrorPayload } from "@/lib/backtests/types";

const API_BASE = env.apiBaseUrl.replace(/\/+$/, "");
const DEFAULT_TIMEOUT_MS = 30_000;

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
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), DEFAULT_TIMEOUT_MS);
  try {
    const response = await fetch(`${API_BASE}${path}`, {
      ...init,
      headers: buildHeaders(token, init),
      cache: init?.cache ?? "no-store",
      signal: init?.signal
        ? AbortSignal.any([init.signal, controller.signal])
        : controller.signal,
    });

    if (!response.ok) {
      await parseApiError(response);
    }

    if (response.status === 204) {
      return undefined as unknown as T;
    }

    try {
      return (await response.json()) as T;
    } catch {
      throw new ApiError("Received an invalid response from the server.", response.status);
    }
  } catch (err) {
    if (err instanceof DOMException && err.name === "AbortError") {
      throw new ApiError("The request timed out.", 0, "timeout");
    }
    throw err;
  } finally {
    clearTimeout(timeout);
  }
}

export async function apiDownload(path: string, token: string, init?: RequestInit): Promise<Response> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), DEFAULT_TIMEOUT_MS);
  try {
    const response = await fetch(`${API_BASE}${path}`, {
      ...init,
      headers: buildHeaders(token, init),
      cache: "no-store",
      signal: init?.signal
        ? AbortSignal.any([init.signal, controller.signal])
        : controller.signal,
    });

    if (!response.ok) {
      await parseApiError(response);
    }

    return response;
  } catch (err) {
    if (err instanceof DOMException && err.name === "AbortError") {
      throw new ApiError("The request timed out.", 0, "timeout");
    }
    throw err;
  } finally {
    clearTimeout(timeout);
  }
}
