import { env } from "@/lib/env";

export interface ApiErrorPayload {
  error?: {
    code?: string;
    message?: string;
    request_id?: string;
  };
}

const API_BASE = env.apiBaseUrl.replace(/\/+$/, "");
const DEFAULT_TIMEOUT_MS = 30_000;

export function combinedSignal(userSignal: AbortSignal, timeoutSignal: AbortSignal): { signal: AbortSignal; cleanup: () => void } {
  if (typeof AbortSignal.any === "function") {
    return { signal: AbortSignal.any([userSignal, timeoutSignal]), cleanup: () => {} };
  }
  const controller = new AbortController();
  const detach = () => {
    userSignal.removeEventListener("abort", onAbort);
    timeoutSignal.removeEventListener("abort", onAbort);
  };
  const onAbort = () => {
    controller.abort();
    detach();
  };
  for (const sig of [userSignal, timeoutSignal]) {
    if (sig.aborted) { controller.abort(); detach(); return { signal: controller.signal, cleanup: () => {} }; }
    sig.addEventListener("abort", onAbort, { once: true });
  }
  return { signal: controller.signal, cleanup: detach };
}

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

const KNOWN_STATUS_DEFAULTS: Record<number, { message: string; code: string }> = {
  400: { message: "The request was invalid. Please check your input and try again.", code: "bad_request" },
  401: { message: "Your session has expired. Please sign in again.", code: "authentication_error" },
  403: { message: "You don't have permission to access this resource.", code: "authorization_error" },
  404: { message: "The requested resource was not found.", code: "not_found" },
  409: { message: "This action conflicts with the current state. Please refresh and try again.", code: "conflict" },
  422: { message: "The submitted data could not be processed. Please review your input.", code: "validation_error" },
  429: { message: "Too many requests. Please try again later.", code: "rate_limited" },
  500: { message: "An unexpected server error occurred. Please try again later.", code: "server_error" },
  502: { message: "The server is temporarily unreachable. Please try again shortly.", code: "bad_gateway" },
  503: { message: "The service is temporarily unavailable. Please try again shortly.", code: "service_unavailable" },
};

async function handleKnownStatus(response: Response): Promise<void> {
  const fallback = KNOWN_STATUS_DEFAULTS[response.status];
  if (!fallback) return;
  let payload: ApiErrorPayload | undefined;
  try {
    payload = (await response.json()) as ApiErrorPayload;
  } catch {
    payload = undefined;
  }
  throw new ApiError(
    payload?.error?.message ?? fallback.message,
    response.status,
    payload?.error?.code ?? fallback.code,
    payload?.error?.request_id,
  );
}

function headersToRecord(headers: HeadersInit | undefined): Record<string, string> {
  if (!headers) return {};
  if (headers instanceof Headers) {
    const record: Record<string, string> = {};
    headers.forEach((value, key) => { record[key] = value; });
    return record;
  }
  if (Array.isArray(headers)) {
    const record: Record<string, string> = {};
    for (const [key, value] of headers) {
      record[key] = value;
    }
    return record;
  }
  return { ...headers };
}

const STRIPPED_HEADERS = new Set(["authorization", "content-type"]);

function buildHeaders(token: string, init?: RequestInit): HeadersInit {
  const custom = headersToRecord(init?.headers);
  const safe: Record<string, string> = {};
  for (const [key, value] of Object.entries(custom)) {
    if (!STRIPPED_HEADERS.has(key.toLowerCase())) {
      safe[key] = value;
    }
  }
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
  let timedOut = false;
  const timeout = setTimeout(() => { timedOut = true; controller.abort(); }, DEFAULT_TIMEOUT_MS);
  const combined = init?.signal
    ? combinedSignal(init.signal, controller.signal)
    : null;
  try {
    const response = await fetch(`${API_BASE}${path}`, {
      ...init,
      headers: buildHeaders(token, init),
      cache: init?.cache ?? "no-store",
      signal: combined?.signal ?? controller.signal,
    });

    if (!response.ok) {
      if (response.status in KNOWN_STATUS_DEFAULTS) {
        await handleKnownStatus(response);
      }
      await parseApiError(response);
    }

    if (response.status === 204) {
      return undefined as T;
    }

    try {
      return (await response.json()) as T;
    } catch {
      throw new ApiError("Received an invalid response from the server.", response.status);
    }
  } catch (err) {
    if (timedOut || (err instanceof DOMException && err.name === "TimeoutError")) {
      throw new ApiError("The request timed out.", 0, "timeout");
    }
    if (err instanceof DOMException && err.name === "AbortError") {
      throw new ApiError("The request was cancelled.", 0, "abort");
    }
    throw err;
  } finally {
    clearTimeout(timeout);
    combined?.cleanup();
  }
}

export async function apiDownload(path: string, token: string, init?: RequestInit): Promise<Response> {
  const controller = new AbortController();
  let timedOut = false;
  const timeout = setTimeout(() => { timedOut = true; controller.abort(); }, DEFAULT_TIMEOUT_MS);
  const combined = init?.signal
    ? combinedSignal(init.signal, controller.signal)
    : null;
  try {
    const response = await fetch(`${API_BASE}${path}`, {
      ...init,
      headers: buildHeaders(token, init),
      cache: "no-store",
      signal: combined?.signal ?? controller.signal,
    });

    if (!response.ok) {
      if (response.status in KNOWN_STATUS_DEFAULTS) {
        await handleKnownStatus(response);
      }
      await parseApiError(response);
    }

    const contentType = response.headers.get("content-type") ?? "";
    if (contentType.includes("application/json")) {
      const errorPayload = await response.json() as ApiErrorPayload;
      throw new ApiError(
        errorPayload?.error?.message ?? "Unexpected JSON response for download.",
        response.status,
        errorPayload?.error?.code,
      );
    }

    return response;
  } catch (err) {
    if (timedOut || (err instanceof DOMException && err.name === "TimeoutError")) {
      throw new ApiError("The request timed out.", 0, "timeout");
    }
    if (err instanceof DOMException && err.name === "AbortError") {
      throw new ApiError("The request was cancelled.", 0, "abort");
    }
    throw err;
  } finally {
    clearTimeout(timeout);
    combined?.cleanup();
  }
}
