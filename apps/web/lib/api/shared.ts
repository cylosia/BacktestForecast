import { env } from "@/lib/env";

export interface ApiErrorPayload {
  error?: {
    code: string;
    message: string;
    request_id?: string;
    detail?: {
      current_tier?: string;
      required_tier?: string;
    };
    details?: Array<{ loc?: string[]; msg?: string; type?: string }>;
  };
}

const API_BASE = (
  typeof window === "undefined" && process.env.API_INTERNAL_BASE_URL
    ? process.env.API_INTERNAL_BASE_URL
    : env.apiBaseUrl
).replace(/\/+$/, "");
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
  const onAbort = (event: Event) => {
    const signal = event.target as AbortSignal;
    controller.abort(signal.reason);
    detach();
  };
  for (const sig of [userSignal, timeoutSignal]) {
    if (sig.aborted) { controller.abort(); detach(); return { signal: controller.signal, cleanup: () => {} }; }
    sig.addEventListener("abort", onAbort, { once: true });
  }
  return { signal: controller.signal, cleanup: detach };
}

export interface QuotaErrorDetail {
  current_tier?: string;
  required_tier?: string;
}

export interface ValidationFieldError {
  loc?: string[];
  msg?: string;
  type?: string;
}

export class ApiError extends Error {
  status: number;
  code?: string;
  requestId?: string;
  detail?: QuotaErrorDetail;
  fieldErrors?: ValidationFieldError[];

  constructor(
    message: string,
    status: number,
    code?: string,
    requestId?: string,
    detail?: QuotaErrorDetail,
    fieldErrors?: ValidationFieldError[],
  ) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
    this.requestId = requestId;
    this.detail = detail;
    this.fieldErrors = fieldErrors;
  }

  get currentTier(): string | undefined {
    return this.detail?.current_tier;
  }

  get requiredTier(): string | undefined {
    return this.detail?.required_tier;
  }

  get isQuotaError(): boolean {
    return this.code === "quota_exceeded" || this.code === "feature_locked";
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

  if (response.status === 401 && typeof window !== "undefined") {
    const path = window.location.pathname + window.location.search;
    const isValidRedirect = path.startsWith("/") && !path.includes("//") && !path.includes("\\");
    const returnTo = isValidRedirect ? encodeURIComponent(path) : "";
    window.location.href = returnTo ? `/sign-in?redirect_url=${returnTo}` : "/sign-in";
    await new Promise<never>(() => {});
  }

  throw new ApiError(
    payload?.error?.message ?? fallback.message,
    response.status,
    payload?.error?.code ?? fallback.code,
    payload?.error?.request_id,
    payload?.error?.detail,
    payload?.error?.details,
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
    payload?.error?.detail,
    payload?.error?.details,
  );
}

export async function apiRequest<T>(path: string, token: string, init?: RequestInit & { timeoutMs?: number }): Promise<T> {
  const controller = new AbortController();
  let timedOut = false;
  const effectiveTimeout = init?.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const timeout = setTimeout(() => { timedOut = true; controller.abort(); }, effectiveTimeout);
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
      return undefined as T & (void extends T ? undefined : never);
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

/**
 * Like {@link apiRequest} but validates the response shape at runtime.
 *
 * @param validator - receives the parsed JSON and must throw if the shape is
 *   wrong (e.g. missing required fields). Return the (possibly narrowed)
 *   value.  A simple guard: `(d) => { if (!d?.id) throw new Error('missing id'); return d as Foo; }`
 */
export async function validatedApiRequest<T>(
  path: string,
  token: string,
  validator: (data: unknown) => T,
  init?: RequestInit & { timeoutMs?: number },
): Promise<T> {
  const raw: unknown = await apiRequest<unknown>(path, token, init);
  try {
    return validator(raw);
  } catch (err) {
    const detail = err instanceof Error ? err.message : String(err);
    throw new ApiError(
      `Unexpected response shape from ${path}: ${detail}`,
      0,
      "response_validation_error",
    );
  }
}

export async function apiDownload(path: string, token: string, init?: RequestInit & { timeoutMs?: number }): Promise<Response> {
  const controller = new AbortController();
  let timedOut = false;
  const effectiveTimeout = init?.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const timeout = setTimeout(() => { timedOut = true; controller.abort(); }, effectiveTimeout);
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
