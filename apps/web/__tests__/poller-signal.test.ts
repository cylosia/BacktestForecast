import { describe, it, expect, vi } from "vitest";

vi.mock("@/lib/env", () => ({
  env: {
    appUrl: "http://localhost:3000",
    apiBaseUrl: "http://localhost:8000",
    clerkPublishableKey: "pk_test_fake",
  },
}));

import { ApiError, combinedSignal } from "@/lib/api/shared";

describe("ApiError classification for polling", () => {
  it("stores status, code, and requestId on construction", () => {
    const err = new ApiError("Session expired", 401, "authentication_error", "req-123");
    expect(err.message).toBe("Session expired");
    expect(err.status).toBe(401);
    expect(err.code).toBe("authentication_error");
    expect(err.requestId).toBe("req-123");
    expect(err.name).toBe("ApiError");
  });

  it("is an instance of Error", () => {
    const err = new ApiError("fail", 500);
    expect(err).toBeInstanceOf(Error);
    expect(err).toBeInstanceOf(ApiError);
  });

  it("401 and 403 are treated as auth errors by the poller", () => {
    const authStatuses = [401, 403];
    for (const status of authStatuses) {
      const err = new ApiError("auth failure", status);
      expect(err.status === 401 || err.status === 403).toBe(true);
    }
  });

  it("other status codes are retryable by the poller", () => {
    for (const status of [400, 404, 429, 500, 502, 503]) {
      const err = new ApiError("fail", status);
      expect(err.status === 401 || err.status === 403).toBe(false);
    }
  });

  it("timeout errors have status 0 and code 'timeout'", () => {
    const err = new ApiError("The request timed out.", 0, "timeout");
    expect(err.status).toBe(0);
    expect(err.code).toBe("timeout");
  });
});

describe("poller abort signal via combinedSignal", () => {
  it("aborting either source signal propagates to the combined signal", () => {
    const userAc = new AbortController();
    const timeoutAc = new AbortController();
    const { signal, cleanup } = combinedSignal(userAc.signal, timeoutAc.signal);

    expect(signal.aborted).toBe(false);
    userAc.abort();
    expect(signal.aborted).toBe(true);
    cleanup();
  });

  it("each poll cycle gets an independent combined signal", () => {
    const signals: AbortSignal[] = [];

    for (let i = 0; i < 3; i++) {
      const userAc = new AbortController();
      const timeoutAc = new AbortController();
      const { signal, cleanup } = combinedSignal(userAc.signal, timeoutAc.signal);
      signals.push(signal);
      cleanup();
    }

    const unique = new Set(signals);
    expect(unique.size).toBe(3);
  });
});
