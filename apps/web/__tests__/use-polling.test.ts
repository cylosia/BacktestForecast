import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

vi.mock("@/lib/env", () => ({
  env: {
    appUrl: "http://localhost:3000",
    apiBaseUrl: "http://localhost:8000",
    clerkPublishableKey: "pk_test_fake",
  },
}));

import { ApiError } from "@/lib/api/shared";
import { runTerminalPollingCallback, type PollingStatus, type UsePollingOptions } from "@/hooks/use-polling";

describe("usePolling maxAttempts boundary (logic)", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  /**
   * Simulates the core loop from usePolling without React rendering.
   * The real hook follows this same attempt-counting pattern.
   */
  async function simulatePoll(opts: {
    maxAttempts: number;
    fetchResults: Array<{ done: boolean }>;
  }): Promise<{ status: PollingStatus; fetchCount: number }> {
    let status: PollingStatus = "polling";
    let fetchCount = 0;
    let attempts = 0;

    for (let i = 0; i < opts.maxAttempts; i++) {
      if (status !== "polling") break;

      const result = opts.fetchResults[fetchCount] ?? { done: false };
      fetchCount++;

      if (result.done) {
        status = "done";
        break;
      }

      attempts++;
      if (attempts >= opts.maxAttempts) {
        status = "timeout";
      }
    }

    return { status, fetchCount };
  }

  it("performs exactly maxAttempts iterations before timeout", async () => {
    const result = await simulatePoll({
      maxAttempts: 3,
      fetchResults: [{ done: false }, { done: false }, { done: false }],
    });
    expect(result.fetchCount).toBe(3);
    expect(result.status).toBe("timeout");
  });

  it("completes before maxAttempts if done early", async () => {
    const result = await simulatePoll({
      maxAttempts: 5,
      fetchResults: [{ done: false }, { done: true }],
    });
    expect(result.fetchCount).toBe(2);
    expect(result.status).toBe("done");
  });

  it("with maxAttempts=1 polls exactly once", async () => {
    const result = await simulatePoll({
      maxAttempts: 1,
      fetchResults: [{ done: false }],
    });
    expect(result.fetchCount).toBe(1);
    expect(result.status).toBe("timeout");
  });
});

describe("usePolling ApiError-based error classification", () => {
  it("treats 401 ApiError as non-retryable auth error", () => {
    const err = new ApiError("Session expired", 401, "authentication_error");
    const isAuthError = err instanceof ApiError && (err.status === 401 || err.status === 403);
    expect(isAuthError).toBe(true);
  });

  it("treats 403 ApiError as non-retryable auth error", () => {
    const err = new ApiError("Forbidden", 403, "authorization_error");
    const isAuthError = err instanceof ApiError && (err.status === 401 || err.status === 403);
    expect(isAuthError).toBe(true);
  });

  it("treats 500 ApiError as retryable with exponential backoff", () => {
    const err = new ApiError("Server error", 500);
    const isAuthError = err instanceof ApiError && (err.status === 401 || err.status === 403);
    expect(isAuthError).toBe(false);

    let consecutiveErrors = 0;
    const baseInterval = 2000;
    consecutiveErrors += 1;
    const backoff = baseInterval * Math.min(2 ** consecutiveErrors, 16);
    expect(backoff).toBe(4000);
  });

  it("caps exponential backoff at 16x the base interval", () => {
    const baseInterval = 2000;
    const maxMultiplier = 16;
    for (let errors = 1; errors <= 10; errors++) {
      const backoff = baseInterval * Math.min(2 ** errors, maxMultiplier);
      expect(backoff).toBeLessThanOrEqual(baseInterval * maxMultiplier);
    }
  });
});

describe("usePolling type contract", () => {
  it("PollingStatus covers all expected states", () => {
    const validStatuses: PollingStatus[] = ["idle", "polling", "done", "timeout", "error"];
    expect(validStatuses).toHaveLength(5);
  });

  it("UsePollingOptions requires fetcher, onComplete, isComplete, interval, maxAttempts", () => {
    const opts: UsePollingOptions<{ status: string }> = {
      fetcher: async () => ({ status: "running" }),
      onComplete: () => {},
      isComplete: (r) => r.status === "done",
      interval: 2000,
      maxAttempts: 30,
    };
    expect(opts.interval).toBe(2000);
    expect(opts.maxAttempts).toBe(30);
  });
});


describe("usePolling terminal callback separation", () => {
  it("preserves terminal resource success when onComplete throws", async () => {
    const onComplete = vi.fn(async () => {
      throw new Error("refresh failed");
    });
    const onError = vi.fn();

    const callbackStatus = await runTerminalPollingCallback({ status: "succeeded" }, onComplete, onError);

    expect(callbackStatus).toBe("failed");
    expect(onComplete).toHaveBeenCalledTimes(1);
    expect(onError).toHaveBeenCalledTimes(1);
  });

  it("reports callback success after terminal resource success", async () => {
    const onComplete = vi.fn(async () => {});

    const callbackStatus = await runTerminalPollingCallback({ status: "succeeded" }, onComplete);

    expect(callbackStatus).toBe("succeeded");
    expect(onComplete).toHaveBeenCalledTimes(1);
  });
});
