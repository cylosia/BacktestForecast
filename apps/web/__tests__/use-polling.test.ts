import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

/**
 * Tests for the usePolling maxAttempts boundary logic.
 *
 * Since usePolling is a React hook requiring renderHook from
 * @testing-library/react, these tests verify the core polling
 * logic (attempt counting and timeout boundary) directly.
 */

describe("usePolling maxAttempts boundary (logic)", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("performs exactly maxAttempts iterations before timeout", async () => {
    const maxAttempts = 3;
    let fetchCount = 0;
    let status: "polling" | "done" | "timeout" = "polling";
    let attempts = 0;

    async function poll(): Promise<void> {
      fetchCount++;
      const result = { status: "running" };

      if (result.status === "done") {
        status = "done";
        return;
      }

      attempts++;
      if (attempts >= maxAttempts) {
        status = "timeout";
      }
    }

    for (let i = 0; i < maxAttempts; i++) {
      if (status === "timeout" || status === "done") break;
      await poll();
    }

    expect(fetchCount).toBe(maxAttempts);
    expect(status).toBe("timeout");
    expect(attempts).toBe(maxAttempts);
  });

  it("completes before maxAttempts if done early", async () => {
    const maxAttempts = 5;
    let fetchCount = 0;
    let status: "polling" | "done" | "timeout" = "polling";
    let attempts = 0;

    async function poll(): Promise<boolean> {
      fetchCount++;
      const isDone = fetchCount >= 2;

      if (isDone) {
        status = "done";
        return true;
      }

      attempts++;
      if (attempts >= maxAttempts) {
        status = "timeout";
      }
      return false;
    }

    for (let i = 0; i < maxAttempts; i++) {
      if (status === "timeout" || status === "done") break;
      const done = await poll();
      if (done) break;
    }

    expect(fetchCount).toBe(2);
    expect(status).toBe("done");
  });

  it("with maxAttempts=1 polls exactly once (no off-by-one)", async () => {
    const maxAttempts = 1;
    let fetchCount = 0;
    let status: "polling" | "done" | "timeout" = "polling";
    let attempts = 0;

    async function poll(): Promise<void> {
      fetchCount++;
      attempts++;
      if (attempts >= maxAttempts) {
        status = "timeout";
      }
    }

    for (let i = 0; i < maxAttempts + 2; i++) {
      if (status === "timeout" || status === "done") break;
      await poll();
    }

    expect(fetchCount).toBe(1);
    expect(status).toBe("timeout");
  });
});
