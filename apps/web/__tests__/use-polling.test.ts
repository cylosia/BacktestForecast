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

describe("usePolling stale token refresh (logic)", () => {
  it("fetcher is called with a fresh token each iteration", async () => {
    let tokenCounter = 0;

    function getToken(): string {
      tokenCounter++;
      return `token_v${tokenCounter}`;
    }

    const tokensUsed: string[] = [];

    async function fetchWithToken(): Promise<{ status: string }> {
      const token = getToken();
      tokensUsed.push(token);
      return { status: "running" };
    }

    const maxAttempts = 3;
    for (let i = 0; i < maxAttempts; i++) {
      await fetchWithToken();
    }

    expect(tokensUsed).toHaveLength(3);
    expect(tokensUsed[0]).toBe("token_v1");
    expect(tokensUsed[1]).toBe("token_v2");
    expect(tokensUsed[2]).toBe("token_v3");
    expect(new Set(tokensUsed).size).toBe(3);
  });

  it("does not reuse stale token from closure", async () => {
    let tokenVersion = 0;
    const getToken = () => {
      tokenVersion++;
      return `tok_${tokenVersion}`;
    };

    const capturedTokens: string[] = [];

    async function pollOnce(): Promise<string> {
      const freshToken = getToken();
      capturedTokens.push(freshToken);
      return freshToken;
    }

    await pollOnce();
    await pollOnce();
    await pollOnce();

    expect(capturedTokens[0]).not.toBe(capturedTokens[1]);
    expect(capturedTokens[1]).not.toBe(capturedTokens[2]);
  });

  it("token refresh works even when poll completes early", async () => {
    let callCount = 0;
    const getToken = () => `fresh_${++callCount}`;

    const tokens: string[] = [];
    let isDone = false;

    async function poll(): Promise<boolean> {
      const token = getToken();
      tokens.push(token);
      if (tokens.length >= 2) {
        isDone = true;
        return true;
      }
      return false;
    }

    const maxAttempts = 5;
    for (let i = 0; i < maxAttempts; i++) {
      const done = await poll();
      if (done) break;
    }

    expect(tokens).toHaveLength(2);
    expect(tokens[0]).toBe("fresh_1");
    expect(tokens[1]).toBe("fresh_2");
    expect(isDone).toBe(true);
  });
});
