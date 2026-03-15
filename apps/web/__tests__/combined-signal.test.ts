import { describe, it, expect, vi } from "vitest";

vi.mock("@/lib/env", () => ({
  env: {
    appUrl: "http://localhost:3000",
    apiBaseUrl: "http://localhost:8000",
    clerkPublishableKey: "pk_test_fake",
  },
}));

import { combinedSignal } from "@/lib/api/shared";

describe("combinedSignal", () => {
  it("aborts when the first source signal aborts", () => {
    const ac1 = new AbortController();
    const ac2 = new AbortController();
    const { signal, cleanup } = combinedSignal(ac1.signal, ac2.signal);

    expect(signal.aborted).toBe(false);
    ac1.abort();
    expect(signal.aborted).toBe(true);
    cleanup();
  });

  it("aborts when the second source signal aborts", () => {
    const ac1 = new AbortController();
    const ac2 = new AbortController();
    const { signal, cleanup } = combinedSignal(ac1.signal, ac2.signal);

    ac2.abort();
    expect(signal.aborted).toBe(true);
    cleanup();
  });

  it("is immediately aborted if a source signal is already aborted", () => {
    const ac1 = new AbortController();
    ac1.abort();
    const ac2 = new AbortController();
    const { signal, cleanup } = combinedSignal(ac1.signal, ac2.signal);

    expect(signal.aborted).toBe(true);
    cleanup();
  });

  it("cleanup does not throw even when called multiple times", () => {
    const ac1 = new AbortController();
    const ac2 = new AbortController();
    const { cleanup } = combinedSignal(ac1.signal, ac2.signal);

    expect(() => {
      cleanup();
      cleanup();
    }).not.toThrow();
  });

  it("does not abort combined signal when sources remain active", () => {
    const ac1 = new AbortController();
    const ac2 = new AbortController();
    const { signal, cleanup } = combinedSignal(ac1.signal, ac2.signal);

    expect(signal.aborted).toBe(false);
    cleanup();
  });
});
