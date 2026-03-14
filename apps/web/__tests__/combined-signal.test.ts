import { describe, it, expect, vi } from "vitest";

/**
 * combinedSignal creates a derived AbortSignal that aborts when ANY source
 * signal aborts. This tests the fallback (non-AbortSignal.any) path that
 * manually wires event listeners.
 */
function combinedSignal(...signals: AbortSignal[]): { signal: AbortSignal; cleanup: () => void } {
  const controller = new AbortController();
  const handlers: Array<[AbortSignal, () => void]> = [];
  for (const sig of signals) {
    if (sig.aborted) {
      controller.abort();
      break;
    }
    const handler = () => controller.abort();
    sig.addEventListener("abort", handler);
    handlers.push([sig, handler]);
  }
  const cleanup = () => {
    for (const [sig, handler] of handlers) {
      sig.removeEventListener("abort", handler);
    }
  };
  return { signal: controller.signal, cleanup };
}

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

  it("cleanup removes all listeners from source signals", () => {
    const ac1 = new AbortController();
    const ac2 = new AbortController();
    const removeSpy1 = vi.spyOn(ac1.signal, "removeEventListener");
    const removeSpy2 = vi.spyOn(ac2.signal, "removeEventListener");

    const { cleanup } = combinedSignal(ac1.signal, ac2.signal);
    cleanup();

    expect(removeSpy1).toHaveBeenCalledWith("abort", expect.any(Function));
    expect(removeSpy2).toHaveBeenCalledWith("abort", expect.any(Function));
  });

  it("does not leak listeners after cleanup", () => {
    const ac1 = new AbortController();
    const ac2 = new AbortController();
    const addSpy = vi.spyOn(ac1.signal, "addEventListener");
    const removeSpy = vi.spyOn(ac1.signal, "removeEventListener");

    const { cleanup } = combinedSignal(ac1.signal, ac2.signal);
    cleanup();

    const addCount = addSpy.mock.calls.filter(([event]) => event === "abort").length;
    const removeCount = removeSpy.mock.calls.filter(([event]) => event === "abort").length;
    expect(removeCount).toBe(addCount);
  });
});
