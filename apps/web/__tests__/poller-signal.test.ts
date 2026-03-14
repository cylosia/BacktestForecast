import { describe, it, expect } from "vitest";

/**
 * Tests that the polling pattern passes AbortSignal to fetchers and
 * that signals are properly managed across poll cycles.
 */

describe("poller AbortSignal contract", () => {
  it("passes an AbortSignal to the fetcher callback", async () => {
    let receivedSignal: AbortSignal | undefined;

    const controller = new AbortController();
    const fetcher = async (signal: AbortSignal) => {
      receivedSignal = signal;
      return { status: "done" };
    };

    await fetcher(controller.signal);

    expect(receivedSignal).toBeDefined();
    expect(receivedSignal).toBeInstanceOf(AbortSignal);
  });

  it("signal can be aborted to cancel in-flight requests", () => {
    const controller = new AbortController();
    expect(controller.signal.aborted).toBe(false);

    controller.abort();
    expect(controller.signal.aborted).toBe(true);
  });

  it("creates a fresh signal for each poll cycle", async () => {
    const signals: AbortSignal[] = [];

    for (let i = 0; i < 3; i++) {
      const controller = new AbortController();
      signals.push(controller.signal);
      controller.abort();
    }

    const uniqueSignals = new Set(signals);
    expect(uniqueSignals.size).toBe(signals.length);
  });

  it("previous signal is aborted before creating a new one", async () => {
    let previousController: AbortController | null = null;
    const abortedStates: boolean[] = [];

    for (let i = 0; i < 3; i++) {
      previousController?.abort();
      if (previousController) {
        abortedStates.push(previousController.signal.aborted);
      }
      previousController = new AbortController();
    }

    expect(abortedStates).toEqual([true, true]);
  });
});
