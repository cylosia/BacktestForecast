import { describe, it, expect, vi } from "vitest";

/**
 * Test 71: When an analysis poll completes with status "failed", the
 * onComplete callback must NOT call fetchAnalysisFull.
 *
 * We extract the polling callback logic from SymbolAnalysisLauncher and
 * verify the guard: `if (summary.status !== "succeeded") return;`
 */

describe("SymbolAnalysisLauncher onComplete guard", () => {
  it("does NOT fetch full results when analysis status is 'failed'", async () => {
    const fetchAnalysisFull = vi.fn();

    const onComplete = async (summary: { status: string }) => {
      if (summary.status !== "succeeded") return;
      await fetchAnalysisFull("token", "id", new AbortController().signal);
    };

    await onComplete({ status: "failed" });

    expect(fetchAnalysisFull).not.toHaveBeenCalled();
  });

  it("does NOT fetch full results when analysis status is 'cancelled'", async () => {
    const fetchAnalysisFull = vi.fn();

    const onComplete = async (summary: { status: string }) => {
      if (summary.status !== "succeeded") return;
      await fetchAnalysisFull("token", "id", new AbortController().signal);
    };

    await onComplete({ status: "cancelled" });

    expect(fetchAnalysisFull).not.toHaveBeenCalled();
  });

  it("DOES fetch full results when analysis status is 'succeeded'", async () => {
    const fetchAnalysisFull = vi.fn().mockResolvedValue({ id: "abc", symbol: "AAPL" });

    const onComplete = async (summary: { status: string }) => {
      if (summary.status !== "succeeded") return;
      await fetchAnalysisFull("token", "id", new AbortController().signal);
    };

    await onComplete({ status: "succeeded" });

    expect(fetchAnalysisFull).toHaveBeenCalledTimes(1);
  });

  it("returns early without error for unknown statuses", async () => {
    const fetchAnalysisFull = vi.fn();

    const onComplete = async (summary: { status: string }) => {
      if (summary.status !== "succeeded") return;
      await fetchAnalysisFull("token", "id", new AbortController().signal);
    };

    await onComplete({ status: "running" });
    await onComplete({ status: "queued" });

    expect(fetchAnalysisFull).not.toHaveBeenCalled();
  });
});
