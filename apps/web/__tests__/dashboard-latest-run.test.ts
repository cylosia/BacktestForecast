import { describe, it, expect } from "vitest";

type BacktestRunSummary = {
  id: string;
  status: "queued" | "running" | "succeeded" | "failed" | "cancelled";
  created_at: string;
  symbol: string;
};

const TERMINAL_STATUSES = new Set(["succeeded", "failed", "cancelled"]);

function getLatestRun(runs: BacktestRunSummary[]): BacktestRunSummary | null {
  const terminal = runs.filter((r) => TERMINAL_STATUSES.has(r.status));
  if (terminal.length === 0) return null;
  return terminal.sort(
    (a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime(),
  )[0];
}

describe("dashboard latestRun filter", () => {
  it("returns the most recent terminal run", () => {
    const runs: BacktestRunSummary[] = [
      { id: "1", status: "running", created_at: "2025-03-14T10:00:00Z", symbol: "AAPL" },
      { id: "2", status: "succeeded", created_at: "2025-03-13T10:00:00Z", symbol: "MSFT" },
      { id: "3", status: "failed", created_at: "2025-03-12T10:00:00Z", symbol: "TSLA" },
    ];
    const result = getLatestRun(runs);
    expect(result).not.toBeNull();
    expect(result!.id).toBe("2");
    expect(result!.status).toBe("succeeded");
  });

  it("returns null when all runs are non-terminal", () => {
    const runs: BacktestRunSummary[] = [
      { id: "1", status: "queued", created_at: "2025-03-14T10:00:00Z", symbol: "AAPL" },
      { id: "2", status: "running", created_at: "2025-03-13T10:00:00Z", symbol: "MSFT" },
    ];
    const result = getLatestRun(runs);
    expect(result).toBeNull();
  });

  it("returns null for an empty array", () => {
    const result = getLatestRun([]);
    expect(result).toBeNull();
  });

  it("prefers the most recent succeeded over an older failed", () => {
    const runs: BacktestRunSummary[] = [
      { id: "1", status: "failed", created_at: "2025-03-10T10:00:00Z", symbol: "AAPL" },
      { id: "2", status: "succeeded", created_at: "2025-03-12T10:00:00Z", symbol: "MSFT" },
    ];
    const result = getLatestRun(runs);
    expect(result!.id).toBe("2");
  });

  it("skips 'running' even if it is the newest", () => {
    const runs: BacktestRunSummary[] = [
      { id: "1", status: "running", created_at: "2025-03-14T23:59:59Z", symbol: "SPY" },
      { id: "2", status: "succeeded", created_at: "2025-03-14T10:00:00Z", symbol: "QQQ" },
    ];
    const result = getLatestRun(runs);
    expect(result!.id).toBe("2");
  });
});
