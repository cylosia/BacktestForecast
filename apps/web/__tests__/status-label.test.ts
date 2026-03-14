import { describe, it, expect } from "vitest";
import { statusLabel, isTerminalStatus } from "@/lib/backtests/format";

describe("statusLabel", () => {
  it('returns "Expired" for the "expired" status', () => {
    expect(statusLabel("expired")).toBe("Expired");
  });

  it('returns "Queued" for the "queued" status', () => {
    expect(statusLabel("queued")).toBe("Queued");
  });

  it('returns "Running" for the "running" status', () => {
    expect(statusLabel("running")).toBe("Running");
  });

  it('returns "Completed" for the "succeeded" status', () => {
    expect(statusLabel("succeeded")).toBe("Completed");
  });

  it('returns "Failed" for the "failed" status', () => {
    expect(statusLabel("failed")).toBe("Failed");
  });

  it('returns "Cancelled" for the "cancelled" status', () => {
    expect(statusLabel("cancelled")).toBe("Cancelled");
  });

  it("returns the raw string for unknown statuses", () => {
    expect(statusLabel("unknown_status")).toBe("unknown_status");
  });
});

describe("isTerminalStatus", () => {
  it("recognizes succeeded as terminal", () => {
    expect(isTerminalStatus("succeeded")).toBe(true);
  });

  it("recognizes failed as terminal", () => {
    expect(isTerminalStatus("failed")).toBe(true);
  });

  it("recognizes cancelled as terminal", () => {
    expect(isTerminalStatus("cancelled")).toBe(true);
  });

  it("recognizes expired as terminal", () => {
    expect(isTerminalStatus("expired")).toBe(true);
  });

  it("does not treat queued as terminal", () => {
    expect(isTerminalStatus("queued")).toBe(false);
  });

  it("does not treat running as terminal", () => {
    expect(isTerminalStatus("running")).toBe(false);
  });
});
