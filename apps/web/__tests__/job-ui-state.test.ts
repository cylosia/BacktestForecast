import { describe, expect, it } from "vitest";
import {
  getBacktestPartialDataMessages,
  getCancellationMessage,
  getComparePartialDataMessages,
  getExportTerminalMessage,
  isTradePayloadPartial,
} from "@/lib/jobs/ui-state";

describe("job ui state helpers", () => {
  it("flags partial trade payloads only when total exceeds returned rows", () => {
    expect(isTradePayloadPartial(10, 5)).toBe(true);
    expect(isTradePayloadPartial(5, 5)).toBe(false);
    expect(isTradePayloadPartial(undefined, 5)).toBe(false);
  });

  it("builds detail partial-data messages for truncated trades and equity", () => {
    const messages = getBacktestPartialDataMessages({
      summary: { trade_count: 12 },
      trades: new Array(5).fill({}),
      equity_curve_truncated: true,
    } as never);

    expect(messages).toHaveLength(2);
    expect(messages[0]).toContain("partial trade list");
    expect(messages[1]).toContain("partial equity curve");
  });

  it("builds compare partial-data messages when compare payload is partial", () => {
    const messages = getComparePartialDataMessages({
      trades_truncated: true,
      items: [{ equity_curve_truncated: true }],
    } as never);

    expect(messages).toHaveLength(2);
    expect(messages[0]).toContain("persisted full-run aggregates");
    expect(messages[1]).toContain("overlay chart");
  });

  it("returns actionable cancellation messages by source", () => {
    expect(getCancellationMessage("scan", "cancelled_by_user")).toContain("cancelled before completion");
    expect(getCancellationMessage("analysis", "subscription_revoked")).toContain("billing access changed");
    expect(getCancellationMessage("backtest", "cancelled_by_support")).toContain("cancelled by support");
  });

  it("distinguishes cancelled and expired exports from generic failures", () => {
    expect(getExportTerminalMessage({ status: "cancelled", error_message: null } as never)).toContain("cancelled");
    expect(getExportTerminalMessage({ status: "expired", error_message: null } as never)).toContain("expired");
    expect(getExportTerminalMessage({ status: "failed", error_message: "boom" } as never)).toBe("boom");
  });
});
