import { describe, expect, it } from "vitest";
import {
  DEFAULT_SWEEP_FORM_VALUES,
  validateSweepForm,
} from "@/lib/sweeps/validation";
import { MAX_BACKTEST_WINDOW_DAYS } from "@/lib/validation-constants";

describe("validateSweepForm", () => {
  it("rejects a sweep range longer than backend max_backtest_window_days", () => {
    const result = validateSweepForm(
      {
        ...DEFAULT_SWEEP_FORM_VALUES,
        startDate: "2018-01-01",
        endDate: "2024-01-01",
      },
      new Set(["bull_put_credit_spread"]),
    );

    expect(result.error).toBe(
      `Date range cannot exceed 5 years (${MAX_BACKTEST_WINDOW_DAYS} days).`,
    );
  });

  it("accepts a sweep range exactly at the shared max_backtest_window_days limit", () => {
    const result = validateSweepForm(
      {
        ...DEFAULT_SWEEP_FORM_VALUES,
        startDate: "2019-01-01",
        endDate: "2023-12-31",
      },
      new Set(["bull_put_credit_spread"]),
    );

    expect(result.error).toBeUndefined();
    expect(result.symbol).toBe("TSLA");
  });
});
