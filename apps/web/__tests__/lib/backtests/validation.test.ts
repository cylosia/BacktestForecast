import { describe, it, expect } from "vitest";
import {
  getDefaultBacktestFormValues,
  validateBacktestForm,
} from "@/lib/backtests/validation";

describe("getDefaultBacktestFormValues", () => {
  it("returns valid ISO date strings for startDate and endDate", () => {
    const defaults = getDefaultBacktestFormValues();

    const isoDatePattern = /^\d{4}-\d{2}-\d{2}$/;
    expect(defaults.startDate).toMatch(isoDatePattern);
    expect(defaults.endDate).toMatch(isoDatePattern);
  });

  it("endDate is today in UTC", () => {
    const defaults = getDefaultBacktestFormValues();
    const now = new Date();
    const todayUTC = now.toISOString().slice(0, 10);
    expect(defaults.endDate).toBe(todayUTC);
  });

  it("startDate is approximately 365 days before endDate", () => {
    const defaults = getDefaultBacktestFormValues();
    const start = new Date(defaults.startDate + "T00:00:00Z");
    const end = new Date(defaults.endDate + "T00:00:00Z");
    const diffMs = end.getTime() - start.getTime();
    const diffDays = diffMs / (1000 * 60 * 60 * 24);
    expect(diffDays).toBeGreaterThanOrEqual(364);
    expect(diffDays).toBeLessThanOrEqual(366);
  });

  it("start and end dates parse to valid Date objects", () => {
    const defaults = getDefaultBacktestFormValues();
    const startDate = new Date(defaults.startDate + "T00:00:00Z");
    const endDate = new Date(defaults.endDate + "T00:00:00Z");
    expect(Number.isNaN(startDate.getTime())).toBe(false);
    expect(Number.isNaN(endDate.getTime())).toBe(false);
  });

  it("endDate is not in the future (UTC)", () => {
    const defaults = getDefaultBacktestFormValues();
    const todayUTC = new Date().toISOString().slice(0, 10);
    expect(defaults.endDate <= todayUTC).toBe(true);
  });
});

describe("validateBacktestForm – zero entry rules", () => {
  it("returns error when both rsiEnabled and movingAverageEnabled are false", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      rsiEnabled: false,
      movingAverageEnabled: false,
    };
    const { errors } = validateBacktestForm(values);
    expect(errors.form).toBeDefined();
    expect(errors.form).toContain("entry rule");
  });

  it("no form error when rsiEnabled is true", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      rsiEnabled: true,
      movingAverageEnabled: false,
    };
    const { errors } = validateBacktestForm(values);
    expect(errors.form).toBeUndefined();
  });

  it("no form error when movingAverageEnabled is true", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      rsiEnabled: false,
      movingAverageEnabled: true,
    };
    const { errors } = validateBacktestForm(values);
    expect(errors.form).toBeUndefined();
  });
});

describe("validateBacktestForm – enabled-but-invalid entry rules", () => {
  it("shows a form error when RSI is enabled but no valid entry rule can be serialized", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      rsiEnabled: true,
      rsiThreshold: "999",
      movingAverageEnabled: false,
    };
    const { errors } = validateBacktestForm(values);
    expect(errors.rsiThreshold).toBeDefined();
    expect(errors.form).toContain("entry rule");
  });

  it("shows a form error when moving average is enabled but no valid entry rule can be serialized", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      rsiEnabled: false,
      movingAverageEnabled: true,
      fastPeriod: "0",
    };
    const { errors } = validateBacktestForm(values);
    expect(errors.fastPeriod).toBeDefined();
    expect(errors.form).toContain("entry rule");
  });
});

describe("validateBacktestForm – extended rule and risk payloads", () => {
  it("serializes backend-aligned TA and risk fields when enabled", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      rsiEnabled: false,
      movingAverageEnabled: false,
      macdEnabled: true,
      bollingerEnabled: true,
      bollingerBand: "middle",
      ivRankEnabled: true,
      ivRankLookbackDays: "300",
      ivPercentileEnabled: true,
      ivPercentileLookbackDays: "275",
      volumeSpikeEnabled: true,
      volumeSpikeOperator: "gt" as const,
      supportResistanceEnabled: true,
      supportResistanceMode: "breakout_above_resistance",
      supportResistanceTolerancePct: "2.5",
      avoidEarningsEnabled: true,
      avoidEarningsDaysBefore: "5",
      avoidEarningsDaysAfter: "0",
      slippagePct: "1.2",
      profitTargetEnabled: true,
      profitTargetPct: "75",
      stopLossEnabled: true,
      stopLossPct: "25",
      riskFreeRate: "0.05",
    };

    const { errors, payload } = validateBacktestForm(values);

    expect(errors).toEqual({});
    expect(payload).toBeDefined();
    expect(payload).toMatchObject({
      slippage_pct: 1.2,
      profit_target_pct: 75,
      stop_loss_pct: 25,
      risk_free_rate: 0.05,
      entry_rules: expect.arrayContaining([
        { type: "macd", fast_period: 12, slow_period: 26, signal_period: 9, direction: "bullish" },
        { type: "bollinger_bands", period: 20, standard_deviations: 2, band: "middle", operator: "lt" },
        { type: "iv_rank", operator: "gt", threshold: 50, lookback_days: 300 },
        { type: "iv_percentile", operator: "gt", threshold: 50, lookback_days: 275 },
        { type: "volume_spike", operator: "gt", multiplier: 2, lookback_period: 20 },
        { type: "support_resistance", mode: "breakout_above_resistance", lookback_period: 20, tolerance_pct: 2.5 },
        { type: "avoid_earnings", days_before: 5, days_after: 0 },
      ]),
    });
  });

  it("requires avoid earnings windows to exclude at least one side", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      rsiEnabled: false,
      movingAverageEnabled: false,
      avoidEarningsEnabled: true,
      avoidEarningsDaysBefore: "0",
      avoidEarningsDaysAfter: "0",
    };

    const { errors } = validateBacktestForm(values);
    expect(errors.avoidEarningsDaysAfter).toContain("greater than 0");
  });

  it("uses backend support/resistance enum values", () => {
    const values = getDefaultBacktestFormValues();
    expect(values.supportResistanceMode).toBe("near_support");
  });
});
