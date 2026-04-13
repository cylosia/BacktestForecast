import { describe, it, expect } from "vitest";
import {
  validateBacktestForm,
  getDefaultBacktestFormValues,
} from "@/lib/backtests/validation";
import { isPlanLimitError } from "@/lib/billing/errors";

describe("scanner form status lifecycle", () => {
  it("returns no errors for valid default form values", () => {
    const values = getDefaultBacktestFormValues();
    const { errors, payload } = validateBacktestForm(values);
    expect(Object.keys(errors)).toHaveLength(0);
    expect(payload).toBeDefined();
    expect(payload!.symbol).toBe("SPY");
  });

  it("transitions to error state when symbol is empty", () => {
    const values = { ...getDefaultBacktestFormValues(), symbol: "" };
    const { errors } = validateBacktestForm(values);
    expect(errors.symbol).toBeDefined();
    expect(errors.symbol).toContain("required");
  });

  it("transitions to error state when start date >= end date", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      startDate: "2025-06-01",
      endDate: "2025-01-01",
    };
    const { errors } = validateBacktestForm(values);
    expect(errors.endDate).toBeDefined();
  });

  it("transitions to error when no entry rules are enabled", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      rsiEnabled: false,
      movingAverageEnabled: false,
    };
    const { errors } = validateBacktestForm(values);
    expect(errors.form).toBeDefined();
    expect(errors.form).toContain("entry rule");
  });

  it("validates numeric bounds on target DTE", () => {
    const values = { ...getDefaultBacktestFormValues(), targetDte: "0" };
    const { errors } = validateBacktestForm(values);
    expect(errors.targetDte).toBeDefined();
  });

  it("rejects non-integer DTE tolerance", () => {
    const values = { ...getDefaultBacktestFormValues(), dteToleranceDays: "5.5" };
    const { errors } = validateBacktestForm(values);
    expect(errors.dteToleranceDays).toContain("whole number");
  });
});

describe("backtest form submission edge cases", () => {
  it("rejects account size below minimum (100)", () => {
    const values = { ...getDefaultBacktestFormValues(), accountSize: "50" };
    const { errors } = validateBacktestForm(values);
    expect(errors.accountSize).toBeDefined();
    expect(errors.accountSize).toContain("at least");
  });

  it("rejects account size above maximum (100M)", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      accountSize: "200000000",
    };
    const { errors } = validateBacktestForm(values);
    expect(errors.accountSize).toBeDefined();
  });

  it("rejects end date before start date", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      startDate: "2025-06-01",
      endDate: "2025-01-01",
    };
    const { errors } = validateBacktestForm(values);
    expect(errors.endDate).toBeDefined();
    expect(errors.endDate).toContain("later");
  });

  it("rejects date range exceeding backend maximum window", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      startDate: "2000-01-01",
      endDate: "2025-01-01",
    };
    const { errors } = validateBacktestForm(values);
    expect(errors.endDate).toBeDefined();
    expect(errors.endDate).toContain("5 years");
  });

  it("rejects non-numeric account size", () => {
    const values = { ...getDefaultBacktestFormValues(), accountSize: "abc" };
    const { errors } = validateBacktestForm(values);
    expect(errors.accountSize).toBeDefined();
  });

  it("rejects risk per trade of zero (exclusive minimum)", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      riskPerTradePct: "0",
    };
    const { errors } = validateBacktestForm(values);
    expect(errors.riskPerTradePct).toBeDefined();
    expect(errors.riskPerTradePct).toContain("greater than");
  });


  it("includes the risk-free rate in the create payload", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      riskFreeRate: "0.031",
    };
    const { errors, payload } = validateBacktestForm(values);
    expect(Object.keys(errors)).toHaveLength(0);
    expect(payload?.risk_free_rate).toBe(0.031);
  });

  it("accepts valid payload and returns correct symbol normalization", () => {
    const values = { ...getDefaultBacktestFormValues(), symbol: "aapl" };
    const { errors, payload } = validateBacktestForm(values);
    expect(Object.keys(errors)).toHaveLength(0);
    expect(payload).toBeDefined();
    expect(payload!.symbol).toBe("AAPL");
  });

  it("serializes put calendar selection as a first-class strategy type", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      strategyType: "put_calendar_spread" as const,
    };
    const { errors, payload } = validateBacktestForm(values);
    expect(Object.keys(errors)).toHaveLength(0);
    expect(payload?.strategy_type).toBe("put_calendar_spread");
    expect(payload?.strategy_overrides).toBeUndefined();
  });

  it("serializes advanced generic indicator rules into the payload", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      rsiEnabled: false,
      advancedRules: [
        {
          id: "rule_1",
          type: "indicator_trend" as const,
          series: {
            indicator: "rsi" as const,
            period: "14",
            fastPeriod: "12",
            slowPeriod: "26",
            signalPeriod: "9",
            standardDeviations: "2",
            band: "lower" as const,
            lookbackDays: "252",
            lookbackPeriod: "20",
            kPeriod: "14",
            dPeriod: "3",
            smoothK: "3",
          },
          leftSeries: {
            indicator: "close" as const,
            period: "14",
            fastPeriod: "12",
            slowPeriod: "26",
            signalPeriod: "9",
            standardDeviations: "2",
            band: "lower" as const,
            lookbackDays: "252",
            lookbackPeriod: "20",
            kPeriod: "14",
            dPeriod: "3",
            smoothK: "3",
          },
          rightSeries: {
            indicator: "ema" as const,
            period: "21",
            fastPeriod: "12",
            slowPeriod: "26",
            signalPeriod: "9",
            standardDeviations: "2",
            band: "lower" as const,
            lookbackDays: "252",
            lookbackPeriod: "20",
            kPeriod: "14",
            dPeriod: "3",
            smoothK: "3",
          },
          operator: "gte" as const,
          direction: "rising" as const,
          level: "50",
          bars: "4",
        },
      ],
    };
    const { errors, payload } = validateBacktestForm(values);
    expect(errors.advancedRules).toBeUndefined();
    expect(payload?.entry_rules).toContainEqual({
      type: "indicator_trend",
      series: {
        indicator: "rsi",
        period: 14,
      },
      direction: "rising",
      bars: 4,
    });
  });


  it("uses backend-compatible bollinger field names", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      rsiEnabled: false,
      bollingerEnabled: true,
    };
    const { errors, payload } = validateBacktestForm(values);
    expect(Object.keys(errors)).toHaveLength(0);
    expect(payload?.entry_rules).toContainEqual({
      type: "bollinger_bands",
      period: 20,
      standard_deviations: 2,
      band: "lower",
      operator: "lt",
    });
  });

  it("rejects MACD configurations where fast period is not less than slow period", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      rsiEnabled: false,
      macdEnabled: true,
      macdFastPeriod: "26",
      macdSlowPeriod: "26",
    };
    const { errors } = validateBacktestForm(values);
    expect(errors.macdSlowPeriod).toContain("greater than fast period");
  });

  it("rejects avoid-earnings rules with a zero-day window on both sides", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      rsiEnabled: false,
      avoidEarningsEnabled: true,
      avoidEarningsDaysBefore: "0",
      avoidEarningsDaysAfter: "0",
    };
    const { errors } = validateBacktestForm(values);
    expect(errors.avoidEarningsDaysAfter).toContain("greater than 0");
  });

  it("uses backend-compatible support/resistance enum values", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      rsiEnabled: false,
      supportResistanceEnabled: true,
      supportResistanceMode: "near_support",
    };
    const { errors, payload } = validateBacktestForm(values);
    expect(errors.supportResistancePeriod).toBeUndefined();
    expect(payload).toBeDefined();
    expect(payload!.entry_rules).toContainEqual({
      type: "support_resistance",
      mode: "near_support",
      lookback_period: 20,
    });
  });

  it("rejects DTE tolerance >= target DTE", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      targetDte: "30",
      dteToleranceDays: "30",
    };
    const { errors } = validateBacktestForm(values);
    expect(errors.dteToleranceDays).toBeDefined();
    expect(errors.dteToleranceDays).toContain("less than");
  });

  it("rejects symbol with special characters", () => {
    const values = { ...getDefaultBacktestFormValues(), symbol: "SPY!" };
    const { errors } = validateBacktestForm(values);
    expect(errors.symbol).toBeDefined();
  });

  it("rejects empty symbol", () => {
    const values = { ...getDefaultBacktestFormValues(), symbol: "  " };
    const { errors } = validateBacktestForm(values);
    expect(errors.symbol).toBeDefined();
  });
});

describe("isPlanLimitError", () => {
  it("identifies quota_exceeded as a plan limit error", () => {
    expect(isPlanLimitError("quota_exceeded")).toBe(true);
  });

  it("identifies feature_locked as a plan limit error", () => {
    expect(isPlanLimitError("feature_locked")).toBe(true);
  });

  it("does not treat generic errors as plan limit errors", () => {
    expect(isPlanLimitError("server_error")).toBe(false);
    expect(isPlanLimitError("validation_error")).toBe(false);
  });

  it("returns false for undefined error code", () => {
    expect(isPlanLimitError(undefined)).toBe(false);
  });
});
