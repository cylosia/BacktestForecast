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
    const values = { ...getDefaultBacktestFormValues(), targetDte: "3" };
    const { errors } = validateBacktestForm(values);
    expect(errors.targetDte).toBeDefined();
  });

  it("rejects non-integer DTE tolerance", () => {
    const values = { ...getDefaultBacktestFormValues(), dteToleranceDays: "5.5" };
    const { errors } = validateBacktestForm(values);
    expect(errors.dteToleranceDays).toContain("whole number");
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
