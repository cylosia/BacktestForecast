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
  it("does NOT show form error when RSI is enabled but has invalid values", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      rsiEnabled: true,
      rsiThreshold: "999",
      movingAverageEnabled: false,
    };
    const { errors } = validateBacktestForm(values);
    expect(errors.rsiThreshold).toBeDefined();
    expect(errors.form).toBeUndefined();
  });

  it("does NOT show form error when moving average is enabled but has invalid values", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      rsiEnabled: false,
      movingAverageEnabled: true,
      fastPeriod: "0",
    };
    const { errors } = validateBacktestForm(values);
    expect(errors.fastPeriod).toBeDefined();
    expect(errors.form).toBeUndefined();
  });
});
