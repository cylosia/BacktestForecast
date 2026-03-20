import { describe, it, expect, vi, afterEach } from "vitest";
import {
  getDefaultBacktestFormValues,
  validateBacktestForm,
} from "@/lib/backtests/validation";

afterEach(() => {
  vi.useRealTimers();
});

describe("getDefaultBacktestFormValues", () => {
  it("returns valid ISO date strings for startDate and endDate", () => {
    const defaults = getDefaultBacktestFormValues();

    const isoDatePattern = /^\d{4}-\d{2}-\d{2}$/;
    expect(defaults.startDate).toMatch(isoDatePattern);
    expect(defaults.endDate).toMatch(isoDatePattern);
  });

  it("endDate matches the latest New York market date in summer", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2025-07-14T03:30:00Z"));

    const defaults = getDefaultBacktestFormValues();
    expect(defaults.endDate).toBe("2025-07-11");
  });

  it("endDate matches the latest New York market date in winter", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2025-01-13T04:30:00Z"));

    const defaults = getDefaultBacktestFormValues();
    expect(defaults.endDate).toBe("2025-01-10");
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

  it("rejects an endDate after the latest New York market date", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2025-07-14T03:30:00Z"));

    const values = {
      ...getDefaultBacktestFormValues(),
      startDate: "2025-07-01",
      endDate: "2025-07-12",
    };

    const { errors } = validateBacktestForm(values);
    expect(errors.endDate).toContain("latest market date");
    expect(errors.endDate).toContain("America/New_York");
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
