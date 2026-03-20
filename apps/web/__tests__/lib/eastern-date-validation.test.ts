import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  getDefaultBacktestFormValues,
  validateBacktestForm,
} from "@/lib/backtests/validation";
import { validateScannerForm, type ScannerFormInput } from "@/lib/scanner/validation";
import { currentEasternDate, daysAgoET } from "@/lib/utils";

function withBacktestEndDate(endDate: string) {
  return {
    ...getDefaultBacktestFormValues(),
    startDate: "2024-01-01",
    endDate,
  };
}

function validScannerInput(overrides: Partial<ScannerFormInput> = {}): ScannerFormInput {
  return {
    mode: "basic",
    symbolsText: "SPY, QQQ, AAPL",
    selectedStrategies: new Set(["long_call"]),
    startDate: "2024-01-01",
    endDate: "2024-06-14",
    targetDte: "30",
    dteTolerance: "5",
    maxHolding: "10",
    accountSize: "10000",
    riskPct: "2",
    commission: "0.65",
    maxRecs: "10",
    ...overrides,
  };
}

describe("US Eastern market-date helpers", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("returns the prior calendar date before midnight ET during daylight saving time", () => {
    vi.setSystemTime(new Date("2024-06-15T03:30:00Z"));

    expect(currentEasternDate()).toBe("2024-06-14");
    expect(daysAgoET(0)).toBe("2024-06-14");
    expect(daysAgoET(1)).toBe("2024-06-13");
  });

  it("returns the prior calendar date before midnight ET during standard time", () => {
    vi.setSystemTime(new Date("2024-12-15T04:30:00Z"));

    expect(currentEasternDate()).toBe("2024-12-14");
    expect(daysAgoET(0)).toBe("2024-12-14");
    expect(daysAgoET(1)).toBe("2024-12-13");
  });
});

describe("backtest validation Eastern time boundary", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("uses the current Eastern market date for default backtest dates during DST", () => {
    vi.setSystemTime(new Date("2024-06-15T03:30:00Z"));

    const defaults = getDefaultBacktestFormValues();
    expect(defaults.endDate).toBe("2024-06-14");
  });

  it("uses the current Eastern market date for default backtest dates during standard time", () => {
    vi.setSystemTime(new Date("2024-12-15T04:30:00Z"));

    const defaults = getDefaultBacktestFormValues();
    expect(defaults.endDate).toBe("2024-12-14");
  });

  it("rejects a future end date during daylight saving time", () => {
    vi.setSystemTime(new Date("2024-06-15T03:30:00Z"));

    const { errors } = validateBacktestForm(withBacktestEndDate("2024-06-15"));
    expect(errors.endDate).toBe("End date cannot be in the future (US Eastern time).");
  });

  it("rejects a future end date during standard time", () => {
    vi.setSystemTime(new Date("2024-12-15T04:30:00Z"));

    const { errors } = validateBacktestForm(withBacktestEndDate("2024-12-15"));
    expect(errors.endDate).toBe("End date cannot be in the future (US Eastern time).");
  });
});

describe("scanner validation Eastern time boundary", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("rejects a future end date during daylight saving time", () => {
    vi.setSystemTime(new Date("2024-06-15T03:30:00Z"));

    const errors = validateScannerForm(validScannerInput({ endDate: "2024-06-15" }));
    expect(errors).toContain("End date cannot be in the future (US Eastern time).");
  });

  it("rejects a future start date during standard time", () => {
    vi.setSystemTime(new Date("2024-12-15T04:30:00Z"));

    const errors = validateScannerForm(
      validScannerInput({ startDate: "2024-12-15", endDate: "2024-12-16" }),
    );
    expect(errors).toContain("Start date cannot be in the future (US Eastern time).");
  });
});
