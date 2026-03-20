import { describe, it, expect } from "vitest";
import { MAX_SCANNER_WINDOW_DAYS, getScannerWindowTooLongError } from "@/lib/scanner/constants";
import {
  parseSymbols,
  validateScannerForm,
  type ScannerFormInput,
} from "@/lib/scanner/validation";

function validInput(overrides: Partial<ScannerFormInput> = {}): ScannerFormInput {
  return {
    mode: "basic",
    symbolsText: "SPY, QQQ, AAPL",
    selectedStrategies: new Set(["long_call"]),
    startDate: "2024-01-01",
    endDate: "2025-01-01",
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

describe("parseSymbols", () => {
  it("returns empty array for whitespace-only input", () => {
    expect(parseSymbols("   ")).toEqual([]);
  });

  it("parses comma-separated symbols and uppercases them", () => {
    expect(parseSymbols("spy, qqq, aapl")).toEqual(["SPY", "QQQ", "AAPL"]);
  });

  it("handles space-separated symbols", () => {
    expect(parseSymbols("SPY QQQ")).toEqual(["SPY", "QQQ"]);
  });

  it("handles mixed delimiters and extra whitespace", () => {
    expect(parseSymbols("  spy,  qqq  aapl , msft ")).toEqual([
      "SPY",
      "QQQ",
      "AAPL",
      "MSFT",
    ]);
  });
});

describe("validateScannerForm", () => {
  it("returns no errors for valid input", () => {
    expect(validateScannerForm(validInput())).toEqual([]);
  });

  it("requires at least one symbol", () => {
    const errors = validateScannerForm(validInput({ symbolsText: "   " }));
    expect(errors).toContain("At least one symbol is required.");
  });

  it("requires at least one strategy type", () => {
    const errors = validateScannerForm(
      validInput({ selectedStrategies: new Set() }),
    );
    expect(errors).toContain("At least one strategy type is required.");
  });

  it("enforces basic mode symbol limit of 5", () => {
    const errors = validateScannerForm(
      validInput({ symbolsText: "SPY, QQQ, AAPL, MSFT, GOOG, AMZN" }),
    );
    expect(errors).toContain(
      "Basic mode allows at most 5 symbols for your plan.",
    );
  });

  it("enforces advanced mode symbol limit for the current plan", () => {
    const symbols = Array.from({ length: 11 }, (_, i) => `SYM${i}`).join(",");
    const errors = validateScannerForm(
      validInput({ mode: "advanced", symbolsText: symbols }),
    );
    expect(errors).toContain(
      "Advanced mode allows at most 10 symbols for your plan.",
    );
  });

  it("requires start date before end date", () => {
    const errors = validateScannerForm(
      validInput({ startDate: "2025-01-01", endDate: "2024-01-01" }),
    );
    expect(errors).toContain("Start date must be before end date.");
  });

  it("rejects a scanner window longer than the backend maximum before submit", () => {
    const errors = validateScannerForm(
      validInput({ startDate: "2023-01-01", endDate: "2025-01-01" }),
    );
    expect(errors).toContain(getScannerWindowTooLongError(MAX_SCANNER_WINDOW_DAYS));
  });

  it("rejects same start and end date", () => {
    const errors = validateScannerForm(
      validInput({ startDate: "2025-01-01", endDate: "2025-01-01" }),
    );
    expect(errors).toContain("Start date must be before end date.");
  });

  it("rejects DTE tolerance >= target DTE", () => {
    const errors = validateScannerForm(
      validInput({ targetDte: "30", dteTolerance: "35" }),
    );
    expect(errors).toContain("DTE tolerance must be less than target DTE.");
  });

  it("rejects negative account size", () => {
    const errors = validateScannerForm(validInput({ accountSize: "-1000" }));
    expect(errors.some((e) => e.includes("Account size"))).toBe(true);
  });

  it("rejects account size below minimum of 100", () => {
    const errors = validateScannerForm(validInput({ accountSize: "50" }));
    expect(errors.some((e) => e.includes("Account size"))).toBe(true);
  });

  it("validates target DTE range (1-365)", () => {
    const errors = validateScannerForm(validInput({ targetDte: "0" }));
    expect(errors.some((e) => e.includes("Target DTE"))).toBe(true);
  });

  it("validates risk percent must be greater than 0", () => {
    const errors = validateScannerForm(validInput({ riskPct: "0" }));
    expect(errors.some((e) => e.includes("Risk %"))).toBe(true);
  });

  it("validates max recommendations cap of 30", () => {
    const errors = validateScannerForm(validInput({ maxRecs: "31" }));
    expect(errors.some((e) => e.includes("Max recommendations"))).toBe(true);
  });

  it("rejects non-integer target DTE", () => {
    const errors = validateScannerForm(validInput({ targetDte: "30.5" }));
    expect(errors).toContain("Target DTE must be a whole number.");
  });

  it("accepts valid advanced mode input", () => {
    const errors = validateScannerForm(
      validInput({
        mode: "advanced",
        symbolsText: "SPY, QQQ",
        selectedStrategies: new Set(["iron_condor"]),
      }),
    );
    expect(errors).toEqual([]);
  });
});
