import { describe, it, expect } from "vitest";
import { toNumber, formatCurrency, formatPercent, formatNumber, formatDate } from "@/lib/backtests/format";

describe("toNumber", () => {
  it("returns NaN for null input", () => {
    expect(toNumber(null)).toBeNaN();
  });

  it("returns NaN for undefined input", () => {
    expect(toNumber(undefined)).toBeNaN();
  });

  it("returns the correct number for valid numeric input", () => {
    expect(toNumber(42)).toBe(42);
    expect(toNumber(3.14)).toBe(3.14);
    expect(toNumber(0)).toBe(0);
    expect(toNumber(-10)).toBe(-10);
  });

  it("parses numeric strings correctly", () => {
    expect(toNumber("42")).toBe(42);
    expect(toNumber("3.14")).toBe(3.14);
  });

  it("does not crash Math.max when result is NaN", () => {
    const nanResult = toNumber(null);
    const maxResult = Math.max(nanResult, 100);
    expect(maxResult).toBeNaN();
  });

  it("does not crash Math.min when result is NaN", () => {
    const nanResult = toNumber(null);
    const minResult = Math.min(nanResult, 0);
    expect(minResult).toBeNaN();
  });

  it("handles edge case of empty string", () => {
    const result = toNumber("");
    expect(Number.isNaN(result) || result === 0).toBe(true);
  });
});

describe("formatCurrency", () => {
  it("formats a positive number as USD", () => {
    expect(formatCurrency(1234.56)).toBe("$1,234.56");
  });

  it("returns dash for null", () => {
    expect(formatCurrency(null)).toBe("-");
  });

  it("returns dash for undefined", () => {
    expect(formatCurrency(undefined)).toBe("-");
  });

  it("formats zero", () => {
    expect(formatCurrency(0)).toBe("$0.00");
  });

  it("accepts numeric strings", () => {
    expect(formatCurrency("99.9")).toBe("$99.90");
  });
});

describe("formatPercent", () => {
  it("formats a positive percentage", () => {
    expect(formatPercent(12.5)).toBe("12.5%");
  });

  it("returns dash for null", () => {
    expect(formatPercent(null)).toBe("-");
  });

  it("formats zero", () => {
    expect(formatPercent(0)).toBe("0%");
  });
});

describe("formatNumber", () => {
  it("formats a number with commas", () => {
    expect(formatNumber(1234567)).toBe("1,234,567");
  });

  it("returns dash for null", () => {
    expect(formatNumber(null)).toBe("-");
  });
});

describe("formatDate", () => {
  it("formats a valid ISO date string", () => {
    const result = formatDate("2025-03-14");
    expect(result).toContain("2025");
    expect(result).not.toBe("-");
  });

  it("returns dash for null", () => {
    expect(formatDate(null)).toBe("-");
  });

  it("returns dash for empty string", () => {
    expect(formatDate("")).toBe("-");
  });

  it("returns dash for invalid date", () => {
    expect(formatDate("not-a-date")).toBe("-");
  });
});
