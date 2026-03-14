import { describe, it, expect } from "vitest";
import { formatDate, formatDateTime, formatCurrency, formatPercent, formatNumber, toNumber } from "@/lib/backtests/format";

describe("formatDate with date-only string", () => {
  it('formats "2024-01-15" to contain "Jan 15" (not shifted by timezone)', () => {
    const result = formatDate("2024-01-15");
    expect(result).toContain("Jan");
    expect(result).toContain("15");
    expect(result).not.toContain("14");
  });

  it('formats "2024-12-31" correctly', () => {
    const result = formatDate("2024-12-31");
    expect(result).toContain("Dec");
    expect(result).toContain("31");
  });

  it('formats "2024-03-01" without shifting to February', () => {
    const result = formatDate("2024-03-01");
    expect(result).toContain("Mar");
    expect(result).toContain("1");
    expect(result).not.toContain("Feb");
  });

  it("returns dash for null", () => {
    expect(formatDate(null)).toBe("—");
  });

  it("returns dash for undefined", () => {
    expect(formatDate(undefined)).toBe("—");
  });

  it("returns dash for empty string", () => {
    expect(formatDate("")).toBe("—");
  });

  it("returns dash for invalid date string", () => {
    expect(formatDate("not-a-date")).toBe("—");
  });

  it("handles ISO datetime string without timezone shift", () => {
    const result = formatDate("2024-06-15T00:00:00Z");
    expect(result).toContain("Jun");
    expect(result).toContain("15");
  });
});

describe("formatCurrency", () => {
  it("formats positive values as USD", () => {
    expect(formatCurrency(1234.56)).toBe("$1,234.56");
  });
  it("returns — for null", () => {
    expect(formatCurrency(null)).toBe("—");
  });
  it("returns — for undefined", () => {
    expect(formatCurrency(undefined)).toBe("—");
  });
  it("formats negative values", () => {
    expect(formatCurrency(-99.5)).toBe("-$99.50");
  });
});

describe("formatPercent", () => {
  it("formats with percent sign", () => {
    expect(formatPercent(50)).toBe("50%");
  });
  it("returns — for null", () => {
    expect(formatPercent(null)).toBe("—");
  });
  it("handles decimal precision", () => {
    expect(formatPercent(33.333)).toBe("33.33%");
  });
});

describe("formatNumber", () => {
  it("formats with commas", () => {
    expect(formatNumber(1000000)).toBe("1,000,000");
  });
  it("returns — for null", () => {
    expect(formatNumber(null)).toBe("—");
  });
});

describe("toNumber", () => {
  it("converts string to number", () => {
    expect(toNumber("42.5")).toBe(42.5);
  });
  it("returns NaN for null", () => {
    expect(toNumber(null)).toBeNaN();
  });
  it("returns NaN for non-numeric string", () => {
    expect(toNumber("abc")).toBeNaN();
  });
  it("passes through numbers", () => {
    expect(toNumber(100)).toBe(100);
  });
});

describe("formatDateTime", () => {
  it("formats ISO datetime", () => {
    const result = formatDateTime("2025-06-15T10:30:00Z");
    expect(result).toContain("Jun");
    expect(result).toContain("15");
    expect(result).toContain("2025");
  });
  it("returns — for null", () => {
    expect(formatDateTime(null)).toBe("—");
  });
});
