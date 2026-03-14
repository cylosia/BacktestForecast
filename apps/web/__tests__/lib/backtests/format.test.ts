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
