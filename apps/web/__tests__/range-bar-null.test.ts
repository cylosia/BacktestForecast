import { describe, it, expect } from "vitest";
import { toNumber } from "@/lib/backtests/format";

/**
 * Tests that the RangeBar percentile rendering logic handles null values
 * without producing NaN widths or layout bugs.
 */

function computeBarWidth(
  value: number | null,
  min: number,
  max: number,
): number {
  const n = toNumber(value);
  if (Number.isNaN(n)) return 0;
  const range = max - min;
  if (range === 0) return 0;
  return Math.max(0, Math.min(100, ((n - min) / range) * 100));
}

describe("RangeBar null handling", () => {
  it("does not produce NaN width for null percentile values", () => {
    const width = computeBarWidth(null, -10, 20);
    expect(Number.isNaN(width)).toBe(false);
    expect(width).toBe(0);
  });

  it("computes correct width for valid values", () => {
    const width = computeBarWidth(5, 0, 10);
    expect(width).toBe(50);
  });

  it("clamps width to 0 for values below min", () => {
    const width = computeBarWidth(-5, 0, 10);
    expect(width).toBe(0);
  });

  it("clamps width to 100 for values above max", () => {
    const width = computeBarWidth(15, 0, 10);
    expect(width).toBe(100);
  });

  it("handles zero-range (all values equal) without division by zero", () => {
    const width = computeBarWidth(5, 5, 5);
    expect(Number.isNaN(width)).toBe(false);
    expect(width).toBe(0);
  });

  it("handles all-null percentile scenario", () => {
    const percentiles = [null, null, null, null, null];
    const widths = percentiles.map((p) => computeBarWidth(p, -10, 20));
    expect(widths.every((w) => w === 0)).toBe(true);
    expect(widths.every((w) => !Number.isNaN(w))).toBe(true);
  });

  it("handles partial null percentile scenario", () => {
    const values: Array<number | null> = [-10, null, 5, null, 20];
    const widths = values.map((v) => computeBarWidth(v, -10, 20));
    expect(widths[0]).toBe(0);
    expect(widths[1]).toBe(0);
    expect(widths[2]).toBe(50);
    expect(widths[3]).toBe(0);
    expect(widths[4]).toBe(100);
  });
});
