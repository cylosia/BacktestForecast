import { describe, expect, it } from "vitest";

import { buildCursorPaginatedPath, buildPaginatedListPath } from "@/lib/api/pagination";

describe("buildPaginatedListPath", () => {
  it("uses offset when no cursor is provided", () => {
    expect(buildPaginatedListPath("/v1/backtests", 25, 75, 100)).toBe(
      "/v1/backtests?limit=25&offset=75",
    );
  });

  it("prefers cursor over offset when a cursor is present", () => {
    expect(buildPaginatedListPath("/v1/scans", 50, 200, 50, "opaque-cursor")).toBe(
      "/v1/scans?limit=50&cursor=opaque-cursor",
    );
  });

  it("clamps invalid limits and offsets", () => {
    expect(buildPaginatedListPath("/v1/sweeps", 999, -5, 50)).toBe(
      "/v1/sweeps?limit=50&offset=0",
    );
  });

  it("builds cursor-only pagination paths without offset fallback", () => {
    expect(buildCursorPaginatedPath("/v1/daily-picks/history", 40, 30, "opaque-cursor")).toBe(
      "/v1/daily-picks/history?limit=30&cursor=opaque-cursor",
    );
    expect(buildCursorPaginatedPath("/v1/daily-picks/history", 5, 30)).toBe(
      "/v1/daily-picks/history?limit=5",
    );
  });

  it("lets the daily-picks page surface next_cursor in the page URL while still sending cursor to the API", () => {
    expect(buildCursorPaginatedPath("/v1/daily-picks/history", 25, 30, "backend-next-cursor")).toBe(
      "/v1/daily-picks/history?limit=25&cursor=backend-next-cursor",
    );
  });
});
