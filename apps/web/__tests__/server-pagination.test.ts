import { describe, expect, it } from "vitest";

import { buildPaginatedListPath } from "@/lib/api/pagination";

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
});
