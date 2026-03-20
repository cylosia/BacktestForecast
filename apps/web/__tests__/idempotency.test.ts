import { describe, expect, it } from "vitest";

import { getOrCreatePendingIdempotencyKey } from "@/lib/idempotency";

describe("getOrCreatePendingIdempotencyKey", () => {
  it("reuses an existing non-empty key", () => {
    expect(getOrCreatePendingIdempotencyKey("existing-key", "backtest")).toBe("existing-key");
  });

  it("generates a prefixed key when none exists", () => {
    const key = getOrCreatePendingIdempotencyKey(null, "scan");
    expect(key.startsWith("scan-")).toBe(true);
    expect(key.length).toBeGreaterThan("scan-".length);
  });

  it("generates an unprefixed UUID when no prefix is provided", () => {
    const key = getOrCreatePendingIdempotencyKey(undefined);
    expect(key).toMatch(/^[0-9a-f-]{36}$/i);
  });
});
