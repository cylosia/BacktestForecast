import { describe, expect, it } from "vitest";
import fs from "node:fs";
import path from "node:path";

const root = path.resolve(__dirname, "..");

function read(relPath: string): string {
  return fs.readFileSync(path.join(root, relPath), "utf8");
}

describe("server component request budget", () => {
  it("memoizes current-user loading behind a cached token-keyed loader", () => {
    const source = read("lib/api/server.ts");
    expect(source).toContain("const loadCurrentUser = cache(async (token: string)");
    expect(source).toContain("return loadCurrentUser(await getServerToken())");
  });

  it("uses getCurrentUser in both layout and dashboard so the cached loader can collapse duplicate /v1/me reads", () => {
    const layout = read("app/app/layout.tsx");
    const dashboard = read("app/app/dashboard/page.tsx");
    expect(layout).toContain("getCurrentUser");
    expect(dashboard).toContain("getCurrentUser");
  });

  it("keeps dashboard page data loading to user + history calls", () => {
    const source = read("app/app/dashboard/page.tsx");
    expect(source).toContain("Promise.allSettled([getCurrentUser(), getBacktestHistory(10)])");
  });

  it("avoids an extra /v1/meta round trip on the daily-picks page", () => {
    const source = read("app/app/daily-picks/page.tsx");
    expect(source).not.toContain("getMeta(");
    expect(source).toContain("getDailyPicksScheduleLabel()");
  });
});
