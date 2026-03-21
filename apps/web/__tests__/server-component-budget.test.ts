import { describe, expect, it } from "vitest";
import fs from "node:fs";
import path from "node:path";

const root = path.resolve(__dirname, "..");

function read(relPath: string): string {
  return fs.readFileSync(path.join(root, relPath), "utf8");
}

describe("server component request budget", () => {
  it("memoizes getCurrentUser via react cache", () => {
    const source = read("lib/api/server.ts");
    expect(source).toContain("export const getCurrentUser = cache(async ()");
  });

  it("avoids fetching /v1/me in the app layout", () => {
    const source = read("app/app/layout.tsx");
    expect(source).not.toContain("getCurrentUser");
  });

  it("keeps dashboard data loading to user + history calls", () => {
    const source = read("app/app/dashboard/page.tsx");
    expect(source).toContain("Promise.allSettled([getCurrentUser(), getBacktestHistory(10)])");
  });
});
