import { describe, expect, it } from "vitest";
import fs from "node:fs";
import path from "node:path";

const root = path.resolve(__dirname, "..");

function read(relPath: string): string {
  return fs.readFileSync(path.join(root, relPath), "utf8");
}

describe("page-level loading budgets", () => {
  it("keeps the app layout to a single current-user fetch helper", () => {
    const source = read("app/app/layout.tsx");
    expect((source.match(/getCurrentUser\(/g) ?? []).length).toBe(1);
  });

  it("keeps dashboard page fetches within the expected two-call budget", () => {
    const source = read("app/app/dashboard/page.tsx");
    expect(source).toContain("Promise.allSettled([getCurrentUser(), getBacktestHistory(10)])");
  });

  it("avoids duplicate getCurrentUser calls on analysis and daily-picks pages", () => {
    for (const relPath of ["app/app/analysis/page.tsx", "app/app/daily-picks/page.tsx"]) {
      const source = read(relPath);
      expect((source.match(/getCurrentUser\(/g) ?? []).length).toBeLessThanOrEqual(1);
    }
  });
});
