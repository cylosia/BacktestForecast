/**
 * E2E tests with a real API backend.
 *
 * These tests require the full stack (API + Worker + Postgres + Redis) to be
 * running.  They are NOT executed in the default Playwright CI job which only
 * starts the Next.js frontend.
 *
 * To run locally:
 *   1. docker compose up -d
 *   2. pnpm --filter @backtestforecast/web test:e2e -- --grep @fullstack
 *
 * Coverage targets (implement as the backend test harness matures):
 *   - Authenticated backtest creation through the UI and polling until complete
 *   - Scanner job submission, progress polling, and recommendation display
 *   - Export flow: trigger CSV export, poll status, download file
 *   - Template CRUD: create, list, update, delete
 *   - Deep-analysis launch and result rendering
 *   - Billing: checkout redirect, webhook-driven plan upgrade, quota enforcement
 *   - Error states: expired session redirect, 429 rate-limit toast, server error page
 */

import { test, expect } from "@playwright/test";

test.describe("Full-stack E2E @fullstack", () => {
  test.skip(
    !process.env.API_BASE_URL,
    "Skipped: set API_BASE_URL to run full-stack E2E tests",
  );

  test("API health endpoint is reachable", async ({ request }) => {
    const base = process.env.API_BASE_URL ?? "http://localhost:8000";
    const res = await request.get(`${base}/health/live`);
    expect(res.ok()).toBeTruthy();
  });

  test("landing page renders with live API meta", async ({ page }) => {
    await page.goto("/");
    await expect(page).toHaveTitle(/BacktestForecast/i);
  });
});
