import { test, expect } from "@playwright/test";

test.describe("Smoke Tests", () => {
  test("landing page loads and contains key elements", async ({ page }) => {
    await page.goto("/");
    await expect(page).toHaveTitle(/BacktestForecast/i);
    await expect(page.locator("body")).toBeVisible();
  });

  test("pricing page is accessible", async ({ page }) => {
    await page.goto("/pricing");
    await expect(page.locator("body")).toBeVisible();
  });

  test("unauthenticated user is redirected from app routes", async ({
    page,
  }) => {
    await page.goto("/app/backtests");
    await page.waitForURL(/sign-in|clerk/);
  });
});
