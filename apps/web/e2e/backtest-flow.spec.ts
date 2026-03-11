import { test, expect } from "@playwright/test";

/**
 * These tests require authentication. In CI, use Clerk test mode
 * or set CLERK_TESTING_TOKEN environment variable. For local
 * development, sign in manually and use storageState.
 *
 * See: https://clerk.com/docs/testing/playwright
 */
test.describe("Backtest Flow", () => {
  test.skip(
    !process.env.CLERK_TESTING_TOKEN,
    "Requires CLERK_TESTING_TOKEN for authenticated tests",
  );

  test("can navigate to backtest creation page", async ({ page }) => {
    await page.goto("/app/backtests");
    await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
  });

  test("backtest form validates required fields", async ({ page }) => {
    await page.goto("/app/backtests/new");

    const submitButton = page.getByRole("button", { name: /run|create|submit/i });
    if (await submitButton.isVisible()) {
      await submitButton.click();
      const errorMessages = page.locator("[role='alert'], .text-destructive, .text-red");
      await expect(errorMessages.first()).toBeVisible({ timeout: 5000 });
    }
  });
});
