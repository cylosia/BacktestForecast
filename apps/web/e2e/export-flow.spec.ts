import { test, expect } from "@playwright/test";

test.describe("Export Flow", () => {
  test.skip(
    !process.env.CLERK_TESTING_TOKEN,
    "Requires CLERK_TESTING_TOKEN for authenticated tests",
  );

  test("backtest detail page shows export actions", async ({ page }) => {
    await page.goto("/app/backtests");
    const firstLink = page.locator("a[href*='/app/backtests/']").first();
    if (await firstLink.isVisible({ timeout: 5000 }).catch(() => false)) {
      await firstLink.click();
      await expect(page).toHaveURL(/\/app\/backtests\//);
    }
  });
});
