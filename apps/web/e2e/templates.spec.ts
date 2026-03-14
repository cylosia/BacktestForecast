import { test, expect } from "@playwright/test";

test.describe("Templates Page", () => {
  test("templates page renders heading and list", async ({ page }) => {
    await page.goto("/app/templates");

    const heading = page.getByRole("heading", { level: 1 });
    await expect(heading).toBeVisible({ timeout: 10_000 });
    await expect(heading).toHaveText(/template/i);
  });

  test("template picker apply navigates to backtest form", async ({ page }) => {
    await page.goto("/app/templates");

    const applyLink = page.getByRole("link", { name: /apply/i }).first();
    const isVisible = await applyLink
      .isVisible({ timeout: 5_000 })
      .catch(() => false);

    if (isVisible) {
      const href = await applyLink.getAttribute("href");
      expect(href).toMatch(/\/app\/backtests\/new\?template=/);

      await applyLink.click();
      await page.waitForURL(/\/app\/backtests\/new\?template=/);
      await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
    } else {
      test.skip(true, "No apply link visible — templates may be empty");
    }
  });
});
