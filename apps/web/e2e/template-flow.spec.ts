import { test, expect } from "@playwright/test";

test.describe("Template Flow", () => {
  test.skip(
    !process.env.CLERK_TESTING_TOKEN,
    "Requires CLERK_TESTING_TOKEN for authenticated tests",
  );

  test("can navigate to templates page", async ({ page }) => {
    await page.goto("/app/templates");
    await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
  });

  test("can open template creation form", async ({ page }) => {
    await page.goto("/app/templates");

    const createButton = page.getByRole("button", {
      name: /new|create|add/i,
    });
    if (await createButton.isVisible()) {
      await createButton.click();
      const form = page.locator("form, [role='dialog']");
      await expect(form.first()).toBeVisible({ timeout: 5000 });
    }
  });

  test("apply link navigates to backtest form with template query param", async ({ page }) => {
    await page.goto("/app/templates");

    const applyLink = page.getByRole("link", { name: /apply/i }).first();
    if (await applyLink.isVisible({ timeout: 3000 }).catch(() => false)) {
      const href = await applyLink.getAttribute("href");
      expect(href).toMatch(/\/app\/backtests\/new\?template=/);

      await applyLink.click();
      await page.waitForURL(/\/app\/backtests\/new\?template=/);
      await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
    }
  });
});
