import { test, expect } from "@playwright/test";

test.describe("Sweep Flow", () => {
  test.skip(
    !process.env.CLERK_TESTING_TOKEN,
    "Requires CLERK_TESTING_TOKEN for authenticated tests",
  );

  test("can navigate to sweep creation page", async ({ page }) => {
    await page.goto("/app/sweeps/new");
    await expect(page.getByRole("heading")).toBeVisible();
  });

  test("sweep form validates required fields", async ({ page }) => {
    await page.goto("/app/sweeps/new");
    const submitButton = page.getByRole("button", { name: /create|start|submit/i });
    if (await submitButton.isVisible()) {
      await submitButton.click();
      const errorMessages = page.locator("[role='alert'], .text-destructive");
      await expect(errorMessages.first()).toBeVisible({ timeout: 5000 });
    }
  });

  test("sweep list page renders", async ({ page }) => {
    await page.goto("/app/sweeps");
    await expect(page.getByRole("heading")).toBeVisible();
  });
});
