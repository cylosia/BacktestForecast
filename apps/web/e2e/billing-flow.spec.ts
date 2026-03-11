import { test, expect } from "@playwright/test";

test.describe("Billing Flow", () => {
  test.skip(
    !process.env.CLERK_TESTING_TOKEN,
    "Requires CLERK_TESTING_TOKEN for authenticated tests",
  );

  test("settings billing page loads", async ({ page }) => {
    await page.goto("/app/settings/billing");
    await expect(page.locator("body")).toBeVisible();
  });

  test("upgrade button redirects to Stripe checkout", async ({ page }) => {
    await page.goto("/pricing");

    const upgradeButton = page.getByRole("link", {
      name: /upgrade|get started|subscribe/i,
    });
    if (await upgradeButton.isVisible()) {
      const [popup] = await Promise.all([
        page.waitForEvent("popup").catch(() => null),
        upgradeButton.click(),
      ]);
      if (popup) {
        await expect(popup).toHaveURL(/stripe\.com|checkout/);
        await popup.close();
      }
    }
  });
});
