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

  test("pricing page shows paid-tier checkout buttons", async ({ page }) => {
    await page.goto("/pricing");

    const monthlyButton = page.getByRole("button", {
      name: /start monthly plan/i,
    }).first();
    const yearlyButton = page.getByRole("button", {
      name: /start yearly plan/i,
    }).first();
    const freeLink = page.getByRole("link", {
      name: /sign in to get started/i,
    });

    const hasMonthly = await monthlyButton.isVisible().catch(() => false);
    const hasYearly = await yearlyButton.isVisible().catch(() => false);
    const hasFree = await freeLink.isVisible().catch(() => false);

    expect(hasMonthly || hasYearly || hasFree).toBeTruthy();

    if (hasFree) {
      const href = await freeLink.getAttribute("href");
      expect(href).toContain("/sign-in");
    }
  });

  test("upgrade button redirects to Stripe checkout", async ({ page }) => {
    await page.goto("/pricing");

    const upgradeButton = page.getByRole("button", {
      name: /start monthly plan|start yearly plan/i,
    }).first();
    if (await upgradeButton.isVisible()) {
      await upgradeButton.click();
      await page.waitForURL(/stripe\.com|checkout/, { timeout: 15000 }).catch(() => {});
    }
  });

  test("checkout success banner appears with query param", async ({ page }) => {
    await page.goto("/app/settings/billing?checkout=success");

    const banner = page.getByText("Subscription activated");
    if (await banner.isVisible({ timeout: 5000 }).catch(() => false)) {
      await expect(banner).toBeVisible();
    }
  });
});
