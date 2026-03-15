import { test, expect } from "@playwright/test";

test.describe("Smoke Tests", () => {
  test("landing page loads and contains key elements", async ({ page }) => {
    await page.goto("/");
    await expect(page).toHaveTitle(/BacktestForecast/i);
    await expect(page.locator("body")).toBeVisible();

    // Header and navigation
    const header = page.locator("header").first();
    await expect(header).toBeVisible();
    const nav = header.getByRole("link");
    expect(await nav.count()).toBeGreaterThanOrEqual(1);

    // Hero section with CTA
    const heading = page.getByRole("heading", { level: 1 });
    await expect(heading).toBeVisible();
    const ctaLink = page.getByRole("link", { name: /get started|sign up|try/i }).first();
    await expect(ctaLink).toBeVisible({ timeout: 5000 });
  });

  test("pricing page is accessible", async ({ page }) => {
    await page.goto("/pricing");
    await expect(page.locator("body")).toBeVisible();

    // Pricing page has its own heading
    const heading = page.getByRole("heading", { level: 1 });
    await expect(heading).toBeVisible();
  });

  test("unauthenticated user is redirected from app routes", async ({
    page,
  }) => {
    await page.goto("/app/backtests");
    await page.waitForURL(/sign-in|clerk/);
  });

  test("sign-in page is accessible", async ({ page }) => {
    await page.goto("/sign-in");
    await expect(page.locator("body")).toBeVisible();
    await expect(page).toHaveURL(/sign-in|clerk/);
  });

  test("pricing page renders all three plan cards", async ({ page }) => {
    await page.goto("/pricing");

    const heading = page.getByRole("heading", { level: 1 });
    await expect(heading).toBeVisible();

    for (const planName of ["Free", "Pro", "Premium"]) {
      const card = page.getByText(planName, { exact: true }).first();
      await expect(card).toBeVisible({ timeout: 5000 });
    }

    await expect(page.getByText("$0")).toBeVisible();
    await expect(page.getByText("$29/mo")).toBeVisible();
    await expect(page.getByText("$79/mo")).toBeVisible();
  });

  test("pricing page has checkout buttons for paid plans", async ({
    page,
  }) => {
    await page.goto("/pricing");

    const monthlyButtons = page.getByRole("button", {
      name: /start monthly plan/i,
    });
    await expect(monthlyButtons.first()).toBeVisible({ timeout: 5000 });
    expect(await monthlyButtons.count()).toBeGreaterThanOrEqual(2);
  });

  test("dashboard redirects unauthenticated user", async ({ page }) => {
    await page.goto("/app/dashboard");
    await page.waitForURL(/sign-in|clerk/, { timeout: 10000 });
  });

  test("scanner route redirects unauthenticated user", async ({ page }) => {
    await page.goto("/app/scanner");
    await page.waitForURL(/sign-in|clerk/, { timeout: 10000 });
  });

  test("landing page navigation links work", async ({ page }) => {
    await page.goto("/");
    const pricingLink = page.getByRole("link", { name: /pricing/i }).first();
    await expect(pricingLink).toBeVisible();

    await pricingLink.click();
    await expect(page).toHaveURL(/pricing/);
    await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
  });
});
