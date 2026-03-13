import * as fs from "fs";
import * as path from "path";
import { test, expect } from "@playwright/test";

const authFile = path.join(__dirname, ".auth", "user.json");
const hasAuth = fs.existsSync(authFile);

test.describe("Backtest creation", () => {
  test.use({ storageState: authFile });

  test.skip(!hasAuth, "Auth state file not found. Set CLERK_TEST_EMAIL and CLERK_TEST_PASSWORD to run authenticated tests.");

  test("backtest form page loads with strategy selector", async ({ page }) => {
    await page.goto("/app/backtests/new");
    await expect(page.getByRole("heading", { name: /create a new backtest/i })).toBeVisible();
    await expect(page.getByLabel("Symbol")).toBeVisible();
    await expect(page.getByLabel("Strategy")).toBeVisible();

    const strategySelect = page.locator("#strategyType");
    await expect(strategySelect).toBeVisible();
    const options = await strategySelect.locator("option").all();
    expect(options.length).toBeGreaterThan(1);
  });

  test("accepts valid symbol AAPL", async ({ page }) => {
    await page.goto("/app/backtests/new");
    const symbolInput = page.getByLabel("Symbol");
    await symbolInput.clear();
    await symbolInput.fill("AAPL");
    await expect(symbolInput).toHaveValue("AAPL");
  });

  test("shows validation error when submitting with empty symbol", async ({ page }) => {
    await page.goto("/app/backtests/new");
    const symbolInput = page.getByLabel("Symbol");
    await symbolInput.clear();

    await page.getByRole("button", { name: /create backtest/i }).click();

    await expect(page.getByText("Symbol is required.")).toBeVisible();
  });

  test("shows validation error for invalid symbol", async ({ page }) => {
    await page.goto("/app/backtests/new");
    const symbolInput = page.getByLabel("Symbol");
    await symbolInput.clear();
    await symbolInput.fill("!!!");

    await page.getByRole("button", { name: /create backtest/i }).click();

    await expect(
      page.getByText(/symbol may only contain letters, digits, dots, slashes, or/i),
    ).toBeVisible();
  });
});
