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

  test("submits payload fields for visible advanced TA and risk controls", async ({ page }) => {
    let capturedPayload: Record<string, unknown> | null = null;

    await page.route("**/v1/backtests", async (route) => {
      capturedPayload = route.request().postDataJSON() as Record<string, unknown>;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          id: "run_test_123",
          status: "queued",
          created_at: "2026-03-20T00:00:00Z",
        }),
      });
    });

    await page.goto("/app/backtests/new");

    await page.getByLabel(/Enable MACD rule/i).check();
    await page.getByLabel(/Enable Bollinger rule/i).check();
    await page.getByLabel(/Enable IV Rank rule/i).check();
    await page.getByLabel(/Enable IV Percentile rule/i).check();
    await page.getByLabel(/Enable volume spike rule/i).check();
    await page.getByLabel(/Enable support\/resistance rule/i).check();
    await page.getByLabel(/Enable earnings avoidance/i).check();
    await page.getByLabel(/Profit target/i).check();
    await page.getByLabel(/Stop loss/i).check();

    await page.locator("#ivRankLookbackDays").fill("300");
    await page.locator("#ivPercentileLookbackDays").fill("275");
    await page.locator("#supportResistanceMode").selectOption("breakout_above_resistance");
    await page.locator("#supportResistanceTolerancePct").fill("2.5");
    await page.locator("#avoidEarningsDaysBefore").fill("5");
    await page.locator("#avoidEarningsDaysAfter").fill("0");
    await page.locator("#slippagePct").fill("1.2");
    await page.locator("#riskFreeRate").fill("0.05");
    await page.locator("#profitTargetPct").fill("75");
    await page.locator("#stopLossPct").fill("25");

    await page.getByRole("button", { name: /create backtest/i }).click();
    await page.waitForURL(/\/app\/backtests\/run_test_123$/);

    expect(capturedPayload).toBeTruthy();
    expect(capturedPayload).toMatchObject({
      slippage_pct: 1.2,
      profit_target_pct: 75,
      stop_loss_pct: 25,
      risk_free_rate: 0.05,
    });

    const entryRules = (capturedPayload?.entry_rules ?? []) as Array<Record<string, unknown>>;
    expect(entryRules).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ type: "macd", fast_period: 12, slow_period: 26, signal_period: 9, direction: "bullish" }),
        expect.objectContaining({ type: "bollinger_bands", period: 20, standard_deviations: 2, band: "lower", operator: "lt" }),
        expect.objectContaining({ type: "iv_rank", threshold: 50, lookback_days: 300, operator: "gt" }),
        expect.objectContaining({ type: "iv_percentile", threshold: 50, lookback_days: 275, operator: "gt" }),
        expect.objectContaining({ type: "volume_spike", multiplier: 2, lookback_period: 20, operator: "gte" }),
        expect.objectContaining({ type: "support_resistance", mode: "breakout_above_resistance", lookback_period: 20, tolerance_pct: 2.5 }),
        expect.objectContaining({ type: "avoid_earnings", days_before: 5, days_after: 0 }),
      ]),
    );
  });
});
