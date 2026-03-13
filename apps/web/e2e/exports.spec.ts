import * as fs from "fs";
import * as path from "path";
import { test, expect } from "@playwright/test";

const authFile = path.join(__dirname, ".auth", "user.json");
const hasAuth = fs.existsSync(authFile);

test.describe("Exports / Backtests list", () => {
  test.use({ storageState: authFile });

  test.skip(!hasAuth, "Auth state file not found. Set CLERK_TEST_EMAIL and CLERK_TEST_PASSWORD to run authenticated tests.");

  test("backtests list page loads", async ({ page }) => {
    await page.goto("/app/backtests");
    await expect(page.getByRole("heading", { name: /backtest runs/i })).toBeVisible();
  });
});
