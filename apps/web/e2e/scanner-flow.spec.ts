import * as fs from "fs";
import * as path from "path";
import { test, expect } from "@playwright/test";

const authFile = path.join(__dirname, ".auth", "user.json");
const hasAuth = fs.existsSync(authFile);

test.describe("Scanner flow", () => {
  test.use({ storageState: authFile });

  test.skip(!hasAuth, "Auth state file not found. Set CLERK_TEST_EMAIL and CLERK_TEST_PASSWORD to run authenticated tests.");

  test("scanner page loads", async ({ page }) => {
    await page.goto("/app/scanner");
    await expect(page.getByRole("heading", { name: /strategy scanner/i })).toBeVisible();
  });

  test("new scan button navigates to scanner form when available", async ({ page }) => {
    await page.goto("/app/scanner");

    const newScanBtn = page.getByRole("link", { name: /new scan/i }).or(page.getByRole("button", { name: /new scan|create your first scan/i }));
    if (await newScanBtn.first().isVisible().catch(() => false)) {
      await newScanBtn.first().click();
      await expect(page).toHaveURL(/\/app\/scanner\/new/);

      // Scanner form elements: symbol input, strategy selector, etc.
      await expect(page.getByRole("heading", { name: /create a new scan/i })).toBeVisible();
      const symbolsInput = page.locator("#symbols").or(page.getByLabel(/symbols/i));
      const strategySection = page.getByText(/strategy types/i);
      await expect(symbolsInput.first().or(strategySection)).toBeVisible();
    } else {
      // User may not have scanner access - page still loads
      await expect(page.getByRole("heading", { name: /strategy scanner/i })).toBeVisible();
    }
  });
});
