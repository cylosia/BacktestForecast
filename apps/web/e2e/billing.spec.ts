import * as fs from "fs";
import * as path from "path";
import { test, expect } from "@playwright/test";

const authFile = path.join(__dirname, ".auth", "user.json");
const hasAuth = fs.existsSync(authFile);

test.describe("Billing", () => {
  test.use({ storageState: authFile });

  test.skip(!hasAuth, "Auth state file not found. Set CLERK_TEST_EMAIL and CLERK_TEST_PASSWORD to run authenticated tests.");

  test("billing settings page loads with plan information", async ({ page }) => {
    await page.goto("/app/settings/billing");
    await expect(page.getByRole("heading", { name: /billing settings/i })).toBeVisible();
    await expect(page.getByText(/current plan|plan tier/i)).toBeVisible();
  });
});
