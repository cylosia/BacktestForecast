import * as fs from "fs";
import * as path from "path";
import { test, expect } from "@playwright/test";

const authFile = path.join(__dirname, ".auth", "user.json");
const hasAuth = fs.existsSync(authFile);

test.describe("Daily picks", () => {
  test.use({ storageState: authFile });

  test.skip(!hasAuth, "Auth state file not found. Set CLERK_TEST_EMAIL and CLERK_TEST_PASSWORD to run authenticated tests.");

  test("daily picks page loads", async ({ page }) => {
    await page.goto("/app/daily-picks");
    await expect(page.getByRole("heading", { name: /today'?s top trades|daily picks|picks for/i })).toBeVisible();
  });

  test("shows pick cards or appropriate message", async ({ page }) => {
    await page.goto("/app/daily-picks");
    await expect(page.getByRole("heading", { name: /today'?s top trades|daily picks|picks for/i })).toBeVisible();

    // Either pick cards (with symbol/score) or a message (no data, upgrade, error)
    const pickCard = page.locator('[class*="card"]').filter({ hasText: /ROI|win rate|score/i }).first();
    const noDataMessage = page.getByText(/no pipeline data|no recommendations|upgrade|unable to load/i);

    const hasCards = await pickCard.isVisible().catch(() => false);
    const hasMessage = await noDataMessage.isVisible().catch(() => false);

    expect(hasCards || hasMessage).toBeTruthy();
  });

  test("history pagination surfaces next_cursor in the page URL when a next page exists", async ({ page }) => {
    await page.goto("/app/daily-picks");
    await expect(page.getByRole("heading", { name: /today'?s top trades|daily picks|picks for/i })).toBeVisible();

    const nextLink = page.getByRole("link", { name: "Next" });
    if ((await nextLink.count()) === 0 || !(await nextLink.first().isVisible().catch(() => false))) {
      test.skip(true, "No history next page available in the current fixture data.");
    }

    await nextLink.first().click();
    await expect(page).toHaveURL(/next_cursor=/);
  });
});
