import * as fs from "fs";
import * as path from "path";
import { chromium, type FullConfig } from "@playwright/test";

const authDir = path.join(__dirname, ".auth");
const authFile = path.join(authDir, "user.json");

async function globalSetup(config: FullConfig) {
  const email = process.env.CLERK_TEST_EMAIL;
  const password = process.env.CLERK_TEST_PASSWORD;

  if (!email || !password) {
    console.warn(
      "[global-setup] CLERK_TEST_EMAIL and CLERK_TEST_PASSWORD not set. Skipping auth. Tests will run against unauthenticated pages.",
    );
    if (!fs.existsSync(authDir)) {
      fs.mkdirSync(authDir, { recursive: true });
    }
    return;
  }

  const baseURL = config.projects[0]?.use?.baseURL ?? "http://localhost:3000";
  const browser = await chromium.launch();
  const context = await browser.newContext({ baseURL });
  const page = await context.newPage();

  try {
    await page.goto("/sign-in");
    await page.waitForLoadState("networkidle");

    // Clerk SignIn: email/identifier input
    const emailInput = page.getByLabel(/email|identifier/i).first();
    await emailInput.waitFor({ state: "visible", timeout: 10_000 });
    await emailInput.fill(email);

    // Click "Continue" to proceed to password step (if Clerk uses multi-step)
    const continueBtn = page.getByRole("button", { name: /continue|next/i }).first();
    if (await continueBtn.isVisible().catch(() => false)) {
      await continueBtn.click();
      await page.waitForLoadState("networkidle");
    }

    // Password input
    const passwordInput = page.getByLabel(/password/i).first();
    await passwordInput.waitFor({ state: "visible", timeout: 5_000 });
    await passwordInput.fill(password);

    // Submit
    const submitBtn = page.getByRole("button", { name: /sign in|continue|submit/i }).first();
    await submitBtn.click();

    // Wait for redirect to app (e.g. /app/* or dashboard)
    await page.waitForURL(/\/(app|dashboard)|clerk\.com/, { timeout: 15_000 }).catch(() => {});

    // Verify we're authenticated by checking we're not on sign-in
    const url = page.url();
    if (url.includes("/sign-in") && !url.includes("clerk")) {
      throw new Error("Authentication may have failed - still on sign-in page");
    }

    await fs.promises.mkdir(authDir, { recursive: true });
    await context.storageState({ path: authFile });
    console.log("[global-setup] Auth state saved to", authFile);
  } finally {
    await context.close();
    await browser.close();
  }
}

export default globalSetup;
