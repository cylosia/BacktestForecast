const isDev = process.env.NODE_ENV !== "production";

/**
 * Environment variables.
 *
 * --- Client-safe (NEXT_PUBLIC_*) ---
 * These are exposed to the browser and must not contain secrets.
 * - NEXT_PUBLIC_APP_URL
 * - NEXT_PUBLIC_API_BASE_URL
 * - NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY
 *
 * --- Server-only ---
 * Use process.env directly in server components, API routes, or getServerSideProps.
 * Never prefix with NEXT_PUBLIC_ to avoid leaking to the client.
 */

function requireValue(
  value: string | undefined,
  fallback: string,
  name: string,
): string {
  const trimmed = value?.trim();
  const result = trimmed && trimmed.length > 0 ? trimmed : fallback;
  if (!result) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return result;
}

/** Client-safe env keys (NEXT_PUBLIC_* only). Safe to use in client components. */
export type ClientSafeEnv = {
  appUrl: string;
  apiBaseUrl: string;
  clerkPublishableKey: string;
};

export const env: ClientSafeEnv = {
  appUrl: requireValue(
    process.env.NEXT_PUBLIC_APP_URL,
    isDev ? "http://localhost:3000" : "",
    "NEXT_PUBLIC_APP_URL",
  ),
  apiBaseUrl: requireValue(
    process.env.NEXT_PUBLIC_API_BASE_URL,
    isDev ? "http://localhost:8000" : "",
    "NEXT_PUBLIC_API_BASE_URL",
  ),
  clerkPublishableKey: requireValue(
    process.env.NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY,
    "",
    "NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY",
  ),
};
