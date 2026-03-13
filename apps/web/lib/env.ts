const isDev = process.env.NODE_ENV !== "production";

function requireValue(
  value: string | undefined,
  fallback: string,
  name?: string,
): string {
  const result = value && value.trim().length > 0 ? value : fallback;
  if (!result && name) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return result;
}

export const env = {
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
