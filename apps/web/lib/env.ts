function requireValue(value: string | undefined, fallback: string): string {
  return value && value.trim().length > 0 ? value : fallback;
}

const isDev = process.env.NODE_ENV !== "production";

export const env = {
  appUrl: requireValue(
    process.env.NEXT_PUBLIC_APP_URL,
    isDev ? "http://localhost:3000" : "",
  ),
  apiBaseUrl: requireValue(
    process.env.NEXT_PUBLIC_API_BASE_URL,
    isDev ? "http://localhost:8000" : "",
  ),
  clerkPublishableKey: requireValue(
    process.env.NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY,
    "",
  ),
};
