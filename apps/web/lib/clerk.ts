const AUTH_OPTIONAL_DEV_PUBLISHABLE_KEY = "pk_test_auth_optional_dev";

export function isClerkEnabled(): boolean {
  const key = process.env.NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY?.trim();
  return Boolean(key && key !== AUTH_OPTIONAL_DEV_PUBLISHABLE_KEY);
}

export function isAuthOptionalDevPlaceholder(value: string | undefined | null): boolean {
  return (value?.trim() ?? "") === AUTH_OPTIONAL_DEV_PUBLISHABLE_KEY;
}
