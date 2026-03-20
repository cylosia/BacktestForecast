export function getOrCreatePendingIdempotencyKey(
  current: string | null | undefined,
  prefix?: string,
): string {
  if (current && current.trim().length > 0) {
    return current;
  }

  const suffix = crypto.randomUUID();
  return prefix ? `${prefix}-${suffix}` : suffix;
}
