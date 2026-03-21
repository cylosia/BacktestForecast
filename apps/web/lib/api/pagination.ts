export function buildPaginatedListPath(
  basePath: string,
  limit: number,
  offset: number,
  maxLimit: number,
  cursor?: string | null,
): string {
  const params = new URLSearchParams();
  params.set("limit", String(Math.max(1, Math.min(limit, maxLimit))));
  if (cursor && cursor.trim().length > 0) {
    params.set("cursor", cursor);
  } else {
    params.set("offset", String(Math.max(0, offset)));
  }
  return `${basePath}?${params.toString()}`;
}

export function buildCursorPaginatedPath(
  basePath: string,
  limit: number,
  maxLimit: number,
  cursor?: string | null,
): string {
  const params = new URLSearchParams();
  params.set("limit", String(Math.max(1, Math.min(limit, maxLimit))));
  if (cursor && cursor.trim().length > 0) {
    params.set("cursor", cursor);
  }
  return `${basePath}?${params.toString()}`;
}
