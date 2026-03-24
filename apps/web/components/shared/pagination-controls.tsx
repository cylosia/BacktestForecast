"use client";

import Link from "next/link";
import { Button } from "@/components/ui/button";

interface PaginationControlsProps {
  basePath: string;
  offset: number;
  limit: number;
  total?: number;
  cursor?: string | null;
  nextCursor?: string | null;
  extraParams?: Record<string, string>;
  cursorParamName?: string;
}

function buildHref(
  basePath: string,
  offset: number,
  limit: number,
  extra?: Record<string, string>,
  cursor?: string | null,
  cursorParamName = "cursor",
): string {
  const params = new URLSearchParams();
  if (cursor && cursor.trim().length > 0) {
    params.set(cursorParamName, cursor);
  } else if (offset > 0) {
    params.set("offset", String(offset));
  }
  if (limit !== 20) params.set("limit", String(limit));
  if (extra) {
    for (const [k, v] of Object.entries(extra)) params.set(k, v);
  }
  const qs = params.toString();
  return qs ? `${basePath}?${qs}` : basePath;
}

export function PaginationControls({
  basePath,
  offset,
  limit,
  total,
  cursor,
  nextCursor,
  extraParams,
  cursorParamName = "cursor",
}: PaginationControlsProps) {
  const usingCursor = Boolean(cursor || nextCursor);
  if (!usingCursor && (total ?? 0) <= limit && offset === 0) return null;
  if (usingCursor && !cursor && !nextCursor) return null;

  const currentPage = Math.floor(offset / limit) + 1;
  const totalPages = Math.max(1, Math.ceil((total ?? 0) / limit));
  const hasPrev = usingCursor ? Boolean(cursor) : offset > 0;
  const hasNext = usingCursor ? Boolean(nextCursor) && (total == null || offset + limit < total) : offset + limit < (total ?? 0);

  return (
    <div className="flex items-center justify-between pt-4">
      <p className="text-sm text-muted-foreground">
        {usingCursor
          ? total != null
            ? `Showing ${Math.min(offset + 1, total)}-${Math.min(offset + limit, total)} of ${total}`
            : `Showing up to ${limit} items`
          : `Showing ${offset + 1}-${Math.min(offset + limit, total ?? 0)} of ${total ?? 0}`}
      </p>
      <div className="flex items-center gap-2">
        {hasPrev ? (
          <Button variant="outline" size="sm" asChild>
            <Link
              href={
                usingCursor
                  ? buildHref(basePath, 0, limit, extraParams, undefined, cursorParamName)
                  : buildHref(basePath, Math.max(0, offset - limit), limit, extraParams)
              }
            >
              {usingCursor ? "First page" : "Previous"}
            </Link>
          </Button>
        ) : (
          <Button variant="outline" size="sm" disabled>
            {usingCursor ? "First page" : "Previous"}
          </Button>
        )}
        <span className="text-sm text-muted-foreground">
          {usingCursor ? (total != null ? `Page ${currentPage} of ${totalPages}` : "Cursor pagination") : `Page ${currentPage} of ${totalPages}`}
        </span>
        {hasNext ? (
          <Button variant="outline" size="sm" asChild>
            <Link
              href={
                usingCursor
                  ? buildHref(basePath, 0, limit, extraParams, nextCursor, cursorParamName)
                  : buildHref(basePath, offset + limit, limit, extraParams)
              }
            >
              Next
            </Link>
          </Button>
        ) : (
          <Button variant="outline" size="sm" disabled>
            Next
          </Button>
        )}
      </div>
    </div>
  );
}
