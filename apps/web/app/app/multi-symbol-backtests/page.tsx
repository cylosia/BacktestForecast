import Link from "next/link";
import { getCurrentUser, getMultiSymbolBacktestHistory } from "@/lib/api/server";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { SelectableMultiSymbolHistoryList } from "@/components/backtests/selectable-multi-symbol-history-list";
import { PaginationControls } from "@/components/shared/pagination-controls";

export const dynamic = "force-dynamic";

const PAGE_SIZE = 20;

export default async function MultiSymbolBacktestsPage({
  searchParams,
}: {
  searchParams: Promise<{ offset?: string; limit?: string; cursor?: string }>;
}) {
  const params = await searchParams;
  const offset = Math.max(0, parseInt(params.offset ?? "0", 10) || 0);
  const limit = Math.min(100, Math.max(1, parseInt(params.limit ?? String(PAGE_SIZE), 10) || PAGE_SIZE));
  const cursor = params.cursor?.trim() || undefined;
  const [user, history] = await Promise.all([
    getCurrentUser(),
    getMultiSymbolBacktestHistory(limit, offset, cursor),
  ]);

  return (
    <div className="space-y-6">
      <div className="flex items-end justify-between gap-4">
        <div>
          <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Alpha workflows</p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Multi-Symbol backtests</h1>
          <p className="mt-2 text-muted-foreground">Create and review coordinated multi-underlying option workflows.</p>
        </div>
        <Button asChild>
          <Link href="/app/multi-symbol-backtests/new">New multi-symbol run</Link>
        </Button>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Run history</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <SelectableMultiSymbolHistoryList
            items={history.items}
            comparisonLimit={user.features?.side_by_side_comparison_limit ?? 2}
          />
          <PaginationControls
            basePath="/app/multi-symbol-backtests"
            offset={offset}
            limit={limit}
            total={history.total}
            cursor={cursor}
            nextCursor={history.next_cursor}
          />
        </CardContent>
      </Card>
    </div>
  );
}
