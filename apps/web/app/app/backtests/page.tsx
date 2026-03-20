import Link from "next/link";
import { getBacktestHistory, getCurrentUser } from "@/lib/api/server";
import { buildBacktestQuota } from "@/lib/backtests/quota";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { SelectableHistoryList } from "@/components/backtests/selectable-history-list";
import { PaginationControls } from "@/components/shared/pagination-controls";

export const dynamic = "force-dynamic";

const PAGE_SIZE = 20;

export default async function BacktestsPage({
  searchParams,
}: {
  searchParams: Promise<{ offset?: string; limit?: string }>;
}) {
  const params = await searchParams;
  const offset = Math.max(0, parseInt(params.offset ?? "0", 10) || 0);
  const limit = Math.min(100, Math.max(1, parseInt(params.limit ?? String(PAGE_SIZE), 10) || PAGE_SIZE));

  try {
    const [user, history] = await Promise.all([getCurrentUser(), getBacktestHistory(limit, offset)]);
    const quota = buildBacktestQuota(user);

    return (
      <div className="space-y-6">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">History</p>
            <h1 className="mt-2 text-3xl font-semibold tracking-tight">Backtest runs</h1>
            <p className="mt-2 text-muted-foreground">Review previous runs, or select completed runs to compare side-by-side.</p>
          </div>
          <Button asChild>
            <Link href="/app/backtests/new">New backtest</Link>
          </Button>
        </div>

        <div className="grid gap-4 md:grid-cols-3">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">Monthly usage</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-3xl font-semibold tracking-tight">
                {quota.limit === null ? quota.used : `${quota.used} / ${quota.limit}`}
              </p>
              <p className="mt-1 text-sm text-muted-foreground">
                {quota.remaining === null ? "Unlimited on this plan" : `Remaining this month: ${quota.remaining}`}
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">History depth</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-3xl font-semibold tracking-tight">{user.features?.history_item_limit}</p>
              <p className="mt-1 text-sm text-muted-foreground">
                {user.features?.history_days ? `${user.features.history_days} day window` : "Full available history"}
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">Comparison slots</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-3xl font-semibold tracking-tight">{user.features?.side_by_side_comparison_limit}</p>
              <p className="mt-1 text-sm text-muted-foreground">Max runs per side-by-side comparison.</p>
            </CardContent>
          </Card>
        </div>

        <Card>
          <CardContent className="pt-6">
            <SelectableHistoryList
              items={history.items}
              comparisonLimit={user.features?.side_by_side_comparison_limit}
            />
            <PaginationControls
              basePath="/app/backtests"
              offset={offset}
              limit={limit}
              total={history.total}
            />
          </CardContent>
        </Card>
      </div>
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "History could not be loaded.";

    return (
      <div className="space-y-6">
        <div>
          <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">History</p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Backtest runs</h1>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Unable to load history</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground">{message}</p>
          </CardContent>
        </Card>
      </div>
    );
  }
}
