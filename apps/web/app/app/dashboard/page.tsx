import Link from "next/link";
import { ArrowRight, CreditCard, History, PlusCircle, ScanSearch, Sparkles } from "lucide-react";
import { getBacktestHistory, getCurrentUser } from "@/lib/api/server";
import { buildBacktestQuota } from "@/lib/backtests/quota";
import {
  formatCurrency,
  formatDateTime,
  formatPercent,
  strategyLabel,
} from "@/lib/backtests/format";
import { UpgradePrompt } from "@/components/billing/upgrade-prompt";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { HistoryList } from "@/components/backtests/history-list";
import { planLabel } from "@/lib/plan";

export default async function DashboardPage() {
  const [userResult, historyResult] = await Promise.allSettled([getCurrentUser(), getBacktestHistory(10)]);

  if (userResult.status === "rejected") {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-3xl font-semibold tracking-tight">Dashboard</h1>
          <p className="mt-2 text-muted-foreground">The app is signed in, but the API data could not be loaded.</p>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Unable to load dashboard data</CardTitle>
            <CardDescription>
              Check Clerk environment values, API availability, and CORS settings, then refresh.
            </CardDescription>
          </CardHeader>
        </Card>
      </div>
    );
  }

  const user = userResult.value;
  const history = historyResult.status === "fulfilled" ? (historyResult.value?.items ?? []) : [];
  const quota = buildBacktestQuota(user);
  const latestRun = history.find((r) => r.status === "succeeded") ?? null;

  return (
    <div className="space-y-8">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Dashboard</p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight sm:text-4xl">Welcome back</h1>
          <p className="mt-2 max-w-2xl text-muted-foreground">
            Signed in as {user.email ?? user.clerk_user_id}. Your account is currently on the {planLabel(user.plan_tier)} plan.
          </p>
        </div>

        <div className="flex flex-wrap gap-3">
          <Button asChild variant="outline">
            <Link href="/app/backtests">
              <History className="h-4 w-4" />
              View history
            </Link>
          </Button>
          <Button asChild>
            <Link href="/app/backtests/new">
              <PlusCircle className="h-4 w-4" />
              New backtest
            </Link>
          </Button>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Monthly backtests</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-3xl font-semibold tracking-tight">
              {quota.limit === null ? `${quota.used}` : `${quota.used} / ${quota.limit}`}
            </p>
            <p className="mt-1 text-sm text-muted-foreground">
              {quota.remaining === null ? "Unlimited on this plan" : `Remaining: ${quota.remaining}`}
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Scanner access</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-3xl font-semibold tracking-tight">
              {(user.features.scanner_modes ?? []).length > 0 ? (user.features.scanner_modes ?? []).join(" + ") : "None"}
            </p>
            <p className="mt-1 text-sm text-muted-foreground">Basic scanner is available on Pro. Advanced requires Premium.</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Forecast access</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-3xl font-semibold tracking-tight">{user.features.forecasting_access ? "Enabled" : "Locked"}</p>
            <p className="mt-1 text-sm text-muted-foreground">Historical-analog ranges are gated by plan.</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Export formats</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-3xl font-semibold tracking-tight">
              {(user.features.export_formats ?? []).length > 0 ? (user.features.export_formats ?? []).join(", ") : "None"}
            </p>
            <p className="mt-1 text-sm text-muted-foreground">CSV starts on Pro. PDF export requires Premium.</p>
          </CardContent>
        </Card>
      </div>

      {user.plan_tier === "free" && quota.reached ? (
        <UpgradePrompt
          message={`You have used all ${quota.limit} free backtests this month. Upgrade to Pro for unlimited backtests, CSV exports, and scanner access.`}
        />
      ) : user.plan_tier === "free" && quota.remaining !== null && quota.remaining <= 2 && quota.remaining > 0 ? (
        <div className="rounded-xl border border-amber-500/30 bg-amber-500/5 p-4 text-sm text-amber-700 dark:text-amber-400">
          You have {quota.remaining} backtest{quota.remaining === 1 ? "" : "s"} remaining this month on the Free plan.{" "}
          <Link href="/pricing" className="font-medium underline underline-offset-2 hover:text-primary">
            View upgrade options
          </Link>
        </div>
      ) : null}

      <div className="grid gap-6 lg:grid-cols-[1.1fr_0.9fr]">
        <Card>
          <CardHeader>
            <CardTitle>Recent runs</CardTitle>
            <CardDescription>
              History depth is currently limited to {user.features?.history_item_limit ?? 50} items{user.features?.history_days ? ` / ${user.features.history_days} days` : ""} for this plan.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <HistoryList embedded items={history} />
          </CardContent>
        </Card>

        <div className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle>Latest run snapshot</CardTitle>
              <CardDescription>
                {latestRun ? `Most recent completed run at ${formatDateTime(latestRun.created_at)}.` : "No successful runs yet."}
              </CardDescription>
            </CardHeader>
            <CardContent>
              {latestRun ? (
                <div className="space-y-4">
                  <div>
                    <p className="text-lg font-semibold">
                      {latestRun.symbol} {strategyLabel(latestRun.strategy_type)}
                    </p>
                    <p className="text-sm text-muted-foreground">ROI {formatPercent(latestRun.summary.total_roi_pct)}</p>
                  </div>
                  <div className="grid grid-cols-2 gap-4 text-sm">
                    <div className="rounded-xl border border-border/70 p-4">
                      <p className="text-muted-foreground">Trades</p>
                      <p className="mt-2 text-xl font-semibold">{latestRun.summary.trade_count}</p>
                    </div>
                    <div className="rounded-xl border border-border/70 p-4">
                      <p className="text-muted-foreground">Net P&amp;L</p>
                      <p className="mt-2 text-xl font-semibold">{formatCurrency(latestRun.summary.total_net_pnl)}</p>
                    </div>
                  </div>
                  <Button asChild className="w-full">
                    <Link href={`/app/backtests/${latestRun.id}`}>
                      Open result detail
                      <ArrowRight className="h-4 w-4" />
                    </Link>
                  </Button>
                </div>
              ) : (
                <div className="space-y-3 text-sm text-muted-foreground">
                  <p>Create a manual backtest to populate your research workspace.</p>
                  <Button asChild>
                    <Link href="/app/backtests/new">
                      <Sparkles className="h-4 w-4" />
                      Start your first run
                    </Link>
                  </Button>
                </div>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Plan actions</CardTitle>
              <CardDescription>Upgrade for broader history, exports, and scanner access.</CardDescription>
            </CardHeader>
            <CardContent className="flex flex-wrap gap-3">
              <Button asChild variant="outline">
                <Link href="/pricing">
                  <CreditCard className="h-4 w-4" />
                  View pricing
                </Link>
              </Button>
              <Button asChild variant="outline">
                <Link href="/app/scanner">
                  <ScanSearch className="h-4 w-4" />
                  Scanner access
                </Link>
              </Button>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
