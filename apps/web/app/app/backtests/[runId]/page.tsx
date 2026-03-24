import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import type { ExportFormat } from "@backtestforecast/api-client";
import { getBacktestRun, getCurrentUser } from "@/lib/api/server";
import { ApiError } from "@/lib/api/shared";
import {
  formatCurrency,
  formatDate,
  formatDateTime,
  formatNumber,
  formatPercent,
  isTerminalStatus,
  statusLabel,
  strategyLabel,
} from "@/lib/backtests/format";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { BacktestRunPoller } from "@/components/backtests/backtest-run-poller";
import { EquityCurveChart } from "@/components/backtests/equity-curve-chart";
import { ExportActions } from "@/components/backtests/export-actions";
import { ResultSummaryCards } from "@/components/backtests/result-summary-cards";
import { TradeListTable } from "@/components/backtests/trade-list-table";
import { statusBadgeVariant } from "@/lib/ui/status-badge";
import { getBacktestPartialDataMessages, getCancellationMessage, isTradePayloadPartial } from "@/lib/jobs/ui-state";

export const dynamic = "force-dynamic";

export default async function BacktestDetailPage({
  params,
}: {
  params: Promise<{ runId: string }>;
}) {
  const { runId } = await params;

  try {
    const [runResult, userResult] = await Promise.allSettled([getBacktestRun(runId), getCurrentUser()]);
    if (runResult.status === "rejected") throw runResult.reason;
    if (userResult.status === "rejected") throw userResult.reason;
    const run = runResult.value;
    const user = userResult.value;
    const isComplete = isTerminalStatus(run.status);
    const partialDataMessages = getBacktestPartialDataMessages(run);
    const tradePayloadPartial = isTradePayloadPartial(run.summary?.trade_count, run.trades.length);

    return (
      <div className="space-y-6">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <Button asChild className="mb-4 px-0" variant="ghost">
              <Link href="/app/backtests">
                <ArrowLeft className="h-4 w-4" />
                Back to history
              </Link>
            </Button>
            <div className="flex flex-wrap items-center gap-3">
              <h1 className="text-3xl font-semibold tracking-tight sm:text-4xl">
                {run.symbol} {strategyLabel(run.strategy_type)}
              </h1>
              <Badge variant={statusBadgeVariant(run.status)}>
                {statusLabel(run.status)}
              </Badge>
            </div>
            <p className="mt-2 text-muted-foreground">
              Created {formatDateTime(run.created_at)} · {formatDate(run.date_from)} to {formatDate(run.date_to)}
            </p>
          </div>

          <div className="space-y-3">
            <Button asChild className="w-full lg:w-auto">
              <Link href="/app/backtests/new">Run another backtest</Link>
            </Button>
            {isComplete ? (
              <ExportActions formats={(user.features.export_formats ?? []).filter((f): f is ExportFormat => f === "csv" || f === "pdf")} runId={run.id} />
            ) : null}
          </div>
        </div>

        {!isComplete ? (
          <BacktestRunPoller
            runId={runId}
            initialStatus={run.status}
          />
        ) : null}

        {isComplete ? (
          <>
            {run.summary ? <ResultSummaryCards summary={run.summary} /> : null}

        {partialDataMessages.length > 0 ? (
          <Card>
            <CardHeader>
              <CardTitle>Partial data returned</CardTitle>
              <CardDescription>
                The API returned a complete summary but only partial raw data for at least one section below.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-2">
              {partialDataMessages.map((message, index) => (
                <p key={`partial-${index}`} className="text-sm text-muted-foreground">
                  {message}
                </p>
              ))}
            </CardContent>
          </Card>
        ) : null}

        <div className="grid gap-6 xl:grid-cols-[0.95fr_1.05fr]">
          <Card>
            <CardHeader>
              <CardTitle>Run inputs</CardTitle>
              <CardDescription>Values returned from the backend detail response.</CardDescription>
            </CardHeader>
            <CardContent className="grid gap-4 sm:grid-cols-2">
              <div className="rounded-xl border border-border/70 p-4">
                <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Date range</p>
                <p className="mt-2 font-medium">
                  {formatDate(run.date_from)} – {formatDate(run.date_to)}
                </p>
              </div>
              <div className="rounded-xl border border-border/70 p-4">
                <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Target DTE</p>
                <p className="mt-2 font-medium">{run.target_dte} days</p>
              </div>
              <div className="rounded-xl border border-border/70 p-4">
                <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">DTE tolerance</p>
                <p className="mt-2 font-medium">±{run.dte_tolerance_days} days</p>
              </div>
              <div className="rounded-xl border border-border/70 p-4">
                <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Max holding days</p>
                <p className="mt-2 font-medium">{run.max_holding_days} days</p>
              </div>
              <div className="rounded-xl border border-border/70 p-4">
                <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Account size</p>
                <p className="mt-2 font-medium">{formatCurrency(run.account_size)}</p>
              </div>
              <div className="rounded-xl border border-border/70 p-4">
                <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Risk per trade</p>
                <p className="mt-2 font-medium">{formatPercent(run.risk_per_trade_pct)}</p>
              </div>
              <div className="rounded-xl border border-border/70 p-4">
                <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Commission</p>
                <p className="mt-2 font-medium">{formatCurrency(run.commission_per_contract)}</p>
              </div>
              <div className="rounded-xl border border-border/70 p-4">
                <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Engine / source</p>
                <p className="mt-2 font-medium">
                  {run.engine_version} · {run.data_source}
                </p>
              </div>
            </CardContent>
          </Card>

          {run.summary ? (
          <Card>
            <CardHeader>
              <CardTitle>Outcome snapshot</CardTitle>
              <CardDescription>Core result metrics from the stored summary payload.</CardDescription>
            </CardHeader>
            <CardContent className="grid gap-4 sm:grid-cols-2">
              <div className="rounded-xl border border-border/70 p-4">
                <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Average win</p>
                <p className="mt-2 text-xl font-semibold tracking-tight">
                  {formatCurrency(run.summary.average_win_amount)}
                </p>
              </div>
              <div className="rounded-xl border border-border/70 p-4">
                <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Average loss</p>
                <p className="mt-2 text-xl font-semibold tracking-tight">
                  {formatCurrency(run.summary.average_loss_amount)}
                </p>
              </div>
              <div className="rounded-xl border border-border/70 p-4">
                <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Average hold</p>
                <p className="mt-2 text-xl font-semibold tracking-tight">
                  {formatNumber(run.summary.average_holding_period_days)} days
                </p>
              </div>
              <div className="rounded-xl border border-border/70 p-4">
                <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Average DTE at open</p>
                <p className="mt-2 text-xl font-semibold tracking-tight">
                  {formatNumber(run.summary.average_dte_at_open)} days
                </p>
              </div>
              {run.summary.profit_factor != null ? (
              <div className="rounded-xl border border-border/70 p-4">
                <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Profit factor</p>
                <p className="mt-2 text-xl font-semibold tracking-tight">
                  {formatNumber(run.summary.profit_factor)}
                </p>
              </div>
              ) : null}
              <div className="rounded-xl border border-border/70 p-4 sm:col-span-2">
                <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Ending equity</p>
                <p className="mt-2 text-2xl font-semibold tracking-tight">
                  {formatCurrency(run.summary.ending_equity)}
                </p>
                <p className="mt-1 text-sm text-muted-foreground">
                  Starting equity {formatCurrency(run.summary.starting_equity)}
                </p>
              </div>
            </CardContent>
          </Card>
          ) : null}
        </div>

        {(run.warnings ?? []).length > 0 ? (
          <Card>
            <CardHeader>
              <CardTitle>Warnings</CardTitle>
              <CardDescription>Messages surfaced by the backend during execution.</CardDescription>
            </CardHeader>
            <CardContent className="space-y-3">
              {(run.warnings ?? []).map((warning, index) => (
                <div key={`${run.id}-warning-${index}`} className="rounded-xl border border-border/70 p-4 text-sm text-muted-foreground">
                  {typeof warning === "string"
                    ? warning
                    : typeof warning === "object" && warning !== null && "message" in warning
                      ? String((warning as Record<string, unknown>).message)
                      : "Warning details unavailable."}
                </div>
              ))}
            </CardContent>
          </Card>
        ) : null}

        {run.status === "cancelled" ? (
          <Card>
            <CardHeader>
              <CardTitle>Backtest cancelled</CardTitle>
              <CardDescription>
                This run ended in a cancelled state instead of producing a complete result set.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-muted-foreground">
                {getCancellationMessage("backtest", run.error_code)}
              </p>
            </CardContent>
          </Card>
        ) : null}

        <EquityCurveChart points={run.equity_curve} />
        <TradeListTable trades={run.trades} totalTrades={run.summary?.trade_count} truncated={tradePayloadPartial} />
          </>
        ) : null}

        {run.status === "failed" && run.error_message ? (
          <Card>
            <CardHeader>
              <CardTitle>Execution error</CardTitle>
              <CardDescription>
                The backtest failed with error code: {run.error_code ?? "unknown"}
              </CardDescription>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-muted-foreground">{run.error_message}</p>
            </CardContent>
          </Card>
        ) : null}
      </div>
    );
  } catch (error) {
    const message = error instanceof ApiError ? error.message : "This page could not be loaded. Please try again.";

    return (
      <div className="space-y-6">
        <Button asChild className="px-0" variant="ghost">
          <Link href="/app/backtests">
            <ArrowLeft className="h-4 w-4" />
            Back to history
          </Link>
        </Button>

        <Card>
          <CardHeader>
            <CardTitle>Unable to load backtest detail</CardTitle>
            <CardDescription>The API request for this run did not succeed.</CardDescription>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground">{message}</p>
          </CardContent>
        </Card>
      </div>
    );
  }
}
