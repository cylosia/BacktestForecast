import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import { compareBacktests } from "@/lib/api/server";

export const dynamic = "force-dynamic";
import { ApiError } from "@/lib/api/shared";
import {
  formatCurrency,
  formatDate,
  formatNumber,
  formatPercent,
  strategyLabel,
  type NumericValue,
} from "@/lib/backtests/format";
import type { BacktestRunDetailResponse, BacktestSummaryResponse } from "@backtestforecast/api-client";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { CompareEquityCurves } from "@/components/backtests/compare-equity-curves";

const GRID_COLS: Record<number, string> = {
  1: "md:grid-cols-1",
  2: "md:grid-cols-2",
  3: "md:grid-cols-3",
};

function safeCurrency(v: unknown): string {
  return v != null ? formatCurrency(v as NumericValue) : "—";
}
function safePercent(v: unknown): string {
  return v != null ? formatPercent(v as NumericValue) : "—";
}
function safeNum(v: unknown): string {
  return v != null ? formatNumber(v as NumericValue) : "—";
}
function safeRatio(v: unknown): string {
  if (v == null) return "—";
  const n = Number(v);
  return Number.isFinite(n) ? n.toFixed(2) : "—";
}

const METRIC_ROWS: Array<{
  label: string;
  key: keyof BacktestSummaryResponse;
  format: (value: unknown) => string;
  higherIsBetter?: boolean;
}> = [
  { label: "Trades", key: "trade_count", format: safeNum },
  { label: "Win rate", key: "win_rate", format: safePercent, higherIsBetter: true },
  { label: "Total ROI", key: "total_roi_pct", format: safePercent, higherIsBetter: true },
  { label: "Net P&L", key: "total_net_pnl", format: safeCurrency, higherIsBetter: true },
  { label: "Max drawdown", key: "max_drawdown_pct", format: safePercent, higherIsBetter: false },
  { label: "Sharpe ratio", key: "sharpe_ratio", format: safeRatio, higherIsBetter: true },
  { label: "Sortino ratio", key: "sortino_ratio", format: safeRatio, higherIsBetter: true },
  { label: "Profit factor", key: "profit_factor", format: safeRatio, higherIsBetter: true },
  { label: "Expectancy", key: "expectancy", format: safeCurrency, higherIsBetter: true },
  { label: "CAGR", key: "cagr_pct", format: safePercent, higherIsBetter: true },
  { label: "Payoff ratio", key: "payoff_ratio", format: safeRatio, higherIsBetter: true },
  { label: "Calmar ratio", key: "calmar_ratio", format: safeRatio, higherIsBetter: true },
  { label: "Recovery factor", key: "recovery_factor", format: safeRatio, higherIsBetter: true },
  { label: "Max consec. wins", key: "max_consecutive_wins", format: safeNum },
  { label: "Max consec. losses", key: "max_consecutive_losses", format: safeNum },
  { label: "Avg win", key: "average_win_amount", format: safeCurrency, higherIsBetter: true },
  { label: "Avg loss", key: "average_loss_amount", format: safeCurrency },
  { label: "Avg hold (days)", key: "average_holding_period_days", format: safeNum },
  { label: "Avg DTE at open", key: "average_dte_at_open", format: safeNum },
  { label: "Total commissions", key: "total_commissions", format: safeCurrency },
  { label: "Starting equity", key: "starting_equity", format: safeCurrency },
  { label: "Ending equity", key: "ending_equity", format: safeCurrency, higherIsBetter: true },
];

// max_drawdown_pct is stored as a positive magnitude (e.g. 5 means -5% drawdown),
// so higherIsBetter: false correctly selects the smallest (best) drawdown.
function bestIndex(runs: BacktestRunDetailResponse[], key: keyof BacktestSummaryResponse, higherIsBetter: boolean): number {
  let best = -1;
  let bestVal = 0;
  for (let i = 0; i < runs.length; i++) {
    const raw = runs[i].summary[key];
    if (raw == null) continue;
    const n = typeof raw === "number" ? raw : Number(raw);
    const val = Number.isFinite(n) ? n : 0;
    if (best === -1 || (higherIsBetter ? val > bestVal : val < bestVal)) {
      best = i;
      bestVal = val;
    }
  }
  return best;
}

function runLabel(run: BacktestRunDetailResponse): string {
  return `${run.symbol} ${strategyLabel(run.strategy_type)}`;
}

export default async function ComparePage({
  searchParams,
}: {
  searchParams: Promise<{ ids?: string }>;
}) {
  const params = await searchParams;
  const idsParam = params.ids ?? "";
  const runIds = idsParam.split(",").filter(Boolean);

  if (runIds.length < 2) {
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
            <CardTitle>Select runs to compare</CardTitle>
            <CardDescription>
              Go to the history page and select 2 or more completed runs to compare side-by-side.
            </CardDescription>
          </CardHeader>
        </Card>
      </div>
    );
  }

  try {
    const data = await compareBacktests(runIds);
    const runs = data.items;

    if (runs.length === 0) {
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
              <CardTitle>No matching runs found</CardTitle>
              <CardDescription>
                The selected runs may have been deleted or are no longer available.
                Go back and select different runs to compare.
              </CardDescription>
            </CardHeader>
          </Card>
        </div>
      );
    }

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
            <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Compare</p>
            <h1 className="mt-2 text-3xl font-semibold tracking-tight">
              Side-by-side comparison
            </h1>
            <p className="mt-2 text-muted-foreground">
              Comparing {runs.length} runs (plan limit: {data.comparison_limit}).
            </p>
          </div>
        </div>

        {/* Run identity cards */}
        <div className={`grid gap-4 ${GRID_COLS[runs.length] ?? "md:grid-cols-2 xl:grid-cols-4"}`}>
          {runs.map((run, index) => (
            <Card key={run.id}>
              <CardHeader className="pb-2">
                <CardTitle className="flex items-center gap-2 text-base">
                  <Badge variant="secondary" className="text-xs">
                    {String.fromCharCode(65 + index)}
                  </Badge>
                  <Link href={`/app/backtests/${run.id}`} className="hover:text-primary">
                    {runLabel(run)}
                  </Link>
                </CardTitle>
                <CardDescription>
                  {formatDate(run.date_from)} – {formatDate(run.date_to)} · {run.target_dte} DTE
                </CardDescription>
              </CardHeader>
              <CardContent>
                <p className="text-sm text-muted-foreground">
                  Account {formatCurrency(run.account_size)} · {formatPercent(run.risk_per_trade_pct)} risk ·{" "}
                  {formatCurrency(run.commission_per_contract)} commission
                </p>
              </CardContent>
            </Card>
          ))}
        </div>

        {/* Metrics comparison table */}
        <Card>
          <CardHeader>
            <CardTitle>Metrics comparison</CardTitle>
            <CardDescription>
              Bold values highlight the best performer per metric across all compared runs.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Metric</TableHead>
                  {runs.map((run, index) => (
                    <TableHead key={run.id}>
                      <span className="mr-2 inline-flex h-5 w-5 items-center justify-center rounded-full bg-primary/10 text-xs font-semibold text-primary">
                        {String.fromCharCode(65 + index)}
                      </span>
                      {run.symbol}
                    </TableHead>
                  ))}
                </TableRow>
              </TableHeader>
              <TableBody>
                {METRIC_ROWS.map((metric) => {
                  const best =
                    metric.higherIsBetter !== undefined
                      ? bestIndex(runs, metric.key, metric.higherIsBetter)
                      : -1;

                  return (
                    <TableRow key={metric.key}>
                      <TableCell className="text-muted-foreground">{metric.label}</TableCell>
                      {runs.map((run, index) => (
                        <TableCell
                          key={run.id}
                          className={index === best ? "font-semibold text-primary" : ""}
                        >
                          {metric.format(run.summary[metric.key])}
                        </TableCell>
                      ))}
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        {/* Overlaid equity curves */}
        <CompareEquityCurves runs={runs} />

        {/* Trade count summary */}
        <Card>
          <CardHeader>
            <CardTitle>Trade counts</CardTitle>
          </CardHeader>
          <CardContent>
            <div className={`grid gap-4 ${GRID_COLS[runs.length] ?? "md:grid-cols-2 xl:grid-cols-4"}`}>
              {runs.map((run, index) => (
                <div key={run.id} className="rounded-xl border border-border/70 p-4">
                  <div className="flex items-center gap-2">
                    <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-primary/10 text-xs font-semibold text-primary">
                      {String.fromCharCode(65 + index)}
                    </span>
                    <p className="font-medium">{runLabel(run)}</p>
                  </div>
                  <p className="mt-2 text-2xl font-semibold tracking-tight">{run.trades.length} trades</p>
                  <p className="mt-1 text-sm text-muted-foreground">
                    {formatPercent(run.summary.win_rate)} win rate · Max DD {formatPercent(run.summary.max_drawdown_pct)}
                  </p>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
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
            <CardTitle>Unable to load comparison</CardTitle>
            <CardDescription>The API request for the comparison did not succeed.</CardDescription>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground">{message}</p>
          </CardContent>
        </Card>
      </div>
    );
  }
}
