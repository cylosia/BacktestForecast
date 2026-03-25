import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import { getMultiStepBacktestRun } from "@/lib/api/server";
import { ApiError } from "@/lib/api/shared";
import {
  formatCurrency,
  formatDate,
  formatNumber,
  formatPercent,
  statusLabel,
} from "@/lib/backtests/format";
import type { MultiStepRunDetailResponse } from "@/lib/api/multi-workflow-types";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

export const dynamic = "force-dynamic";

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function fmtCurrency(value: unknown): string {
  return formatCurrency(value as number | string | null | undefined);
}

function fmtNumber(value: unknown): string {
  return formatNumber(value as number | string | null | undefined);
}

function fmtPercent(value: unknown): string {
  return formatPercent(value as number | string | null | undefined);
}

function runLabel(run: MultiStepRunDetailResponse): string {
  return run.name ?? `${run.symbol} · ${run.workflow_type}`;
}

type SummaryKey =
  | "trade_count"
  | "decided_trades"
  | "win_rate"
  | "total_roi_pct"
  | "total_net_pnl"
  | "max_drawdown_pct"
  | "total_commissions";

const METRICS: Array<{
  label: string;
  key: SummaryKey;
  format: (value: unknown) => string;
}> = [
  { label: "Trades", key: "trade_count", format: fmtNumber },
  { label: "Decided trades", key: "decided_trades", format: fmtNumber },
  { label: "Win rate", key: "win_rate", format: fmtPercent },
  { label: "Total ROI", key: "total_roi_pct", format: fmtPercent },
  { label: "Net P&L", key: "total_net_pnl", format: fmtCurrency },
  { label: "Max drawdown", key: "max_drawdown_pct", format: fmtPercent },
  { label: "Commissions", key: "total_commissions", format: fmtCurrency },
];

export default async function MultiStepComparePage({
  searchParams,
}: {
  searchParams: Promise<{ ids?: string }>;
}) {
  const params = await searchParams;
  const ids = (params.ids ?? "").split(",").map((item) => item.trim()).filter((item) => UUID_RE.test(item)).slice(0, 6);

  if (ids.length < 2) {
    return (
      <div className="space-y-6">
        <Button asChild className="px-0" variant="ghost">
          <Link href="/app/multi-step-backtests">
            <ArrowLeft className="h-4 w-4" />
            Back to multi-step runs
          </Link>
        </Button>
        <Card>
          <CardHeader>
            <CardTitle>Select runs to compare</CardTitle>
            <CardDescription>Go back to history and select 2 or more completed multi-step runs.</CardDescription>
          </CardHeader>
        </Card>
      </div>
    );
  }

  try {
    const runs = await Promise.all(ids.map((id) => getMultiStepBacktestRun(id)));

    return (
      <div className="space-y-6">
        <div>
          <Button asChild className="mb-4 px-0" variant="ghost">
            <Link href="/app/multi-step-backtests">
              <ArrowLeft className="h-4 w-4" />
              Back to multi-step runs
            </Link>
          </Button>
          <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Compare</p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Multi-Step comparison</h1>
          <p className="mt-2 text-muted-foreground">Comparing {runs.length} staged workflow runs side by side.</p>
        </div>

        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          {runs.map((run) => (
            <Card key={run.id}>
              <CardHeader className="pb-2">
                <CardTitle className="text-base">{runLabel(run)}</CardTitle>
                <CardDescription>{formatDate(run.start_date)} to {formatDate(run.end_date)}</CardDescription>
              </CardHeader>
              <CardContent className="space-y-2 text-sm text-muted-foreground">
                <p>{run.symbol}</p>
                <Badge variant={run.status === "succeeded" ? "success" : "secondary"}>{statusLabel(run.status)}</Badge>
              </CardContent>
            </Card>
          ))}
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Workflow metrics</CardTitle>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Metric</TableHead>
                  {runs.map((run) => (
                    <TableHead key={run.id}>{runLabel(run)}</TableHead>
                  ))}
                </TableRow>
              </TableHeader>
              <TableBody>
                {METRICS.map((metric) => (
                  <TableRow key={metric.key}>
                    <TableCell className="text-muted-foreground">{metric.label}</TableCell>
                    {runs.map((run) => (
                      <TableCell key={`${run.id}-${metric.key}`}>{metric.format(run.summary[metric.key])}</TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Step outcomes</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {runs.map((run) => (
              <div key={`${run.id}-steps`} className="rounded-xl border border-border/70 p-4">
                <p className="font-semibold">{runLabel(run)}</p>
                <div className="mt-3 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                  {run.steps.map((step) => (
                    <div key={`${run.id}-${step.step_number}`} className="rounded-lg border border-border/60 p-3">
                      <p className="font-medium">Step {step.step_number}</p>
                      <p className="mt-1 text-sm text-muted-foreground">{step.name}</p>
                      <p className="mt-1 text-sm text-muted-foreground">{statusLabel(step.status)}</p>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </CardContent>
        </Card>
      </div>
    );
  } catch (error) {
    const message = error instanceof ApiError ? error.message : "Comparison could not be loaded.";
    return (
      <div className="space-y-6">
        <Button asChild className="px-0" variant="ghost">
          <Link href="/app/multi-step-backtests">
            <ArrowLeft className="h-4 w-4" />
            Back to multi-step runs
          </Link>
        </Button>
        <Card>
          <CardHeader>
            <CardTitle>Unable to load comparison</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground">{message}</p>
          </CardContent>
        </Card>
      </div>
    );
  }
}
