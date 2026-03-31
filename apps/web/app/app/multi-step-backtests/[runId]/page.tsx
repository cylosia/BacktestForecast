import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import type { ExportFormat } from "@backtestforecast/api-client";
import { getCurrentUser, getMultiStepBacktestRun } from "@/lib/api/server";
import {
  formatCurrency,
  formatDate,
  formatDateTime,
  formatNumber,
  formatPercent,
  formatReason,
  statusLabel,
  strategyLabel,
} from "@/lib/backtests/format";
import { MultiStepRunPoller } from "@/components/backtests/multi-step-run-poller";
import { DataQualityBoundaryCard } from "@/components/backtests/data-quality-boundary-card";
import { ExportActions } from "@/components/backtests/export-actions";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

export const dynamic = "force-dynamic";

type WarningItem = {
  code: string;
  message: string;
  severity?: string | null;
};

function asWarnings(value: unknown[]): WarningItem[] {
  return value.filter((item): item is WarningItem => {
    if (item == null || typeof item !== "object") return false;
    const candidate = item as Record<string, unknown>;
    return typeof candidate.code === "string" && typeof candidate.message === "string";
  });
}

export default async function MultiStepRunPage({
  params,
}: {
  params: Promise<{ runId: string }>;
}) {
  const { runId } = await params;
  const [run, user] = await Promise.all([getMultiStepBacktestRun(runId), getCurrentUser()]);
  const warnings = asWarnings(run.warnings);

  return (
    <div className="space-y-6">
      <div>
        <div className="flex items-start justify-between gap-4">
          <div>
            <Button asChild className="mb-4 px-0" variant="ghost">
              <Link href="/app/multi-step-backtests">
                <ArrowLeft className="h-4 w-4" />
                Back to multi-step runs
              </Link>
            </Button>
            <h1 className="text-3xl font-semibold tracking-tight">{run.name ?? `${run.symbol} · ${run.workflow_type}`}</h1>
            <p className="mt-2 text-muted-foreground">
              {statusLabel(run.status)} · Created {formatDateTime(run.created_at)} · {formatDate(run.start_date)} to {formatDate(run.end_date)}
            </p>
          </div>
          {run.status === "succeeded" ? (
            <ExportActions formats={(user.features.export_formats ?? []).filter((f): f is ExportFormat => f === "csv" || f === "pdf")} runId={run.id} />
          ) : null}
        </div>
      </div>

      <MultiStepRunPoller runId={runId} initialStatus={run.status} />
      <DataQualityBoundaryCard warnings={run.warnings} />

      {run.error_message ? (
        <div className="rounded-xl border border-amber-500/40 bg-amber-500/5 p-4 text-sm text-muted-foreground">
          {run.error_message}
        </div>
      ) : null}

      {warnings.length > 0 ? (
        <Card>
          <CardHeader>
            <CardTitle>Warnings</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {warnings.map((warning) => (
              <div key={`${warning.code}-${warning.message}`} className="rounded-xl border border-amber-500/30 bg-amber-500/5 p-3">
                <p className="font-medium">{warning.code}</p>
                <p className="mt-1 text-sm text-muted-foreground">{warning.message}</p>
              </div>
            ))}
          </CardContent>
        </Card>
      ) : null}

      <div className="grid gap-4 md:grid-cols-4">
        <Card>
          <CardHeader className="pb-2"><CardTitle className="text-sm font-medium text-muted-foreground">Workflow</CardTitle></CardHeader>
          <CardContent><p className="text-xl font-semibold tracking-tight">{run.workflow_type}</p></CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2"><CardTitle className="text-sm font-medium text-muted-foreground">Trades</CardTitle></CardHeader>
          <CardContent><p className="text-3xl font-semibold tracking-tight">{formatNumber(run.summary.trade_count)}</p></CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2"><CardTitle className="text-sm font-medium text-muted-foreground">Total ROI</CardTitle></CardHeader>
          <CardContent><p className="text-3xl font-semibold tracking-tight">{formatPercent(run.summary.total_roi_pct)}</p></CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2"><CardTitle className="text-sm font-medium text-muted-foreground">Net P&L</CardTitle></CardHeader>
          <CardContent><p className="text-3xl font-semibold tracking-tight">{formatCurrency(run.summary.total_net_pnl)}</p></CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Workflow summary</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <div>
            <p className="text-sm text-muted-foreground">Win rate</p>
            <p className="mt-1 text-xl font-semibold">{formatPercent(run.summary.win_rate)}</p>
          </div>
          <div>
            <p className="text-sm text-muted-foreground">Decided trades</p>
            <p className="mt-1 text-xl font-semibold">{formatNumber(run.summary.decided_trades)}</p>
          </div>
          <div>
            <p className="text-sm text-muted-foreground">Max drawdown</p>
            <p className="mt-1 text-xl font-semibold">{formatPercent(run.summary.max_drawdown_pct)}</p>
          </div>
          <div>
            <p className="text-sm text-muted-foreground">Commissions</p>
            <p className="mt-1 text-xl font-semibold">{formatCurrency(run.summary.total_commissions)}</p>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Step timeline</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          {run.steps.length === 0 ? (
            <p className="text-sm text-muted-foreground">No step execution data yet.</p>
          ) : run.steps.map((step) => (
            <div key={step.step_number} className="rounded-xl border border-border/70 p-4">
              <div className="flex items-start justify-between gap-4">
                <div>
                  <p className="font-medium">Step {step.step_number}: {step.name}</p>
                  <p className="mt-1 text-sm text-muted-foreground">{step.action}</p>
                </div>
                <p className="text-sm font-medium">{statusLabel(step.status)}</p>
              </div>
              <div className="mt-3 grid gap-3 md:grid-cols-3">
                <div>
                  <p className="text-sm text-muted-foreground">Triggered</p>
                  <p className="font-medium">{formatDateTime(step.triggered_at)}</p>
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">Executed</p>
                  <p className="font-medium">{formatDateTime(step.executed_at)}</p>
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">Failure reason</p>
                  <p className="font-medium">{step.failure_reason ?? "-"}</p>
                </div>
              </div>
            </div>
          ))}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Workflow events</CardTitle>
        </CardHeader>
        <CardContent>
          {run.events.length === 0 ? (
            <p className="text-sm text-muted-foreground">No workflow events were recorded for this run.</p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Step</TableHead>
                  <TableHead>Event</TableHead>
                  <TableHead>Timestamp</TableHead>
                  <TableHead>Message</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {run.events.map((event, index) => (
                  <TableRow key={`${event.step_number}-${event.event_type}-${event.event_at}-${index}`}>
                    <TableCell className="font-medium">{event.step_number}</TableCell>
                    <TableCell>{event.event_type.replace(/_/g, " ")}</TableCell>
                    <TableCell>{formatDateTime(event.event_at)}</TableCell>
                    <TableCell>{event.message ?? "-"}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Trades</CardTitle>
        </CardHeader>
        <CardContent>
          {run.trades.length === 0 ? (
            <p className="text-sm text-muted-foreground">No trades were recorded for this run.</p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Step</TableHead>
                  <TableHead>Strategy</TableHead>
                  <TableHead>Option</TableHead>
                  <TableHead>Entry</TableHead>
                  <TableHead>Exit</TableHead>
                  <TableHead>Qty</TableHead>
                  <TableHead>Net P&L</TableHead>
                  <TableHead>Exit reason</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {run.trades.map((trade) => (
                  <TableRow key={trade.id}>
                    <TableCell className="font-medium">{trade.step_number}</TableCell>
                    <TableCell>{strategyLabel(trade.strategy_type)}</TableCell>
                    <TableCell>{trade.option_ticker}</TableCell>
                    <TableCell>{formatDate(trade.entry_date)}</TableCell>
                    <TableCell>{formatDate(trade.exit_date)}</TableCell>
                    <TableCell>{formatNumber(trade.quantity)}</TableCell>
                    <TableCell>{formatCurrency(trade.net_pnl)}</TableCell>
                    <TableCell>{formatReason(trade.exit_reason)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
