import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import type { ExportFormat } from "@backtestforecast/api-client";
import { getCurrentUser, getMultiSymbolBacktestRun } from "@/lib/api/server";
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
import { MultiSymbolRunPoller } from "@/components/backtests/multi-symbol-run-poller";
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

export default async function MultiSymbolRunPage({
  params,
}: {
  params: Promise<{ runId: string }>;
}) {
  const { runId } = await params;
  const [run, user] = await Promise.all([getMultiSymbolBacktestRun(runId), getCurrentUser()]);
  const warnings = asWarnings(run.warnings);

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between gap-4">
        <div>
          <Button asChild className="mb-4 px-0" variant="ghost">
            <Link href="/app/multi-symbol-backtests">
              <ArrowLeft className="h-4 w-4" />
              Back to multi-symbol runs
            </Link>
          </Button>
          <h1 className="text-3xl font-semibold tracking-tight">{run.name ?? run.symbols.map((symbol) => symbol.symbol).join(" + ")}</h1>
          <p className="mt-2 text-muted-foreground">
            {statusLabel(run.status)} · Created {formatDateTime(run.created_at)} · {formatDate(run.start_date)} to {formatDate(run.end_date)}
          </p>
        </div>
        {run.status === "succeeded" ? (
          <ExportActions formats={(user.features.export_formats ?? []).filter((f): f is ExportFormat => f === "csv" || f === "pdf")} runId={run.id} />
        ) : null}
      </div>

      <MultiSymbolRunPoller runId={runId} initialStatus={run.status} />

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
          <CardHeader className="pb-2"><CardTitle className="text-sm font-medium text-muted-foreground">Symbols</CardTitle></CardHeader>
          <CardContent><p className="text-3xl font-semibold tracking-tight">{run.symbols.length}</p></CardContent>
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
          <CardTitle>Combined summary</CardTitle>
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
          <CardTitle>Per-symbol setup and results</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {run.symbols.map((symbol) => {
            const summary = run.symbol_summaries.find((item) => item.symbol === symbol.symbol)?.summary;
            const equity = run.symbol_equity_curves[symbol.symbol] ?? [];
            const lastPoint = equity.at(-1);
            return (
              <div key={symbol.symbol} className="rounded-xl border border-border/70 p-4">
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <p className="text-lg font-semibold">{symbol.symbol}</p>
                    <p className="mt-1 text-sm text-muted-foreground">
                      Risk {formatPercent(symbol.risk_per_trade_pct)} · Max open {symbol.max_open_positions}
                      {symbol.capital_allocation_pct != null ? ` · Allocation ${formatPercent(symbol.capital_allocation_pct)}` : ""}
                    </p>
                  </div>
                  <div className="text-right text-sm text-muted-foreground">
                    <p>{equity.length} equity points</p>
                    <p>{lastPoint ? `Ending equity ${formatCurrency(lastPoint.equity)}` : "No equity history"}</p>
                  </div>
                </div>
                {summary ? (
                  <div className="mt-4 grid gap-3 md:grid-cols-4">
                    <div>
                      <p className="text-sm text-muted-foreground">ROI</p>
                      <p className="font-semibold">{formatPercent(summary.total_roi_pct)}</p>
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">Net P&L</p>
                      <p className="font-semibold">{formatCurrency(summary.total_net_pnl)}</p>
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">Trades</p>
                      <p className="font-semibold">{formatNumber(summary.trade_count)}</p>
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">Max drawdown</p>
                      <p className="font-semibold">{formatPercent(summary.max_drawdown_pct)}</p>
                    </div>
                  </div>
                ) : null}
              </div>
            );
          })}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Grouped trades</CardTitle>
        </CardHeader>
        <CardContent>
          {run.trade_groups.length === 0 ? (
            <p className="text-sm text-muted-foreground">No grouped trades were recorded for this run.</p>
          ) : (
            <div className="space-y-4">
              {run.trade_groups.map((group) => (
                <div key={group.id} className="rounded-xl border border-border/70 p-4">
                  <div className="flex items-start justify-between gap-4">
                    <div>
                      <p className="font-semibold">Group {group.id.slice(0, 8)}</p>
                      <p className="mt-1 text-sm text-muted-foreground">
                        {formatDate(group.entry_date)} · {group.exit_date ? `Exited ${formatDate(group.exit_date)}` : "Still open"}
                      </p>
                    </div>
                    <p className="text-sm font-medium">{statusLabel(group.status)}</p>
                  </div>
                  <div className="mt-4">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Symbol</TableHead>
                          <TableHead>Strategy</TableHead>
                          <TableHead>Option</TableHead>
                          <TableHead>Entry</TableHead>
                          <TableHead>Exit</TableHead>
                          <TableHead>Qty</TableHead>
                          <TableHead>Net P&L</TableHead>
                          <TableHead>Reason</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {group.trades.map((trade) => (
                          <TableRow key={trade.id}>
                            <TableCell className="font-medium">{trade.symbol}</TableCell>
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
                  </div>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
