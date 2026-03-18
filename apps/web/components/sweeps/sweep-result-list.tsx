import type { SweepResultResponse } from "@backtestforecast/api-client";
import { formatCurrency, formatNumber, formatPercent, strategyLabel, toNumber } from "@/lib/backtests/format";
import { ScoreBar } from "@/components/shared/score-bar";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

interface LegData {
  contract_type?: string;
  asset_type?: string;
  side?: string;
  strike_offset?: number;
  expiration_offset?: number;
  quantity_ratio?: number;
}

function LegTable({ legs }: { legs: LegData[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b text-left text-xs text-muted-foreground">
            <th className="pb-2 pr-3">Type</th>
            <th className="pb-2 pr-3">Side</th>
            <th className="pb-2 pr-3">Strike offset</th>
            <th className="pb-2 pr-3">Exp offset</th>
            <th className="pb-2">Qty ratio</th>
          </tr>
        </thead>
        <tbody>
          {legs.map((leg: LegData, i: number) => (
            <tr key={i} className="border-b border-border/40">
              <td className="py-1.5 pr-3 font-medium">{leg.contract_type ?? leg.asset_type}</td>
              <td className="py-1.5 pr-3">
                <Badge variant={leg.side === "long" ? "default" : "secondary"} className="text-xs">
                  {leg.side}
                </Badge>
              </td>
              <td className="py-1.5 pr-3">{leg.strike_offset ?? 0}</td>
              <td className="py-1.5 pr-3">{leg.expiration_offset ?? 0}</td>
              <td className="py-1.5">{String(leg.quantity_ratio ?? "1")}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function SweepResultList({ items }: { items: SweepResultResponse[] }) {
  if (items.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Results</CardTitle>
          <CardDescription>No results were produced for this sweep.</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  const scores = items.map((r) => toNumber(r.score)).filter(Number.isFinite);
  const rawMax = scores.length === 0 ? 0 : Math.max(...scores);
  const allNegative = rawMax <= 0;
  const maxScore = allNegative ? Math.max(Math.abs(Math.min(...scores)), 1) : Math.max(rawMax, 1);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Ranked results</CardTitle>
        <CardDescription>
          {items.length} result(s) ranked by composite score (win rate, ROI, Sharpe, drawdown).
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {items.map((result) => {
          const params = result.parameter_snapshot_json ?? result;
          const customLegs = params.custom_legs;
          const isGenetic = params.mode === "genetic";

          return (
            <div key={result.id} className="rounded-xl border border-border/70 p-4 space-y-3">
              <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                <div className="space-y-1">
                  <div className="flex items-center gap-2">
                    <Badge variant="default" className="text-xs">#{result.rank}</Badge>
                    <p className="text-lg font-semibold">{strategyLabel(result.strategy_type)}</p>
                    {isGenetic ? <Badge variant="secondary" className="text-xs">genetic</Badge> : null}
                  </div>
                  <div className="flex flex-wrap gap-2 text-sm text-muted-foreground">
                    {result.delta != null ? <span>Delta: {result.delta}</span> : null}
                    {result.width_value != null ? <span>Width: {result.width_value} ({result.width_mode})</span> : null}
                    {result.entry_rule_set_name ? <span>Entry: {result.entry_rule_set_name}</span> : null}
                    {result.exit_rule_set_name ? <span>Exit: {result.exit_rule_set_name}</span> : null}
                    {result.profit_target_pct != null ? <span>PT: {result.profit_target_pct}%</span> : null}
                    {result.stop_loss_pct != null ? <span>SL: {result.stop_loss_pct}%</span> : null}
                  </div>
                </div>
                <div className="text-right">
                  <p className="text-2xl font-semibold tracking-tight">{formatNumber(toNumber(result.score))}</p>
                  <p className="text-xs text-muted-foreground">Score</p>
                  <div className="mt-1 w-24">
                    <ScoreBar score={allNegative ? 0 : Math.max(toNumber(result.score), 0)} max={maxScore} />
                  </div>
                </div>
              </div>

              {customLegs && Array.isArray(customLegs) && customLegs.length > 0 ? (
                <div className="rounded-lg border border-border/60 p-3">
                  <p className="text-xs font-medium text-muted-foreground mb-2">Leg configuration</p>
                  <LegTable legs={customLegs} />
                </div>
              ) : null}

              <div className="grid gap-3 sm:grid-cols-3 lg:grid-cols-6">
                <div className="rounded-lg border border-border/60 p-3">
                  <p className="text-xs text-muted-foreground">Trades</p>
                  <p className="mt-1 font-semibold">{result.summary.trade_count ?? "—"}</p>
                </div>
                <div className="rounded-lg border border-border/60 p-3">
                  <p className="text-xs text-muted-foreground">Win rate</p>
                  <p className="mt-1 font-semibold">{result.summary.win_rate != null ? formatPercent(result.summary.win_rate) : "—"}</p>
                </div>
                <div className="rounded-lg border border-border/60 p-3">
                  <p className="text-xs text-muted-foreground">ROI</p>
                  <p className="mt-1 font-semibold">{result.summary.total_roi_pct != null ? formatPercent(result.summary.total_roi_pct) : "—"}</p>
                </div>
                <div className="rounded-lg border border-border/60 p-3">
                  <p className="text-xs text-muted-foreground">Net P&L</p>
                  <p className="mt-1 font-semibold">{result.summary.total_net_pnl != null ? formatCurrency(result.summary.total_net_pnl) : "—"}</p>
                </div>
                <div className="rounded-lg border border-border/60 p-3">
                  <p className="text-xs text-muted-foreground">Max DD</p>
                  <p className="mt-1 font-semibold">{result.summary.max_drawdown_pct != null ? formatPercent(result.summary.max_drawdown_pct) : "—"}</p>
                </div>
                <div className="rounded-lg border border-border/60 p-3">
                  <p className="text-xs text-muted-foreground">Sharpe</p>
                  <p className="mt-1 font-semibold">{result.summary.sharpe_ratio != null ? formatNumber(toNumber(result.summary.sharpe_ratio)) : "—"}</p>
                </div>
              </div>

              {(result.warnings ?? []).length > 0 ? (
                <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 p-3 space-y-1">
                  <p className="text-xs font-medium text-amber-700 dark:text-amber-400">
                    {(result.warnings ?? []).length} warning(s)
                  </p>
                  <ul className="list-disc list-inside text-xs text-amber-700 dark:text-amber-400">
                    {(result.warnings ?? []).map((w: Record<string, unknown>, i: number) => (
                      <li key={i}>{typeof w === "string" ? w : String(w.message ?? JSON.stringify(w))}</li>
                    ))}
                  </ul>
                </div>
              ) : null}
            </div>
          );
        })}
      </CardContent>
    </Card>
  );
}
