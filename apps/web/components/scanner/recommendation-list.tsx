import type { ScannerRecommendationResponse } from "@/lib/backtests/types";
import { formatCurrency, formatNumber, formatPercent, strategyLabel, toNumber } from "@/lib/backtests/format";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

function ScoreBar({ score, max }: { score: number; max: number }) {
  const pct = max > 0 ? Math.min((score / max) * 100, 100) : 0;
  return (
    <div className="h-1.5 w-full overflow-hidden rounded-full bg-muted">
      <div className="h-full rounded-full bg-primary" style={{ width: `${pct}%` }} />
    </div>
  );
}

export function RecommendationList({
  items,
}: {
  items: ScannerRecommendationResponse[];
}) {
  if (items.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Recommendations</CardTitle>
          <CardDescription>No recommendations were produced for this scan.</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  const maxScore = Math.max(...items.map((r) => toNumber(r.score)), 1);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Ranked recommendations</CardTitle>
        <CardDescription>
          {items.length} recommendation(s) ranked by composite score (backtest performance + historical patterns + forecast alignment).
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {items.map((rec) => (
          <div
            key={rec.id}
            className="rounded-xl border border-border/70 p-4 space-y-3"
          >
            <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
              <div className="space-y-1">
                <div className="flex items-center gap-2">
                  <Badge variant="default" className="text-xs">
                    #{rec.rank}
                  </Badge>
                  <p className="text-lg font-semibold">
                    {rec.symbol} — {strategyLabel(rec.strategy_type)}
                  </p>
                </div>
                <p className="text-sm text-muted-foreground">
                  Rule set: {rec.rule_set_name}
                </p>
              </div>
              <div className="text-right">
                <p className="text-2xl font-semibold tracking-tight">
                  {formatNumber(toNumber(rec.score))}
                </p>
                <p className="text-xs text-muted-foreground">Composite score</p>
                <div className="mt-1 w-24">
                  <ScoreBar score={toNumber(rec.score)} max={maxScore} />
                </div>
              </div>
            </div>

            <div className="grid gap-3 sm:grid-cols-3 lg:grid-cols-6">
              <div className="rounded-lg border border-border/60 p-3">
                <p className="text-xs text-muted-foreground">Trades</p>
                <p className="mt-1 font-semibold">{rec.summary.trade_count ?? "—"}</p>
              </div>
              <div className="rounded-lg border border-border/60 p-3">
                <p className="text-xs text-muted-foreground">Win rate</p>
                <p className="mt-1 font-semibold">{rec.summary.win_rate != null ? formatPercent(rec.summary.win_rate) : "—"}</p>
              </div>
              <div className="rounded-lg border border-border/60 p-3">
                <p className="text-xs text-muted-foreground">ROI</p>
                <p className="mt-1 font-semibold">{rec.summary.total_roi_pct != null ? formatPercent(rec.summary.total_roi_pct) : "—"}</p>
              </div>
              <div className="rounded-lg border border-border/60 p-3">
                <p className="text-xs text-muted-foreground">Net P&L</p>
                <p className="mt-1 font-semibold">{rec.summary.total_net_pnl != null ? formatCurrency(rec.summary.total_net_pnl) : "—"}</p>
              </div>
              <div className="rounded-lg border border-border/60 p-3">
                <p className="text-xs text-muted-foreground">Max DD</p>
                <p className="mt-1 font-semibold">{rec.summary.max_drawdown_pct != null ? formatPercent(rec.summary.max_drawdown_pct) : "—"}</p>
              </div>
              <div className="rounded-lg border border-border/60 p-3">
                <p className="text-xs text-muted-foreground">Commissions</p>
                <p className="mt-1 font-semibold">{rec.summary.total_commissions != null ? formatCurrency(rec.summary.total_commissions) : "—"}</p>
              </div>
            </div>

            <div className="grid gap-3 sm:grid-cols-2">
              <div className="rounded-lg border border-border/60 p-3 space-y-1">
                <p className="text-xs font-medium text-muted-foreground">Forecast</p>
                <p className="text-sm">
                  {rec.forecast.expected_return_low_pct != null ? formatPercent(rec.forecast.expected_return_low_pct) : "—"} to{" "}
                  {rec.forecast.expected_return_high_pct != null ? formatPercent(rec.forecast.expected_return_high_pct) : "—"} expected range
                </p>
                <p className="text-sm">
                  Median: {rec.forecast.expected_return_median_pct != null ? formatPercent(rec.forecast.expected_return_median_pct) : "—"} ·{" "}
                  Positive outcome: {rec.forecast.positive_outcome_rate_pct != null ? formatPercent(rec.forecast.positive_outcome_rate_pct) : "—"}
                </p>
                <p className="text-xs text-muted-foreground">{rec.forecast.disclaimer}</p>
              </div>

              <div className="rounded-lg border border-border/60 p-3 space-y-1">
                <p className="text-xs font-medium text-muted-foreground">Ranking breakdown</p>
                <div className="grid grid-cols-3 gap-2 text-sm">
                  <div>
                    <p className="text-muted-foreground">Current</p>
                    <p className="font-medium">{formatNumber(toNumber(rec.ranking_breakdown.current_performance_score))}</p>
                  </div>
                  <div>
                    <p className="text-muted-foreground">Historical</p>
                    <p className="font-medium">{formatNumber(toNumber(rec.ranking_breakdown.historical_performance_score))}</p>
                  </div>
                  <div>
                    <p className="text-muted-foreground">Forecast</p>
                    <p className="font-medium">{formatNumber(toNumber(rec.ranking_breakdown.forecast_alignment_score))}</p>
                  </div>
                </div>
                {rec.ranking_breakdown.reasoning.length > 0 ? (
                  <p className="text-xs text-muted-foreground">
                    {rec.ranking_breakdown.reasoning.join(" · ")}
                  </p>
                ) : null}
              </div>
            </div>

            {rec.warnings.length > 0 ? (
              <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 p-3">
                <p className="text-xs font-medium text-amber-700 dark:text-amber-400">
                  {rec.warnings.length} warning(s)
                </p>
              </div>
            ) : null}
          </div>
        ))}
      </CardContent>
    </Card>
  );
}
