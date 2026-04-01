"use client";

import { useState } from "react";
import type { ScannerRecommendationResponse } from "@backtestforecast/api-client";
import { formatCurrency, formatNumber, formatPercent, strategyLabel, toNumber } from "@/lib/backtests/format";
import { ScoreBar } from "@/components/shared/score-bar";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

const PAGE_SIZE = 25;

function normalizedScoreValue(score: number, rawMin: number, rawMax: number): number {
  if (!Number.isFinite(score)) return 0;
  if (rawMax <= 0) {
    if (rawMax === rawMin) return 1;
    return Math.max(score - rawMin, 0);
  }
  return Math.max(score, 0);
}

function decidedTradeContext(summary: ScannerRecommendationResponse["summary"]): string | null {
  const total = summary.trade_count;
  const decided = summary.decided_trades;
  if (total == null || decided == null || total <= 0 || decided === total) return null;
  const breakEven = total - decided;
  if (breakEven <= 0) return null;
  return `Based on ${formatNumber(decided)} of ${formatNumber(total)} trades (${formatNumber(breakEven)} break-even excluded)`;
}

export function RecommendationList({
  items,
}: {
  items: ScannerRecommendationResponse[];
}) {
  const [currentPage, setCurrentPage] = useState(0);

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

  const scores = items.map((r) => toNumber(r.score)).filter(Number.isFinite);
  const rawMax = scores.length === 0 ? 0 : Math.max(...scores);
  const rawMin = scores.length === 0 ? 0 : Math.min(...scores);
  const scoreRange = rawMax <= 0 ? (rawMax === rawMin ? 1 : Math.max(rawMax - rawMin, 1)) : Math.max(rawMax, 1);

  const totalPages = Math.ceil(items.length / PAGE_SIZE);
  const pageItems = items.slice(currentPage * PAGE_SIZE, (currentPage + 1) * PAGE_SIZE);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Ranked recommendations</CardTitle>
        <CardDescription>
          {items.length} recommendation(s) ranked by composite score (backtest performance + historical patterns + forecast alignment).
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        {pageItems.map((rec) => {
          const rankingBreakdown = rec.ranking_breakdown;
          const rankingReasoning = rankingBreakdown?.reasoning ?? [];
          const warnings = rec.warnings ?? [];

          return (
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
                      {rec.symbol} - {strategyLabel(rec.strategy_type)}
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
                    <ScoreBar score={normalizedScoreValue(toNumber(rec.score), rawMin, rawMax)} max={scoreRange} />
                  </div>
                </div>
              </div>

              <div className="grid gap-3 sm:grid-cols-3 lg:grid-cols-6">
                <div className="rounded-lg border border-border/60 p-3">
                  <p className="text-xs text-muted-foreground">Trades</p>
                  <p className="mt-1 font-semibold">{rec.summary.trade_count ?? "-"}</p>
                </div>
                <div className="rounded-lg border border-border/60 p-3">
                  <p className="text-xs text-muted-foreground">Win rate</p>
                  <p className="mt-1 font-semibold">{rec.summary.win_rate != null ? formatPercent(rec.summary.win_rate) : "-"}</p>
                  {decidedTradeContext(rec.summary) ? <p className="mt-1 text-xs text-muted-foreground">{decidedTradeContext(rec.summary)}</p> : null}
                </div>
                <div className="rounded-lg border border-border/60 p-3">
                  <p className="text-xs text-muted-foreground">ROI</p>
                  <p className="mt-1 font-semibold">{rec.summary.total_roi_pct != null ? formatPercent(rec.summary.total_roi_pct) : "-"}</p>
                </div>
                <div className="rounded-lg border border-border/60 p-3">
                  <p className="text-xs text-muted-foreground">Net P&L</p>
                  <p className="mt-1 font-semibold">{rec.summary.total_net_pnl != null ? formatCurrency(rec.summary.total_net_pnl) : "-"}</p>
                </div>
                <div className="rounded-lg border border-border/60 p-3">
                  <p className="text-xs text-muted-foreground">Max DD</p>
                  <p className="mt-1 font-semibold">{rec.summary.max_drawdown_pct != null ? formatPercent(rec.summary.max_drawdown_pct) : "-"}</p>
                </div>
                <div className="rounded-lg border border-border/60 p-3">
                  <p className="text-xs text-muted-foreground">Commissions</p>
                  <p className="mt-1 font-semibold">{rec.summary.total_commissions != null ? formatCurrency(rec.summary.total_commissions) : "-"}</p>
                </div>
              </div>

              <div className="grid gap-3 sm:grid-cols-2">
                <div className="rounded-lg border border-border/60 p-3 space-y-1">
                  <p className="text-xs font-medium text-muted-foreground">Forecast</p>
                  <p className="text-sm">
                    {rec.forecast?.expected_return_low_pct != null ? formatPercent(rec.forecast.expected_return_low_pct) : "-"} to{" "}
                    {rec.forecast?.expected_return_high_pct != null ? formatPercent(rec.forecast.expected_return_high_pct) : "-"} expected range
                  </p>
                  <p className="text-sm">
                    Median: {rec.forecast?.expected_return_median_pct != null ? formatPercent(rec.forecast.expected_return_median_pct) : "-"} |{" "}
                    Favorable outcome: {rec.forecast?.positive_outcome_rate_pct != null ? formatPercent(rec.forecast.positive_outcome_rate_pct) : "-"}
                  </p>
                  {rec.forecast?.summary ? <p className="text-xs text-muted-foreground">{rec.forecast.summary}</p> : null}
                  <p className="text-xs text-muted-foreground">{rec.forecast?.disclaimer}</p>
                </div>

                <div className="rounded-lg border border-border/60 p-3 space-y-1">
                  <p className="text-xs font-medium text-muted-foreground">Ranking breakdown</p>
                  <div className="grid grid-cols-3 gap-2 text-sm">
                    <div>
                      <p className="text-muted-foreground">Current</p>
                      <p className="font-medium">
                        {rankingBreakdown ? formatNumber(toNumber(rankingBreakdown.current_performance_score)) : "-"}
                      </p>
                    </div>
                    <div>
                      <p className="text-muted-foreground">Historical</p>
                      <p className="font-medium">
                        {rankingBreakdown ? formatNumber(toNumber(rankingBreakdown.historical_performance_score)) : "-"}
                      </p>
                    </div>
                    <div>
                      <p className="text-muted-foreground">Forecast</p>
                      <p className="font-medium">
                        {rankingBreakdown ? formatNumber(toNumber(rankingBreakdown.forecast_alignment_score)) : "-"}
                      </p>
                    </div>
                  </div>
                  {rankingReasoning.length > 0 ? (
                    <p className="text-xs text-muted-foreground">
                      {rankingReasoning.join(" | ")}
                    </p>
                  ) : null}
                </div>
              </div>

              {warnings.length > 0 ? (
                <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 p-3 space-y-1">
                  <p className="text-xs font-medium text-amber-700 dark:text-amber-400">
                    {warnings.length} warning(s)
                  </p>
                  <ul className="list-disc list-inside text-xs text-amber-700 dark:text-amber-400">
                    {warnings.map((w, i) => {
                      const warning = w as Record<string, unknown> | undefined;
                      return (
                        <li key={i}>{typeof w === "string" ? w : String(warning?.message ?? warning?.type ?? "Unknown warning")}</li>
                      );
                    })}
                  </ul>
                </div>
              ) : null}
            </div>
          );
        })}
        {totalPages > 1 && (
          <div className="mt-4 flex items-center justify-between">
            <p className="text-sm text-muted-foreground">
              Page {currentPage + 1} of {totalPages} ({items.length} items)
            </p>
            <div className="flex gap-2">
              <Button
                variant="outline"
                size="sm"
                disabled={currentPage === 0}
                onClick={() => setCurrentPage((p) => p - 1)}
              >
                Previous
              </Button>
              <Button
                variant="outline"
                size="sm"
                disabled={currentPage >= totalPages - 1}
                onClick={() => setCurrentPage((p) => p + 1)}
              >
                Next
              </Button>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
