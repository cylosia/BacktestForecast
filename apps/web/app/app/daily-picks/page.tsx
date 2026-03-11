import { getCurrentUser, getDailyPicks } from "@/lib/api/server";
import { formatPercent, strategyLabel } from "@/lib/backtests/format";
import type { DailyPickItem, DailyPicksResponse } from "@/lib/backtests/types";
import { UpgradePrompt } from "@/components/billing/upgrade-prompt";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

function regimeColor(regime: string): string {
  switch (regime) {
    case "bullish":
      return "bg-emerald-500/10 text-emerald-700 dark:text-emerald-400";
    case "bearish":
      return "bg-red-500/10 text-red-700 dark:text-red-400";
    case "high_iv":
      return "bg-amber-500/10 text-amber-700 dark:text-amber-400";
    case "low_iv":
      return "bg-blue-500/10 text-blue-700 dark:text-blue-400";
    default:
      return "bg-muted text-muted-foreground";
  }
}

function ScoreBar({ score, max }: { score: number; max: number }) {
  const pct = Math.min((score / max) * 100, 100);
  return (
    <div className="h-2 w-full rounded-full bg-muted overflow-hidden">
      <div
        className="h-full rounded-full bg-primary transition-all"
        style={{ width: `${pct}%` }}
      />
    </div>
  );
}

function PickCard({ pick, maxScore }: { pick: DailyPickItem; maxScore: number }) {
  const summary = pick.summary as Record<string, number>;
  const forecast = (pick.forecast ?? {}) as Record<string, number>;

  return (
    <Card>
      <CardContent className="p-4">
        <div className="flex items-start justify-between gap-3">
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2">
              <span className="text-lg font-semibold">{pick.symbol}</span>
              <span className="text-sm text-muted-foreground">#{pick.rank}</span>
            </div>
            <p className="text-sm text-muted-foreground">{strategyLabel(pick.strategy_type)}</p>
            <div className="mt-2 flex flex-wrap gap-1">
              {pick.regime_labels.map((r) => (
                <Badge key={r} variant="secondary" className={regimeColor(r)}>
                  {r.replace(/_/g, " ")}
                </Badge>
              ))}
            </div>
          </div>
          <div className="text-right shrink-0">
            <p className="text-xs text-muted-foreground">Score</p>
            <p className="text-lg font-semibold">{pick.score.toFixed(1)}</p>
          </div>
        </div>

        <div className="mt-3">
          <ScoreBar score={pick.score} max={maxScore} />
        </div>

        <div className="mt-3 grid grid-cols-2 gap-x-4 gap-y-1 text-sm sm:grid-cols-4">
          <div>
            <span className="text-xs text-muted-foreground">ROI</span>
            <p className="font-medium">{summary.total_roi_pct?.toFixed(1) ?? "—"}%</p>
          </div>
          <div>
            <span className="text-xs text-muted-foreground">Win rate</span>
            <p className="font-medium">{summary.win_rate?.toFixed(0) ?? "—"}%</p>
          </div>
          <div>
            <span className="text-xs text-muted-foreground">Trades</span>
            <p className="font-medium">{summary.trade_count ?? "—"}</p>
          </div>
          <div>
            <span className="text-xs text-muted-foreground">Max DD</span>
            <p className="font-medium">{summary.max_drawdown_pct?.toFixed(1) ?? "—"}%</p>
          </div>
        </div>

        {forecast.expected_return_median_pct != null ? (
          <div className="mt-3 rounded-lg border border-border/60 bg-muted/30 p-2 text-xs">
            <span className="text-muted-foreground">Forecast: </span>
            <span className="font-medium">
              {Number(forecast.expected_return_median_pct).toFixed(1)}% median
            </span>
            <span className="text-muted-foreground">
              {" "}({Number(forecast.positive_outcome_rate_pct).toFixed(0)}% positive rate,{" "}
              {forecast.analog_count} analogs)
            </span>
          </div>
        ) : null}

        <div className="mt-2 flex items-center gap-3 text-xs text-muted-foreground">
          <span>Close: ${pick.close_price.toFixed(2)}</span>
          <span>DTE: {pick.target_dte}d</span>
        </div>
      </CardContent>
    </Card>
  );
}

export default async function DailyPicksPage() {
  const user = await getCurrentUser();
  const hasAccess = user.features.forecasting_access;

  if (!hasAccess) {
    return (
      <div className="space-y-6">
        <div>
          <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Daily picks</p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Today&apos;s top trades</h1>
        </div>
        <UpgradePrompt message="Daily Picks requires a Pro or Premium plan. Upgrade to see the best option trades for tomorrow, generated nightly from a 5-stage AI pipeline." />
      </div>
    );
  }

  let data: DailyPicksResponse;
  try {
    data = await getDailyPicks();
  } catch {
    return (
      <div className="space-y-6">
        <div>
          <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Daily picks</p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Today&apos;s top trades</h1>
        </div>
        <Card>
          <CardContent className="p-6 text-center text-muted-foreground">
            Daily picks could not be loaded. The nightly pipeline may not have run yet.
          </CardContent>
        </Card>
      </div>
    );
  }

  const maxScore = data.items.length > 0 ? Math.max(...data.items.map((i) => i.score)) : 1;

  return (
    <div className="space-y-6">
      <div>
        <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Daily picks</p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">
          {data.trade_date ? `Picks for ${data.trade_date}` : "Today's top trades"}
        </h1>
        <p className="mt-2 max-w-2xl text-muted-foreground">
          The best option trades ranked by a 5-stage nightly pipeline: universe screening,
          regime-based strategy matching, quick backtesting, full backtesting, and forecast
          overlay.
        </p>
      </div>

      {data.pipeline_stats ? (
        <div className="grid gap-3 sm:grid-cols-3 lg:grid-cols-6">
          {[
            { label: "Symbols screened", value: data.pipeline_stats.symbols_screened },
            { label: "Passed screen", value: data.pipeline_stats.symbols_after_screen },
            { label: "Pairs tested", value: data.pipeline_stats.pairs_generated },
            { label: "Quick backtests", value: data.pipeline_stats.quick_backtests_run },
            { label: "Full backtests", value: data.pipeline_stats.full_backtests_run },
            { label: "Duration", value: data.pipeline_stats.duration_seconds ? `${data.pipeline_stats.duration_seconds.toFixed(0)}s` : "—" },
          ].map((stat) => (
            <div key={stat.label} className="rounded-xl border border-border/70 p-3 text-center">
              <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">{stat.label}</p>
              <p className="mt-1 text-lg font-semibold">{stat.value}</p>
            </div>
          ))}
        </div>
      ) : null}

      {data.items.length === 0 ? (
        <Card>
          <CardContent className="p-6 text-center text-muted-foreground">
            {data.status === "no_data"
              ? "No pipeline data available yet. The nightly scan runs at 4:00 AM UTC."
              : "No recommendations were produced for this date."}
          </CardContent>
        </Card>
      ) : (
        <div className="grid gap-4 md:grid-cols-2">
          {data.items.map((pick) => (
            <PickCard key={`${pick.symbol}-${pick.strategy_type}`} pick={pick} maxScore={maxScore} />
          ))}
        </div>
      )}

      <div className="rounded-xl border border-border/70 bg-muted/30 p-4 text-sm text-muted-foreground space-y-2">
        <p className="font-medium text-foreground">How the pipeline works</p>
        <p>
          Every night at 4:00 AM UTC, the pipeline screens ~100 optionable symbols for
          technical signals, matches each to compatible strategies based on its current
          regime (bullish/bearish/neutral × high/low IV), runs quick 90-day backtests
          across a parameter grid, refines the top candidates with full 1-year backtests,
          and overlays historical analog forecasts for the final ranking.
        </p>
        <p className="text-xs">
          These are research outputs based on historical patterns, not trading signals,
          predictions, or financial advice.
        </p>
      </div>
    </div>
  );
}
