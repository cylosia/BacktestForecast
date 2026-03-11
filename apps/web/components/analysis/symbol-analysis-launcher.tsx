"use client";

import { useCallback, useState } from "react";
import { useAuth } from "@clerk/nextjs";
import { Loader2, Microscope, TrendingDown, TrendingUp } from "lucide-react";
import {
  createSymbolAnalysis,
  fetchAnalysisFull,
  fetchAnalysisStatus,
} from "@/lib/api/client";
import { ApiError } from "@/lib/api/shared";
import { strategyLabel } from "@/lib/backtests/format";
import type {
  AnalysisTopResult,
  LandscapeCell,
  RegimeDetail,
  SymbolAnalysisFullResponse,
  SymbolAnalysisSummary,
} from "@/lib/backtests/types";
import { isPlanLimitError, UpgradePrompt } from "@/components/billing/upgrade-prompt";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

const POLL_INTERVAL = 2_500;
const MAX_POLLS = 120; // ~5 minutes

function regimeColor(r: string) {
  if (r === "bullish") return "bg-emerald-500/10 text-emerald-700 dark:text-emerald-400";
  if (r === "bearish") return "bg-red-500/10 text-red-700 dark:text-red-400";
  if (r === "high_iv") return "bg-amber-500/10 text-amber-700 dark:text-amber-400";
  if (r === "low_iv") return "bg-blue-500/10 text-blue-700 dark:text-blue-400";
  if (r === "trending") return "bg-purple-500/10 text-purple-700 dark:text-purple-400";
  return "bg-muted text-muted-foreground";
}

function indicatorCard(label: string, value: number | null, suffix?: string) {
  return (
    <div className="rounded-lg border border-border/60 p-3">
      <p className="text-xs uppercase tracking-[0.14em] text-muted-foreground">{label}</p>
      <p className="mt-1 text-lg font-semibold">
        {value != null ? `${value.toFixed(2)}${suffix || ""}` : "—"}
      </p>
    </div>
  );
}

function RegimeSection({ regime }: { regime: RegimeDetail }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Current regime</CardTitle>
        <CardDescription>Technical indicator snapshot as of the latest close at ${regime.close_price.toFixed(2)}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex flex-wrap gap-2">
          {regime.regimes.map((r) => (
            <Badge key={r} variant="secondary" className={regimeColor(r)}>
              {r.replace(/_/g, " ")}
            </Badge>
          ))}
        </div>
        <div className="grid gap-3 sm:grid-cols-3 lg:grid-cols-4">
          {indicatorCard("RSI (14)", regime.rsi_14)}
          {indicatorCard("EMA (8)", regime.ema_8)}
          {indicatorCard("EMA (21)", regime.ema_21)}
          {indicatorCard("SMA (50)", regime.sma_50)}
          {indicatorCard("SMA (200)", regime.sma_200)}
          {indicatorCard("Vol (20d)", regime.realized_vol_20, "%")}
          {indicatorCard("IV Rank", regime.iv_rank_proxy, "%")}
          {indicatorCard("Volume ratio", regime.volume_ratio, "×")}
        </div>
      </CardContent>
    </Card>
  );
}

function LandscapeSection({ cells }: { cells: LandscapeCell[] }) {
  const strategies = [...new Set(cells.map((c) => c.strategy_type))];
  const dtes = [...new Set(cells.map((c) => c.target_dte))].sort((a, b) => a - b);

  // Best score per strategy
  const bestByStrategy = new Map<string, LandscapeCell>();
  for (const cell of cells) {
    const existing = bestByStrategy.get(cell.strategy_type);
    if (!existing || cell.score > existing.score) {
      bestByStrategy.set(cell.strategy_type, cell);
    }
  }

  // Sort strategies by best score
  const sorted = [...bestByStrategy.entries()]
    .sort((a, b) => b[1].score - a[1].score)
    .slice(0, 20);

  const maxScore = sorted.length > 0 ? sorted[0][1].score : 1;

  return (
    <Card>
      <CardHeader>
        <CardTitle>Strategy landscape</CardTitle>
        <CardDescription>
          {cells.length} configurations tested across {strategies.length} strategies.
          Showing top 20 strategies by best score.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="space-y-2">
          {sorted.map(([stratType, best]) => (
            <div
              key={stratType}
              className="flex items-center gap-3 rounded-lg border border-border/60 p-3"
            >
              <div className="min-w-0 flex-1">
                <p className="text-sm font-medium truncate">{strategyLabel(stratType)}</p>
                <p className="text-xs text-muted-foreground">
                  DTE {best.target_dte} · {best.trade_count} trades · {best.win_rate.toFixed(0)}% WR
                </p>
              </div>
              <div className="w-24 shrink-0">
                <div className="h-2 w-full rounded-full bg-muted overflow-hidden">
                  <div
                    className="h-full rounded-full bg-primary"
                    style={{ width: `${Math.max((best.score / maxScore) * 100, 2)}%` }}
                  />
                </div>
              </div>
              <div className="w-16 text-right shrink-0">
                <span className={`text-sm font-semibold ${best.total_roi_pct >= 0 ? "text-emerald-600" : "text-red-500"}`}>
                  {best.total_roi_pct.toFixed(1)}%
                </span>
              </div>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}

function TopResultCard({ result }: { result: AnalysisTopResult }) {
  const summary = result.summary as Record<string, number>;
  const forecast = result.forecast as Record<string, number>;

  return (
    <Card>
      <CardContent className="p-4 space-y-3">
        <div className="flex items-start justify-between">
          <div>
            <div className="flex items-center gap-2">
              <span className="text-sm font-medium text-muted-foreground">#{result.rank}</span>
              <span className="font-semibold">{strategyLabel(result.strategy_type)}</span>
            </div>
            <p className="text-xs text-muted-foreground">DTE {result.target_dte}</p>
          </div>
          <div className="text-right">
            <p className="text-xs text-muted-foreground">Score</p>
            <p className="text-lg font-semibold">{result.score.toFixed(1)}</p>
          </div>
        </div>

        <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-sm sm:grid-cols-4">
          <div>
            <span className="text-xs text-muted-foreground">ROI</span>
            <p className={`font-medium ${(summary.total_roi_pct ?? 0) >= 0 ? "text-emerald-600" : "text-red-500"}`}>
              {summary.total_roi_pct?.toFixed(1) ?? "—"}%
            </p>
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
          <div className="rounded-lg border border-border/60 bg-muted/30 p-2 text-xs">
            <span className="text-muted-foreground">Forecast: </span>
            <span className="font-medium">
              {Number(forecast.expected_return_median_pct).toFixed(1)}% median
            </span>
            <span className="text-muted-foreground">
              {" "}({Number(forecast.positive_outcome_rate_pct ?? 0).toFixed(0)}% positive,{" "}
              {forecast.analog_count ?? 0} analogs)
            </span>
          </div>
        ) : null}

        {result.trades.length > 0 ? (
          <p className="text-xs text-muted-foreground">
            {result.trades.length} trade{result.trades.length !== 1 ? "s" : ""} in full backtest
          </p>
        ) : null}
      </CardContent>
    </Card>
  );
}

export function SymbolAnalysisLauncher() {
  const { getToken } = useAuth();
  const [symbol, setSymbol] = useState("AAPL");
  const [phase, setPhase] = useState<"idle" | "polling" | "done" | "error">("idle");
  const [stage, setStage] = useState("");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [errorCode, setErrorCode] = useState<string | undefined>();
  const [result, setResult] = useState<SymbolAnalysisFullResponse | null>(null);

  const pollForCompletion = useCallback(
    async (token: string, analysisId: string) => {
      for (let i = 0; i < MAX_POLLS; i++) {
        await new Promise((r) => setTimeout(r, POLL_INTERVAL));
        const status = await fetchAnalysisStatus(token, analysisId);
        setStage(status.stage);

        if (status.status === "succeeded") {
          const full = await fetchAnalysisFull(token, analysisId);
          setResult(full);
          setPhase("done");
          return;
        }
        if (status.status === "failed") {
          throw new Error(status.error_message || "Analysis failed.");
        }
      }
      throw new Error("Analysis is still running. Check back later.");
    },
    [],
  );

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const sym = symbol.trim().toUpperCase();
    if (!sym) return;

    setPhase("polling");
    setStage("pending");
    setErrorMessage(null);
    setErrorCode(undefined);
    setResult(null);

    try {
      const token = await getToken();
      if (!token) throw new Error("Session expired.");

      const created = await createSymbolAnalysis(token, sym, `deep-${sym}-${Date.now()}`);

      if (created.status === "succeeded") {
        const full = await fetchAnalysisFull(token, created.id);
        setResult(full);
        setPhase("done");
      } else {
        await pollForCompletion(token, created.id);
      }
    } catch (error) {
      const msg =
        error instanceof ApiError ? error.message : error instanceof Error ? error.message : "Analysis failed.";
      const code = error instanceof ApiError ? error.code : undefined;
      setPhase("error");
      setErrorMessage(msg);
      setErrorCode(code);
    }
  }

  const stageLabels: Record<string, string> = {
    pending: "Queued…",
    regime: "Analyzing regime…",
    landscape: "Testing strategies…",
    deep_dive: "Running full backtests…",
    forecast: "Generating forecasts…",
  };

  return (
    <div className="space-y-6">
      <form className="space-y-4" onSubmit={handleSubmit}>
        <Card>
          <CardHeader>
            <CardTitle>Analyze a symbol</CardTitle>
            <CardDescription>
              Run an exhaustive analysis: all strategies, dense parameter grid, regime classification,
              and forecast overlay.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex items-end gap-3">
              <div className="space-y-2 flex-1 max-w-xs">
                <Label htmlFor="analysisSymbol">Symbol</Label>
                <Input
                  id="analysisSymbol"
                  placeholder="AAPL"
                  maxLength={16}
                  value={symbol}
                  onChange={(e) => setSymbol(e.target.value.toUpperCase())}
                  disabled={phase === "polling"}
                />
              </div>
              <Button type="submit" disabled={phase === "polling"}>
                {phase === "polling" ? (
                  <>
                    <Loader2 className="h-4 w-4 animate-spin" />
                    {stageLabels[stage] || "Analyzing…"}
                  </>
                ) : (
                  <>
                    <Microscope className="h-4 w-4" />
                    Analyze
                  </>
                )}
              </Button>
            </div>
          </CardContent>
        </Card>
      </form>

      {errorMessage && isPlanLimitError(errorCode) ? (
        <UpgradePrompt message={errorMessage} />
      ) : errorMessage ? (
        <div className="rounded-xl border border-destructive/40 bg-destructive/5 p-4 text-sm text-destructive">
          {errorMessage}
        </div>
      ) : null}

      {result ? (
        <>
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <div className="rounded-xl border border-border/70 p-3 text-center">
              <p className="text-xs uppercase tracking-[0.14em] text-muted-foreground">Strategies</p>
              <p className="mt-1 text-xl font-semibold">{result.strategies_tested}</p>
            </div>
            <div className="rounded-xl border border-border/70 p-3 text-center">
              <p className="text-xs uppercase tracking-[0.14em] text-muted-foreground">Configs tested</p>
              <p className="mt-1 text-xl font-semibold">{result.configs_tested}</p>
            </div>
            <div className="rounded-xl border border-border/70 p-3 text-center">
              <p className="text-xs uppercase tracking-[0.14em] text-muted-foreground">Top results</p>
              <p className="mt-1 text-xl font-semibold">{result.top_results_count}</p>
            </div>
            <div className="rounded-xl border border-border/70 p-3 text-center">
              <p className="text-xs uppercase tracking-[0.14em] text-muted-foreground">Duration</p>
              <p className="mt-1 text-xl font-semibold">{result.duration_seconds?.toFixed(0) ?? "—"}s</p>
            </div>
          </div>

          {result.regime ? <RegimeSection regime={result.regime} /> : null}

          {result.landscape && result.landscape.length > 0 ? (
            <LandscapeSection cells={result.landscape} />
          ) : null}

          {result.top_results && result.top_results.length > 0 ? (
            <Card>
              <CardHeader>
                <CardTitle>Top {result.top_results.length} configurations</CardTitle>
                <CardDescription>
                  Full 1-year backtests with forecast overlay. Best strategy per type, ranked by composite score.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="grid gap-4 md:grid-cols-2">
                  {result.top_results.map((r) => (
                    <TopResultCard key={`${r.strategy_type}-${r.rank}`} result={r} />
                  ))}
                </div>
              </CardContent>
            </Card>
          ) : null}
        </>
      ) : null}
    </div>
  );
}
