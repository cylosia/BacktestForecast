"use client";

import { useEffect, useRef, useState } from "react";
import { useAuth } from "@clerk/nextjs";
import { Loader2, TrendingUp } from "lucide-react";
import { fetchForecast } from "@/lib/api/client";
import { ApiError } from "@/lib/api/shared";
import { formatDate, formatPercent, strategyLabel, toNumber } from "@/lib/backtests/format";
import type { ForecastEnvelopeResponse, StrategyType } from "@backtestforecast/api-client";
import { isPlanLimitError, UpgradePrompt } from "@/components/billing/upgrade-prompt";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";

const STRATEGY_OPTIONS = [
  { value: "", label: "Any (default: Long Call)" },
  { value: "long_call", label: "Long Call" },
  { value: "long_put", label: "Long Put" },
  { value: "covered_call", label: "Covered Call" },
  { value: "cash_secured_put", label: "Cash-Secured Put" },
  { value: "bull_call_debit_spread", label: "Bull Call Spread" },
  { value: "bear_put_debit_spread", label: "Bear Put Spread" },
  { value: "iron_condor", label: "Iron Condor" },
  { value: "long_straddle", label: "Long Straddle" },
  { value: "long_strangle", label: "Long Strangle" },
];

const HORIZON_OPTIONS = [
  { value: "10", label: "10 days" },
  { value: "20", label: "20 days" },
  { value: "30", label: "30 days" },
  { value: "45", label: "45 days" },
  { value: "60", label: "60 days" },
  { value: "90", label: "90 days" },
];

function returnColor(value: number): string {
  if (value > 0) return "text-emerald-600 dark:text-emerald-400";
  if (value < 0) return "text-red-500 dark:text-red-400";
  return "text-muted-foreground";
}

export function ForecastLookup() {
  const { getToken } = useAuth();
  const [ticker, setTicker] = useState("SPY");
  const [strategyType, setStrategyType] = useState("");
  const [horizonDays, setHorizonDays] = useState("20");
  const [status, setStatus] = useState<"idle" | "loading" | "error">("idle");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [errorCode, setErrorCode] = useState<string | undefined>();
  const [result, setResult] = useState<ForecastEnvelopeResponse | null>(null);
  const mountedRef = useRef(true);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    return () => {
      mountedRef.current = false;
      abortRef.current?.abort();
    };
  }, []);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const normalizedTicker = ticker.trim().toUpperCase();
    if (!normalizedTicker) {
      setStatus("error");
      setErrorMessage("Enter a ticker symbol.");
      return;
    }
    if (!/^[A-Z0-9./^]{1,16}$/.test(normalizedTicker)) {
      setStatus("error");
      setErrorMessage("Ticker may only contain letters, digits, dots, slashes, or ^ (max 16 characters).");
      return;
    }

    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    setStatus("loading");
    setErrorMessage(null);
    setErrorCode(undefined);
    setResult(null);

    try {
      const token = await getToken();
      if (!token) throw new Error("Session expired.");

      const data = await fetchForecast(token, normalizedTicker, {
        strategyType: strategyType || undefined,
        horizonDays: Number(horizonDays),
        signal: controller.signal,
      });
      if (!mountedRef.current || controller.signal.aborted) return;
      setResult(data);
      setStatus("idle");
    } catch (error) {
      if (!mountedRef.current || controller.signal.aborted) return;
      const msg =
        error instanceof ApiError
          ? error.message
          : error instanceof Error
            ? error.message
            : "Forecast could not be loaded.";
      const code = error instanceof ApiError ? error.code : undefined;
      setStatus("error");
      setErrorMessage(msg);
      setErrorCode(code);
    }
  }

  const forecast = result?.forecast;

  return (
    <div className="space-y-6">
      <form className="space-y-4" onSubmit={handleSubmit}>
        <Card>
          <CardHeader>
            <CardTitle>Forecast lookup</CardTitle>
            <CardDescription>
              Enter a ticker and horizon to see a probabilistic expected-return range
              based on historical analog setups.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid gap-4 sm:grid-cols-3">
              <div className="space-y-2">
                <Label htmlFor="forecastTicker">Ticker</Label>
                <Input
                  id="forecastTicker"
                  autoComplete="off"
                  maxLength={16}
                  placeholder="SPY"
                  value={ticker}
                  onChange={(e) => setTicker(e.target.value.toUpperCase().replace(/[^A-Z0-9./^]/g, ""))}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="forecastStrategy">Strategy context (optional)</Label>
                <Select
                  id="forecastStrategy"
                  value={strategyType}
                  options={STRATEGY_OPTIONS}
                  onChange={(e) => setStrategyType(e.target.value)}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="forecastHorizon">Horizon</Label>
                <Select
                  id="forecastHorizon"
                  value={horizonDays}
                  options={HORIZON_OPTIONS}
                  onChange={(e) => setHorizonDays(e.target.value)}
                />
              </div>
            </div>
            <div className="flex justify-end">
              <Button disabled={status === "loading"} type="submit">
                {status === "loading" ? (
                  <>
                    <Loader2 className="h-4 w-4 animate-spin" />
                    Loading forecast...
                  </>
                ) : (
                  <>
                    <TrendingUp className="h-4 w-4" />
                    Get forecast
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
        <div role="alert" className="rounded-xl border border-destructive/40 bg-destructive/5 p-4 text-sm text-destructive">
          {errorMessage}
        </div>
      ) : null}

      {forecast ? (
        <div className="space-y-6">
          <Card>
            <CardHeader>
              <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
                <div>
                  <CardTitle>{forecast.symbol} — Expected return range</CardTitle>
                  <CardDescription>
                    {forecast.horizon_days}-day horizon · As of {formatDate(forecast.as_of_date)}
                    {forecast.strategy_type
                      ? ` · ${strategyLabel(forecast.strategy_type)}`
                      : ""}
                  </CardDescription>
                </div>
                <div className="text-right">
                  <p className="text-xs text-muted-foreground">Expected move</p>
                  <p className="text-xl font-semibold tracking-tight">
                    ±{formatPercent(result?.expected_move_abs_pct ?? 0)}
                  </p>
                </div>
              </div>
            </CardHeader>
            <CardContent className="space-y-4">
              {/* Range visualization */}
              <div className="rounded-xl border border-border/70 p-4">
                <div className="flex items-center justify-between text-sm text-muted-foreground">
                  <span>Low</span>
                  <span>Median</span>
                  <span>High</span>
                </div>
                <div className="relative mt-2 h-8 w-full rounded-full bg-muted overflow-hidden">
                  <RangeBar
                    low={toNumber(forecast.expected_return_low_pct)}
                    median={toNumber(forecast.expected_return_median_pct)}
                    high={toNumber(forecast.expected_return_high_pct)}
                  />
                </div>
                <div className="mt-2 flex items-center justify-between">
                  <span className={`text-lg font-semibold ${returnColor(toNumber(forecast.expected_return_low_pct))}`}>
                    {formatPercent(forecast.expected_return_low_pct)}
                  </span>
                  <span className={`text-xl font-bold ${returnColor(toNumber(forecast.expected_return_median_pct))}`}>
                    {formatPercent(forecast.expected_return_median_pct)}
                  </span>
                  <span className={`text-lg font-semibold ${returnColor(toNumber(forecast.expected_return_high_pct))}`}>
                    {formatPercent(forecast.expected_return_high_pct)}
                  </span>
                </div>
              </div>

              <div className="grid gap-4 sm:grid-cols-3">
                <div className="rounded-xl border border-border/70 p-4">
                  <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">
                    Positive outcome rate
                  </p>
                  <p className="mt-2 text-2xl font-semibold tracking-tight">
                    {formatPercent(forecast.positive_outcome_rate_pct)}
                  </p>
                  <p className="mt-1 text-xs text-muted-foreground">
                    Of historical analogs that were positive
                  </p>
                </div>
                <div className="rounded-xl border border-border/70 p-4">
                  <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">
                    Analog count
                  </p>
                  <p className="mt-2 text-2xl font-semibold tracking-tight">
                    {forecast.analog_count}
                  </p>
                  <p className="mt-1 text-xs text-muted-foreground">
                    Historical setups matched
                  </p>
                </div>
                <div className="rounded-xl border border-border/70 p-4">
                  <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">
                    Horizon
                  </p>
                  <p className="mt-2 text-2xl font-semibold tracking-tight">
                    {forecast.horizon_days} days
                  </p>
                  <p className="mt-1 text-xs text-muted-foreground">
                    Forward-looking window
                  </p>
                </div>
              </div>

              <div className="rounded-xl border border-border/70 bg-muted/30 p-4 text-sm text-muted-foreground space-y-2">
                <p>{forecast.summary}</p>
                <p className="text-xs">{forecast.disclaimer}</p>
                <p className="text-xs">{result?.probabilistic_note}</p>
              </div>
            </CardContent>
          </Card>
        </div>
      ) : null}
    </div>
  );
}

/**
 * Simple visual range bar for low → median → high expected returns.
 */
function RangeBar({
  low,
  median,
  high,
}: {
  low: number;
  median: number;
  high: number;
}) {
  if (!Number.isFinite(low) || !Number.isFinite(median) || !Number.isFinite(high)) {
    return <div className="absolute inset-0 rounded-full bg-muted" />;
  }

  const range = high - low || 1;
  const medianPct = ((median - low) / range) * 100;
  const zeroPct = low >= 0 ? 0 : ((-low) / range) * 100;

  return (
    <>
      {/* Background gradient: red (left) → green (right) */}
      <div
        className="absolute inset-0 rounded-full"
        style={{
          background: `linear-gradient(to right, hsl(0 84% 60% / 0.25), hsl(0 0% 50% / 0.1) ${zeroPct}%, hsl(142 71% 45% / 0.25))`,
        }}
      />
      {/* Zero line if in range */}
      {low < 0 && high > 0 ? (
        <div
          className="absolute top-0 bottom-0 w-px bg-border"
          style={{ left: `${zeroPct}%` }}
        />
      ) : null}
      {/* Median marker */}
      <div
        className="absolute top-1/2 h-5 w-5 -translate-x-1/2 -translate-y-1/2 rounded-full bg-primary shadow-sm"
        style={{ left: `${Math.min(Math.max(medianPct, 5), 95)}%` }}
      />
    </>
  );
}
