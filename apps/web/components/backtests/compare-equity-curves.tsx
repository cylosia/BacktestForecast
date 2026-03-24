"use client";

import { useMemo } from "react";
import type { BacktestRunDetailResponse } from "@backtestforecast/api-client";
import { formatCurrency, strategyLabel, toNumber } from "@/lib/backtests/format";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

const WIDTH = 720;
const HEIGHT = 300;
const PADDING_X = 18;
const PADDING_Y = 24;

const COLORS = [
  "hsl(221 83% 53%)",   // blue
  "hsl(142 71% 45%)",   // green
  "hsl(0 84% 60%)",     // red
  "hsl(38 92% 50%)",    // amber
  "hsl(262 83% 58%)",   // purple
  "hsl(190 90% 40%)",   // teal
  "hsl(330 81% 60%)",   // pink
  "hsl(25 95% 53%)",    // orange
  "hsl(173 80% 36%)",   // emerald
  "hsl(47 96% 53%)",    // yellow
  "hsl(292 84% 61%)",   // fuchsia
  "hsl(210 40% 50%)",   // slate-blue
];

export function CompareEquityCurves({
  runs,
}: {
  runs: BacktestRunDetailResponse[];
}) {
  const nonEmptyEntries = runs
    .map((r, i) => ({ run: r, originalIndex: i }))
    .filter((entry) => (entry.run.equity_curve ?? []).length > 0);

  let globalMin = Infinity;
  let globalMax = -Infinity;

  for (const { run } of nonEmptyEntries) {
    for (const point of run.equity_curve) {
      const raw = toNumber(point.equity);
      const eq = Number.isFinite(raw) ? raw : 0;
      if (eq < globalMin) globalMin = eq;
      if (eq > globalMax) globalMax = eq;
    }
  }

  if (!Number.isFinite(globalMin)) globalMin = 0;
  if (!Number.isFinite(globalMax)) globalMax = 0;
  const range = globalMax - globalMin || 1;

  const toPath = useMemo(() => {
    const MAX_SVG_POINTS = 500;

    const downsample = <T,>(arr: T[]): T[] => {
      if (arr.length <= MAX_SVG_POINTS) return arr;
      const result: T[] = [arr[0]];
      const stride = (arr.length - 1) / (MAX_SVG_POINTS - 1);
      for (let i = 1; i < MAX_SVG_POINTS - 1; i++) {
        result.push(arr[Math.round(i * stride)]);
      }
      result.push(arr[arr.length - 1]);
      return result;
    };

    return (run: BacktestRunDetailResponse): string => {
      const sampled = downsample(run.equity_curve);
      const count = sampled.length;
      return sampled
        .map((point, index) => {
          const raw = toNumber(point.equity);
          const eq = Number.isFinite(raw) ? raw : 0;
          const x = PADDING_X + (index / Math.max(count - 1, 1)) * (WIDTH - PADDING_X * 2);
          const y =
            HEIGHT -
            PADDING_Y -
            ((eq - globalMin) / range) * (HEIGHT - PADDING_Y * 2);
          return `${index === 0 ? "M" : "L"} ${x} ${y}`;
        })
        .join(" ");
    };
  }, [globalMin, range]);

  if (nonEmptyEntries.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Equity curves</CardTitle>
          <CardDescription>No equity data is available for the compared runs.</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <CardTitle>Equity curves</CardTitle>
            <CardDescription>Overlaid curves normalized to the same y-axis scale.</CardDescription>
          </div>
          <div className="flex flex-wrap gap-3 text-sm">
            {nonEmptyEntries.map(({ run, originalIndex }, colorIdx) => (
              <div key={run.id} className="flex items-center gap-2">
                <div
                  className="h-2.5 w-5 rounded-sm"
                  style={{ backgroundColor: COLORS[colorIdx % COLORS.length] }}
                />
                <span className="text-muted-foreground">
                  {String.fromCharCode(65 + originalIndex)} {run.symbol}
                </span>
              </div>
            ))}
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="overflow-hidden rounded-xl border border-border/70 bg-background/60 p-3">
          <svg className="h-auto w-full" viewBox={`0 0 ${WIDTH} ${HEIGHT}`} role="img">
            <title>Overlaid equity curves</title>
            {[0.25, 0.5, 0.75].map((step) => {
              const y = PADDING_Y + step * (HEIGHT - PADDING_Y * 2);
              return (
                <line
                  key={step}
                  x1={PADDING_X}
                  x2={WIDTH - PADDING_X}
                  y1={y}
                  y2={y}
                  stroke="hsl(var(--border))"
                  strokeDasharray="4 4"
                />
              );
            })}
            {nonEmptyEntries.map(({ run, originalIndex }, colorIdx) => (
              <path
                key={run.id}
                d={toPath(run)}
                fill="none"
                stroke={COLORS[colorIdx % COLORS.length]}
                strokeLinecap="round"
                strokeWidth="2.5"
                opacity="0.85"
              >
                <title>
                  {`${String.fromCharCode(65 + originalIndex)} ${run.symbol} — ${strategyLabel(run.strategy_type)} — End: ${run.summary ? formatCurrency(run.summary.ending_equity) : "N/A"}`}
                </title>
              </path>
            ))}
          </svg>
        </div>

        <div className="grid gap-3 text-sm sm:grid-cols-2 lg:grid-cols-4">
          <div className="rounded-lg border border-border/70 p-3">
            <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Y-axis min</p>
            <p className="mt-1 font-medium">{formatCurrency(globalMin)}</p>
          </div>
          <div className="rounded-lg border border-border/70 p-3">
            <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Y-axis max</p>
            <p className="mt-1 font-medium">{formatCurrency(globalMax)}</p>
          </div>
          {nonEmptyEntries.map(({ run, originalIndex }, colorIdx) => (
            <div key={run.id} className="rounded-lg border border-border/70 p-3">
              <div className="flex items-center gap-2 text-xs uppercase tracking-[0.16em] text-muted-foreground">
                <div
                  className="h-2 w-4 rounded-sm"
                  style={{ backgroundColor: COLORS[colorIdx % COLORS.length] }}
                />
                {String.fromCharCode(65 + originalIndex)} ending
              </div>
              <p className="mt-1 font-medium">{run.summary ? formatCurrency(run.summary.ending_equity) : "—"}</p>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
