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
];

export function CompareEquityCurves({
  runs,
}: {
  runs: BacktestRunDetailResponse[];
}) {
  const nonEmptyRuns = runs.filter((r) => r.equity_curve.length > 0);

  if (nonEmptyRuns.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Equity curves</CardTitle>
          <CardDescription>No equity data is available for the compared runs.</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  // Compute global min/max equity across all runs
  let globalMin = Infinity;
  let globalMax = -Infinity;
  let maxPointCount = 0;

  for (const run of nonEmptyRuns) {
    for (const point of run.equity_curve) {
      const eq = toNumber(point.equity);
      if (eq < globalMin) globalMin = eq;
      if (eq > globalMax) globalMax = eq;
    }
    if (run.equity_curve.length > maxPointCount) {
      maxPointCount = run.equity_curve.length;
    }
  }

  const range = globalMax - globalMin || 1;

  function toPath(run: BacktestRunDetailResponse): string {
    const points = run.equity_curve;
    const count = points.length;
    return points
      .map((point, index) => {
        const x = PADDING_X + (index / Math.max(count - 1, 1)) * (WIDTH - PADDING_X * 2);
        const y =
          HEIGHT -
          PADDING_Y -
          ((toNumber(point.equity) - globalMin) / range) * (HEIGHT - PADDING_Y * 2);
        return `${index === 0 ? "M" : "L"} ${x} ${y}`;
      })
      .join(" ");
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
            {nonEmptyRuns.map((run, index) => (
              <div key={run.id} className="flex items-center gap-2">
                <div
                  className="h-2.5 w-5 rounded-sm"
                  style={{ backgroundColor: COLORS[index % COLORS.length] }}
                />
                <span className="text-muted-foreground">
                  {String.fromCharCode(65 + runs.indexOf(run))} {run.symbol}
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
            {/* Gridlines */}
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
            {/* Curves */}
            {nonEmptyRuns.map((run, index) => (
              <path
                key={run.id}
                d={toPath(run)}
                fill="none"
                stroke={COLORS[index % COLORS.length]}
                strokeLinecap="round"
                strokeWidth="2.5"
                opacity="0.85"
              />
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
          {nonEmptyRuns.map((run, index) => (
            <div key={run.id} className="rounded-lg border border-border/70 p-3">
              <div className="flex items-center gap-2 text-xs uppercase tracking-[0.16em] text-muted-foreground">
                <div
                  className="h-2 w-4 rounded-sm"
                  style={{ backgroundColor: COLORS[index % COLORS.length] }}
                />
                {String.fromCharCode(65 + runs.indexOf(run))} ending
              </div>
              <p className="mt-1 font-medium">{formatCurrency(run.summary.ending_equity)}</p>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
