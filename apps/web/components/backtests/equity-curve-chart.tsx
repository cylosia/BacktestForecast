import type { EquityCurvePointResponse } from "@backtestforecast/api-client";
import { formatCurrency, formatDate, formatPercent, toNumber } from "@/lib/backtests/format";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

const WIDTH = 720;
const HEIGHT = 260;
const PADDING_X = 18;
const PADDING_Y = 18;

export function EquityCurveChart({ points }: { points: EquityCurvePointResponse[] }) {
  if (points.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Equity curve</CardTitle>
          <CardDescription>No equity points were returned for this run.</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  const equityValues = points.map((point) => toNumber(point.equity));
  const minEquity = Math.min(...equityValues);
  const maxEquity = Math.max(...equityValues);
  const range = maxEquity - minEquity || 1;

  const chartPoints = points.map((point, index) => {
    const x =
      PADDING_X +
      (index / Math.max(points.length - 1, 1)) * (WIDTH - PADDING_X * 2);
    const y =
      HEIGHT -
      PADDING_Y -
      ((toNumber(point.equity) - minEquity) / range) * (HEIGHT - PADDING_Y * 2);

    return { x, y };
  });

  const path = chartPoints.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`).join(" ");
  const areaPath = `${path} L ${chartPoints[chartPoints.length - 1]?.x ?? WIDTH - PADDING_X} ${HEIGHT - PADDING_Y} L ${chartPoints[0]?.x ?? PADDING_X} ${HEIGHT - PADDING_Y} Z`;

  const firstEquity = points[0]?.equity ?? 0;
  const latestPoint = points[points.length - 1];
  const latestEquity = latestPoint?.equity ?? 0;
  const latestDrawdown = latestPoint?.drawdown_pct ?? 0;

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <CardTitle>Equity curve</CardTitle>
            <CardDescription>
              {formatDate(points[0].trade_date)} to {formatDate(points[points.length - 1].trade_date)}
            </CardDescription>
          </div>
          <div className="text-right">
            <p className="text-sm text-muted-foreground">Latest equity</p>
            <p className="text-xl font-semibold tracking-tight">{formatCurrency(latestEquity)}</p>
            <p className="text-xs text-muted-foreground">Drawdown {formatPercent(latestDrawdown)}</p>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="overflow-hidden rounded-xl border border-border/70 bg-background/60 p-3">
          <svg className="h-auto w-full" viewBox={`0 0 ${WIDTH} ${HEIGHT}`} role="img">
            <title>Equity curve chart</title>
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
            <path d={areaPath} fill="hsl(var(--primary) / 0.12)" />
            <path d={path} fill="none" stroke="hsl(var(--primary))" strokeLinecap="round" strokeWidth="3" />
            {chartPoints.map((point, index) => {
              if (index !== 0 && index !== chartPoints.length - 1) {
                return null;
              }

              return (
                <circle key={`${point.x}-${point.y}`} cx={point.x} cy={point.y} fill="hsl(var(--primary))" r="4" />
              );
            })}
          </svg>
        </div>

        <div className="grid gap-3 text-sm text-muted-foreground sm:grid-cols-3">
          <div className="rounded-lg border border-border/70 p-3">
            <p className="text-xs uppercase tracking-[0.16em]">Start</p>
            <p className="mt-1 text-base font-medium text-foreground">{formatCurrency(firstEquity)}</p>
          </div>
          <div className="rounded-lg border border-border/70 p-3">
            <p className="text-xs uppercase tracking-[0.16em]">Peak</p>
            <p className="mt-1 text-base font-medium text-foreground">{formatCurrency(maxEquity)}</p>
          </div>
          <div className="rounded-lg border border-border/70 p-3">
            <p className="text-xs uppercase tracking-[0.16em]">Finish</p>
            <p className="mt-1 text-base font-medium text-foreground">{formatCurrency(latestEquity)}</p>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
