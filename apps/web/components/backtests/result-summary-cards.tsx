import type { BacktestSummaryResponse } from "@backtestforecast/api-client";
import { formatCurrency, formatNumber, formatPercent } from "@/lib/backtests/format";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

type NumericValue = string | number | null | undefined;

function safeRatio(value: NumericValue): string {
  if (value == null) return "—";
  const n = Number(value);
  return Number.isFinite(n) ? n.toFixed(2) : "—";
}

const primaryCards: Array<{
  key: keyof BacktestSummaryResponse;
  label: string;
  formatter: (value: NumericValue) => string;
}> = [
  { key: "trade_count", label: "Trades", formatter: (value) => (value != null ? formatNumber(value) : "—") },
  { key: "win_rate", label: "Win rate", formatter: (value) => (value != null ? formatPercent(value) : "—") },
  { key: "total_roi_pct", label: "Total ROI", formatter: (value) => (value != null ? formatPercent(value) : "—") },
  { key: "total_net_pnl", label: "Net P&L", formatter: (value) => (value != null ? formatCurrency(value) : "—") },
  { key: "max_drawdown_pct", label: "Max drawdown", formatter: (value) => (value != null ? formatPercent(value) : "—") },
  { key: "total_commissions", label: "Commissions", formatter: (value) => (value != null ? formatCurrency(value) : "—") },
];

function WinRateContext({ summary }: { summary: BacktestSummaryResponse }) {
  const decided = summary.decided_trades;
  const total = summary.trade_count;
  if (decided == null || decided === total || total === 0) return null;
  const breakEven = total - decided;
  return (
    <p className="mt-1 text-xs text-muted-foreground">
      Based on {decided} of {total} trades (excludes {breakEven} break-even)
    </p>
  );
}

const advancedCards: Array<{
  key: keyof BacktestSummaryResponse;
  label: string;
  formatter: (value: NumericValue) => string;
}> = [
  { key: "sharpe_ratio", label: "Sharpe ratio", formatter: safeRatio },
  { key: "sortino_ratio", label: "Sortino ratio", formatter: safeRatio },
  { key: "profit_factor", label: "Profit factor", formatter: safeRatio },
  { key: "expectancy", label: "Expectancy", formatter: (value) => (value != null ? formatCurrency(value) : "—") },
  { key: "cagr_pct", label: "CAGR", formatter: (value) => (value != null ? formatPercent(value) : "—") },
  { key: "payoff_ratio", label: "Payoff ratio", formatter: safeRatio },
  { key: "calmar_ratio", label: "Calmar ratio", formatter: safeRatio },
  { key: "recovery_factor", label: "Recovery factor", formatter: safeRatio },
  { key: "max_consecutive_wins", label: "Max consec. wins", formatter: (value) => (value != null ? String(value) : "—") },
  { key: "max_consecutive_losses", label: "Max consec. losses", formatter: (value) => (value != null ? String(value) : "—") },
];

function CardGrid({ cards, summary }: { cards: typeof primaryCards; summary: BacktestSummaryResponse }) {
  return (
    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
      {cards.map((item) => (
        <Card key={item.key}>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">{item.label}</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-semibold tracking-tight">{item.formatter(summary[item.key])}</p>
            {item.key === "win_rate" && <WinRateContext summary={summary} />}
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

export function ResultSummaryCards({ summary }: { summary: BacktestSummaryResponse }) {
  return (
    <div className="space-y-6">
      <CardGrid cards={primaryCards} summary={summary} />
      <CardGrid cards={advancedCards} summary={summary} />
    </div>
  );
}
