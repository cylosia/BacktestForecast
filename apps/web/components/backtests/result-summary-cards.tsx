import type { BacktestSummaryResponse } from "@backtestforecast/api-client";
import { formatCurrency, formatNumber, formatPercent } from "@/lib/backtests/format";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

const summaryCards: Array<{
  key: keyof BacktestSummaryResponse;
  label: string;
  formatter: (value: BacktestSummaryResponse[keyof BacktestSummaryResponse]) => string;
}> = [
  { key: "trade_count", label: "Trades", formatter: (value) => (value != null ? formatNumber(value) : "—") },
  { key: "win_rate", label: "Win rate", formatter: (value) => (value != null ? formatPercent(value) : "—") },
  { key: "total_roi_pct", label: "Total ROI", formatter: (value) => (value != null ? formatPercent(value) : "—") },
  { key: "total_net_pnl", label: "Net P&L", formatter: (value) => (value != null ? formatCurrency(value) : "—") },
  { key: "max_drawdown_pct", label: "Max drawdown", formatter: (value) => (value != null ? formatPercent(value) : "—") },
  { key: "total_commissions", label: "Commissions", formatter: (value) => (value != null ? formatCurrency(value) : "—") },
];

export function ResultSummaryCards({ summary }: { summary: BacktestSummaryResponse }) {
  return (
    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
      {summaryCards.map((item) => (
        <Card key={item.key}>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">{item.label}</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-2xl font-semibold tracking-tight">{item.formatter(summary[item.key])}</p>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
