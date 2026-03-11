import Link from "next/link";
import type { BacktestRunHistoryItemResponse } from "@/lib/backtests/types";
import {
  formatCurrency,
  formatDate,
  formatDateTime,
  formatPercent,
  statusLabel,
  strategyLabel,
} from "@/lib/backtests/format";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

function statusBadgeVariant(status: string): "default" | "success" | "destructive" | "secondary" {
  switch (status) {
    case "succeeded":
      return "success";
    case "failed":
      return "destructive";
    case "running":
      return "default";
    default:
      return "secondary";
  }
}

function HistoryTable({ items }: { items: BacktestRunHistoryItemResponse[] }) {
  if (items.length === 0) {
    return (
      <div className="rounded-xl border border-dashed p-10 text-center">
        <p className="text-base font-medium">No runs yet</p>
        <p className="mt-2 text-sm text-muted-foreground">Create your first manual backtest to populate history.</p>
      </div>
    );
  }

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Run</TableHead>
          <TableHead>Window</TableHead>
          <TableHead>Created</TableHead>
          <TableHead>Trades</TableHead>
          <TableHead>ROI</TableHead>
          <TableHead>Net P&amp;L</TableHead>
          <TableHead>Status</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {items.map((item) => (
          <TableRow key={item.id}>
            <TableCell>
              <div className="space-y-1">
                <Link className="font-medium hover:text-primary" href={`/app/backtests/${item.id}`}>
                  {item.symbol}
                </Link>
                <p className="text-xs text-muted-foreground">{strategyLabel(item.strategy_type)}</p>
              </div>
            </TableCell>
            <TableCell>
              <div className="space-y-1">
                <p>
                  {formatDate(item.date_from)} – {formatDate(item.date_to)}
                </p>
                <p className="text-xs text-muted-foreground">
                  Target DTE {item.target_dte} · Max hold {item.max_holding_days}
                </p>
              </div>
            </TableCell>
            <TableCell className="text-muted-foreground">{formatDateTime(item.created_at)}</TableCell>
            <TableCell>{item.summary.trade_count}</TableCell>
            <TableCell>{formatPercent(item.summary.total_roi_pct)}</TableCell>
            <TableCell>{formatCurrency(item.summary.total_net_pnl)}</TableCell>
            <TableCell>
              <Badge variant={statusBadgeVariant(item.status)}>
                {statusLabel(item.status)}
              </Badge>
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

export function HistoryList({
  items,
  embedded = false,
}: {
  items: BacktestRunHistoryItemResponse[];
  embedded?: boolean;
}) {
  if (embedded) {
    return <HistoryTable items={items} />;
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Backtest history</CardTitle>
        <CardDescription>Recent runs from the existing backend history endpoint.</CardDescription>
      </CardHeader>
      <CardContent>
        <HistoryTable items={items} />
      </CardContent>
    </Card>
  );
}
