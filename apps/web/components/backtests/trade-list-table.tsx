import type { BacktestTradeResponse } from "@backtestforecast/api-client";
import {
  formatCurrency,
  formatDate,
  formatNumber,
  strategyLabel,
  toNumber,
} from "@/lib/backtests/format";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

export function TradeListTable({ trades }: { trades: BacktestTradeResponse[] }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Trade list</CardTitle>
        <CardDescription>{trades.length} trade(s) recorded for this run.</CardDescription>
      </CardHeader>
      <CardContent>
        {trades.length === 0 ? (
          <div className="rounded-xl border border-dashed p-8 text-center text-sm text-muted-foreground">
            No trades were produced for this backtest.
          </div>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Option</TableHead>
                <TableHead>Dates</TableHead>
                <TableHead>Details</TableHead>
                <TableHead>Pricing</TableHead>
                <TableHead>P&amp;L</TableHead>
                <TableHead>Exit</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {trades.map((trade, idx) => {
                const pnl = toNumber(trade.net_pnl);
                const isPositive = Number.isFinite(pnl) ? pnl >= 0 : null;

                return (
                  <TableRow key={trade.id ?? idx}>
                    <TableCell>
                      <div className="space-y-1">
                        <p className="font-medium">{trade.option_ticker}</p>
                        <p className="text-xs text-muted-foreground">{strategyLabel(trade.strategy_type)}</p>
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="space-y-1">
                        <p>{formatDate(trade.entry_date)}</p>
                        <p className="text-xs text-muted-foreground">Exit {formatDate(trade.exit_date)}</p>
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="space-y-1">
                        <p>{trade.quantity} contract(s)</p>
                        <p className="text-xs text-muted-foreground">
                          {formatNumber(trade.dte_at_open)} DTE · {formatNumber(trade.holding_period_days)} day hold
                        </p>
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="space-y-1">
                        <p>
                          {formatCurrency(trade.entry_mid)} → {formatCurrency(trade.exit_mid)}
                        </p>
                        <p className="text-xs text-muted-foreground">
                          Underlying {formatCurrency(trade.entry_underlying_close)} → {formatCurrency(trade.exit_underlying_close)}
                        </p>
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="space-y-1">
                        <p className={isPositive === null ? "text-muted-foreground" : isPositive ? "text-emerald-600 dark:text-emerald-400" : "text-destructive"}>
                          {formatCurrency(trade.net_pnl)}
                        </p>
                        <p className="text-xs text-muted-foreground">
                          Gross {formatCurrency(trade.gross_pnl)} · Fees {formatCurrency(trade.total_commissions)}
                        </p>
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="space-y-2">
                        <Badge variant={isPositive === null ? "outline" : isPositive ? "success" : "secondary"}>{trade.exit_reason}</Badge>
                        <p className="text-xs text-muted-foreground">Entered via {trade.entry_reason}</p>
                      </div>
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        )}
      </CardContent>
    </Card>
  );
}
