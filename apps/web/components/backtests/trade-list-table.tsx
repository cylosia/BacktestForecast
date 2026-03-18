"use client";

import { useState } from "react";
import type { BacktestTradeResponse } from "@backtestforecast/api-client";
import {
  formatCurrency,
  formatDate,
  formatNumber,
  formatReason,
  strategyLabel,
  toNumber,
} from "@/lib/backtests/format";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

const PAGE_SIZE = 100;

export function TradeListTable({ trades }: { trades: BacktestTradeResponse[] }) {
  const [visibleCount, setVisibleCount] = useState(PAGE_SIZE);
  const visibleTrades = trades.slice(0, visibleCount);
  const hasMore = visibleCount < trades.length;

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
          <Table aria-label="Trade list">
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
              {visibleTrades.map((trade, idx) => {
                const pnl = toNumber(trade.net_pnl);
                const isPositive = Number.isFinite(pnl) ? (pnl > 0 ? true : pnl < 0 ? false : null) : null;

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
                        <Badge variant={isPositive === null ? "outline" : isPositive ? "success" : "secondary"}>{formatReason(trade.exit_reason)}</Badge>
                        <p className="text-xs text-muted-foreground">Entered via {formatReason(trade.entry_reason)}</p>
                      </div>
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
          {trades.length > PAGE_SIZE && (
            <div className="mt-4 flex items-center justify-between text-sm text-muted-foreground">
              <p>Showing {Math.min(visibleCount, trades.length)} of {trades.length} trades</p>
              {hasMore && (
                <Button variant="outline" size="sm" onClick={() => setVisibleCount((c) => c + PAGE_SIZE)}>
                  Show more
                </Button>
              )}
            </div>
          )}
        )}
      </CardContent>
    </Card>
  );
}
