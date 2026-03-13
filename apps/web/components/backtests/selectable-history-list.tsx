"use client";

import { useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { GitCompareArrows } from "lucide-react";
import type { BacktestRunHistoryItemResponse } from "@backtestforecast/api-client";
import {
  formatCurrency,
  formatDate,
  formatDateTime,
  formatPercent,
  statusLabel,
  strategyLabel,
} from "@/lib/backtests/format";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
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

export function SelectableHistoryList({
  items,
  comparisonLimit,
}: {
  items: BacktestRunHistoryItemResponse[];
  comparisonLimit: number;
}) {
  const router = useRouter();
  const [selected, setSelected] = useState<Set<string>>(new Set());

  const canCompare = selected.size >= 2 && selected.size <= comparisonLimit;

  function toggle(id: string) {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else if (next.size < comparisonLimit) {
        next.add(id);
      }
      return next;
    });
  }

  function handleCompare() {
    if (!canCompare) return;
    const ids = Array.from(selected).join(",");
    router.push(`/app/backtests/compare?ids=${ids}`);
  }

  if (items.length === 0) {
    return (
      <div className="rounded-xl border border-dashed p-10 text-center">
        <p className="text-base font-medium">No runs yet</p>
        <p className="mt-2 text-sm text-muted-foreground">
          Create your first manual backtest to populate history.
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {comparisonLimit >= 2 ? (
        <div className="flex items-center justify-between">
          <p className="text-sm text-muted-foreground">
            {selected.size > 0
              ? `${selected.size} of ${comparisonLimit} runs selected`
              : `Select 2–${comparisonLimit} succeeded runs to compare`}
          </p>
          <Button
            disabled={!canCompare}
            size="sm"
            variant="outline"
            onClick={handleCompare}
          >
            <GitCompareArrows className="h-4 w-4" />
            Compare {selected.size > 0 ? `(${selected.size})` : ""}
          </Button>
        </div>
      ) : null}

      <Table>
        <TableHeader>
          <TableRow>
            {comparisonLimit >= 2 ? <TableHead className="w-10" /> : null}
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
          {items.map((item) => {
            const isSucceeded = item.status === "succeeded";
            const isSelected = selected.has(item.id);

            return (
              <TableRow
                key={item.id}
                className={isSelected ? "bg-primary/5" : undefined}
              >
                {comparisonLimit >= 2 ? (
                  <TableCell>
                    {isSucceeded ? (
                      <input
                        type="checkbox"
                        checked={isSelected}
                        disabled={!isSelected && selected.size >= comparisonLimit}
                        className="h-4 w-4 rounded border-input"
                        aria-label={`Select ${item.symbol} ${item.strategy_type} run for comparison`}
                        onChange={() => toggle(item.id)}
                      />
                    ) : null}
                  </TableCell>
                ) : null}
                <TableCell>
                  <div className="space-y-1">
                    <Link
                      className="font-medium hover:text-primary"
                      href={`/app/backtests/${item.id}`}
                    >
                      {item.symbol}
                    </Link>
                    <p className="text-xs text-muted-foreground">
                      {strategyLabel(item.strategy_type)}
                    </p>
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
                <TableCell className="text-muted-foreground">
                  {formatDateTime(item.created_at)}
                </TableCell>
                <TableCell>{item.summary.trade_count ?? "—"}</TableCell>
                <TableCell>{item.summary.total_roi_pct != null ? formatPercent(item.summary.total_roi_pct) : "—"}</TableCell>
                <TableCell>{item.summary.total_net_pnl != null ? formatCurrency(item.summary.total_net_pnl) : "—"}</TableCell>
                <TableCell>
                  <Badge variant={statusBadgeVariant(item.status)}>
                    {statusLabel(item.status)}
                  </Badge>
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </div>
  );
}
