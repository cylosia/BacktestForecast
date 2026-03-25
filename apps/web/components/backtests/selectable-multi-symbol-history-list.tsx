"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { GitCompareArrows } from "lucide-react";
import type { MultiSymbolRunHistoryItemResponse } from "@/lib/api/multi-workflow-types";
import { formatCurrency, formatDateTime, formatPercent, statusLabel } from "@/lib/backtests/format";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { statusBadgeVariant } from "@/lib/ui/status-badge";

export function SelectableMultiSymbolHistoryList({
  items,
  comparisonLimit,
}: {
  items: MultiSymbolRunHistoryItemResponse[];
  comparisonLimit: number;
}) {
  const router = useRouter();
  const [selected, setSelected] = useState<Set<string>>(new Set());

  useEffect(() => {
    const validIds = new Set(items.map((item) => item.id));
    setSelected((prev) => {
      const next = new Set([...prev].filter((id) => validIds.has(id)));
      return next.size === prev.size ? prev : next;
    });
  }, [items]);

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
    router.push(`/app/multi-symbol-backtests/compare?ids=${encodeURIComponent(Array.from(selected).join(","))}`);
  }

  if (items.length === 0) {
    return (
      <div className="rounded-xl border border-dashed p-10 text-center">
        <p className="text-base font-medium">No multi-symbol runs yet</p>
        <p className="mt-2 text-sm text-muted-foreground">Create your first grouped workflow to populate history.</p>
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
              : `Select 2-${comparisonLimit} completed runs to compare`}
          </p>
          <Button disabled={!canCompare} size="sm" variant="outline" onClick={handleCompare}>
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
              <TableRow key={item.id} className={isSelected ? "bg-primary/5" : undefined}>
                {comparisonLimit >= 2 ? (
                  <TableCell>
                    {isSucceeded ? (
                      <input
                        type="checkbox"
                        checked={isSelected}
                        disabled={!isSelected && selected.size >= comparisonLimit}
                        className="h-4 w-4 rounded border-input focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
                        aria-label={`Select ${item.name ?? item.symbols.join(" + ")} for comparison`}
                        onChange={() => toggle(item.id)}
                      />
                    ) : null}
                  </TableCell>
                ) : null}
                <TableCell>
                  <div className="space-y-1">
                    <Link className="font-medium hover:text-primary" href={`/app/multi-symbol-backtests/${item.id}`}>
                      {item.name ?? item.symbols.join(" + ")}
                    </Link>
                    <p className="text-xs text-muted-foreground">{item.symbols.join(" + ")}</p>
                  </div>
                </TableCell>
                <TableCell className="text-muted-foreground">{formatDateTime(item.created_at)}</TableCell>
                <TableCell>{item.summary.trade_count}</TableCell>
                <TableCell>{formatPercent(item.summary.total_roi_pct)}</TableCell>
                <TableCell>{formatCurrency(item.summary.total_net_pnl)}</TableCell>
                <TableCell>
                  <Badge variant={statusBadgeVariant(item.status)}>{statusLabel(item.status)}</Badge>
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </div>
  );
}
