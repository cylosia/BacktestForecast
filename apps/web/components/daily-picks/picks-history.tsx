"use client";

import { useState } from "react";
import type { PipelineHistoryResponse } from "@backtestforecast/api-client";
import { formatDate, formatDateTime } from "@/lib/backtests/format";
import { statusBadgeVariant } from "@/lib/ui/status-badge";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

const PAGE_SIZE = 25;

export function PicksHistory({ data }: { data: PipelineHistoryResponse }) {
  const [currentPage, setCurrentPage] = useState(0);

  if (data.items.length === 0) {
    return (
      <Card>
        <CardContent className="p-6 text-center text-muted-foreground">
          No pipeline history yet.
        </CardContent>
      </Card>
    );
  }

  const totalPages = Math.ceil(data.items.length / PAGE_SIZE);
  const pageItems = data.items.slice(currentPage * PAGE_SIZE, (currentPage + 1) * PAGE_SIZE);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Pipeline history</CardTitle>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Trade Date</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Recommendations</TableHead>
              <TableHead>Duration</TableHead>
              <TableHead>Completed</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {pageItems.map((item) => (
              <TableRow key={item.id}>
                <TableCell className="font-medium">{formatDate(item.trade_date)}</TableCell>
                <TableCell>
                  <Badge variant={statusBadgeVariant(item.status)}>
                    {item.status}
                  </Badge>
                </TableCell>
                <TableCell>{item.recommendations_produced}</TableCell>
                <TableCell className="text-muted-foreground">
                  {item.duration_seconds != null ? (Math.round(Number(item.duration_seconds)) < 1 ? "<1s" : `${Math.round(Number(item.duration_seconds))}s`) : "—"}
                </TableCell>
                <TableCell className="text-muted-foreground">
                  {formatDateTime(item.completed_at)}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
        {totalPages > 1 && (
          <div className="mt-4 flex items-center justify-between">
            <p className="text-sm text-muted-foreground">
              Page {currentPage + 1} of {totalPages} ({data.items.length} items)
            </p>
            <div className="flex gap-2">
              <Button
                variant="outline"
                size="sm"
                disabled={currentPage === 0}
                onClick={() => setCurrentPage((p) => p - 1)}
              >
                Previous
              </Button>
              <Button
                variant="outline"
                size="sm"
                disabled={currentPage >= totalPages - 1}
                onClick={() => setCurrentPage((p) => p + 1)}
              >
                Next
              </Button>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
