"use client";

import type { PipelineHistoryResponse } from "@backtestforecast/api-client";
import { formatDate, formatDateTime } from "@/lib/backtests/format";
import { statusBadgeVariant } from "@/lib/ui/status-badge";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

export function PicksHistory({ data }: { data: PipelineHistoryResponse }) {
  if (data.items.length === 0) {
    return (
      <Card>
        <CardContent className="p-6 text-center text-muted-foreground">
          No pipeline history yet.
        </CardContent>
      </Card>
    );
  }

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
            {data.items.map((item) => (
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
      </CardContent>
    </Card>
  );
}
