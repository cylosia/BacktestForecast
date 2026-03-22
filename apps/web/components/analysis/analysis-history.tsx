import Link from "next/link";
import type { AnalysisListResponse } from "@backtestforecast/api-client";
import { formatDateTime } from "@/lib/backtests/format";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

function statusVariant(status: string): "success" | "secondary" | "destructive" | "outline" {
  switch (status) {
    case "succeeded":
      return "success";
    case "running":
    case "queued":
      return "secondary";
    case "failed":
      return "destructive";
    default:
      return "outline";
  }
}

export function AnalysisHistory({ data }: { data: AnalysisListResponse }) {
  if (data.items.length === 0) {
    return (
      <Card>
        <CardContent className="p-6 text-center text-muted-foreground">
          No analyses yet.
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Analysis history</CardTitle>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Symbol</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Stage</TableHead>
              <TableHead>Top Results</TableHead>
              <TableHead>Created</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.items.map((item) => (
              <TableRow key={item.id}>
                <TableCell className="font-medium">
                  <Link href={`/app/analysis/${item.id}`} className="underline underline-offset-2 hover:text-primary">
                    {item.symbol}
                  </Link>
                </TableCell>
                <TableCell>
                  <Badge variant={statusVariant(item.status)}>
                    {item.status}
                  </Badge>
                </TableCell>
                <TableCell className="text-muted-foreground">{item.stage}</TableCell>
                <TableCell>{item.top_results_count}</TableCell>
                <TableCell className="text-muted-foreground">
                  {formatDateTime(item.created_at)}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}
