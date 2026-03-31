import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

type WarningItem = {
  code: string;
  message: string;
  severity?: string | null;
};

function asWarnings(value: unknown[] | null | undefined): WarningItem[] {
  return (value ?? []).filter((item): item is WarningItem => {
    if (item == null || typeof item !== "object") return false;
    const candidate = item as Record<string, unknown>;
    return typeof candidate.code === "string" && typeof candidate.message === "string";
  });
}

function boundaryMessages(dataSource: string | null | undefined, warnings: WarningItem[]): string[] {
  const codes = new Set(warnings.map((warning) => warning.code));
  const messages: string[] = [];
  if (dataSource === "historical_flatfile" || codes.has("historical_aggregate_close_pricing")) {
    messages.push(
      "This run used local historical flat-file option aggregates. Option prices are derived from daily close data, not quote-mid or full NBBO history, so fills and mark values are approximate.",
    );
  }
  if (codes.has("ex_dividend_dates_unavailable")) {
    messages.push(
      "Dividend dates were unavailable for part of this run. Early-assignment-sensitive strategies can be materially wrong when dividend data is missing.",
    );
  }
  return messages;
}

export function DataQualityBoundaryCard({
  dataSource,
  warnings,
}: {
  dataSource?: string | null;
  warnings?: unknown[] | null;
}) {
  const messages = boundaryMessages(dataSource, asWarnings(warnings));
  if (messages.length === 0) return null;

  return (
    <Card className="border-amber-500/40 bg-amber-500/5">
      <CardHeader>
        <CardTitle>Data quality boundary</CardTitle>
        <CardDescription>
          This result used a degraded market-data mode or a missing supporting dataset. Treat the metrics as directional, not execution-equivalent.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-2">
        {messages.map((message) => (
          <p key={message} className="text-sm text-muted-foreground">
            {message}
          </p>
        ))}
      </CardContent>
    </Card>
  );
}
