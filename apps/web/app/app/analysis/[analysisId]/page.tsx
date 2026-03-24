import Link from "next/link";
import { notFound } from "next/navigation";
import { ApiError } from "@/lib/api/shared";
import { getAnalysisDetail } from "@/lib/api/server";
import { formatCurrency, formatDateTime, formatNumber, formatPercent, strategyLabel } from "@/lib/backtests/format";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { statusBadgeVariant } from "@/lib/ui/status-badge";
import { getCancellationMessage } from "@/lib/jobs/ui-state";

export const dynamic = "force-dynamic";

function asMetricRecord(value: unknown): Record<string, number | string | null | undefined> {
  if (value != null && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, number | string | null | undefined>;
  }
  return {};
}

function decidedTradeContext(summary: Record<string, number | string | null | undefined>): string | null {
  const totalRaw = summary.trade_count;
  const decidedRaw = summary.decided_trades;
  const total = typeof totalRaw === "number" ? totalRaw : Number(totalRaw);
  const decided = typeof decidedRaw === "number" ? decidedRaw : Number(decidedRaw);
  if (Number.isNaN(total) || Number.isNaN(decided) || total <= 0 || decided === total) return null;
  const breakEven = total - decided;
  if (breakEven <= 0) return null;
  return `Based on ${formatNumber(decided)} of ${formatNumber(total)} trades (${formatNumber(breakEven)} break-even excluded)`;
}

export default async function AnalysisDetailPage({
  params,
}: {
  params: Promise<{ analysisId: string }>;
}) {
  const { analysisId } = await params;

  let analysis;
  try {
    analysis = await getAnalysisDetail(analysisId);
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) {
      notFound();
    }
    throw error;
  }

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between gap-4">
        <div>
          <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Deep analysis</p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">{analysis.symbol} analysis</h1>
          <p className="mt-2 text-muted-foreground">
            Created {formatDateTime(analysis.created_at)}. Status: {analysis.status}.
          </p>
        </div>
        <Link href="/app/analysis" className="text-sm font-medium text-primary underline underline-offset-2">
          Back to analysis history
        </Link>
      </div>

      <div className="grid gap-4 md:grid-cols-4">
        <Card>
          <CardHeader className="pb-2"><CardTitle className="text-sm">Status</CardTitle></CardHeader>
          <CardContent><Badge variant={statusBadgeVariant(analysis.status)}>{analysis.status}</Badge></CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2"><CardTitle className="text-sm">Stage</CardTitle></CardHeader>
          <CardContent><p className="text-2xl font-semibold">{analysis.stage}</p></CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2"><CardTitle className="text-sm">Top results</CardTitle></CardHeader>
          <CardContent><p className="text-2xl font-semibold">{analysis.top_results_count}</p></CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2"><CardTitle className="text-sm">Close price</CardTitle></CardHeader>
          <CardContent><p className="text-2xl font-semibold">{analysis.close_price != null ? formatCurrency(analysis.close_price) : "—"}</p></CardContent>
        </Card>
      </div>

      {analysis.integrity_warnings && analysis.integrity_warnings.length > 0 ? (
        <Card className="border-amber-500/40 bg-amber-500/5">
          <CardHeader>
            <CardTitle>Data integrity warnings</CardTitle>
            <CardDescription>Some stored analysis sections were omitted because persisted JSON failed validation.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-2 text-sm text-muted-foreground">
            {analysis.integrity_warnings.map((warning, index) => (
              <p key={`${index}-${warning}`}>{warning}</p>
            ))}
          </CardContent>
        </Card>
      ) : null}

      {analysis.top_results && analysis.top_results.length > 0 ? (
        <Card>
          <CardHeader>
            <CardTitle>Top configurations</CardTitle>
            <CardDescription>Ranked strategies from the completed deep analysis.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            {analysis.top_results.map((result) => {
              const summary = asMetricRecord(result.summary);
              const winRateContext = decidedTradeContext(summary);
              return (
              <div key={`${result.rank}-${result.strategy_type}`} className="rounded-xl border border-border/70 p-4">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <p className="font-semibold">#{result.rank} {strategyLabel(result.strategy_type)}</p>
                    <p className="text-sm text-muted-foreground">DTE {result.target_dte}</p>
                  </div>
                  <Badge variant="secondary">Score {formatNumber(result.score)}</Badge>
                </div>
                <div className="mt-3 grid gap-3 sm:grid-cols-3 text-sm">
                  <div>
                    <p className="text-muted-foreground">ROI</p>
                    <p>{summary.total_roi_pct != null ? formatPercent(summary.total_roi_pct) : "—"}</p>
                  </div>
                  <div>
                    <p className="text-muted-foreground">Win rate</p>
                    <p>{summary.win_rate != null ? formatPercent(summary.win_rate) : "—"}</p>
                    {winRateContext ? <p className="text-xs text-muted-foreground">{winRateContext}</p> : null}
                  </div>
                  <div>
                    <p className="text-muted-foreground">Trades</p>
                    <p>{summary.trade_count != null ? formatNumber(summary.trade_count) : "—"}</p>
                  </div>
                </div>
              </div>
              );
            })}
          </CardContent>
        </Card>
      ) : null}

      {analysis.error_message ? (
        <Card>
          <CardHeader><CardTitle>Diagnostic</CardTitle></CardHeader>
          <CardContent className="text-sm text-muted-foreground">{analysis.error_message}</CardContent>
        </Card>
      ) : null}

      {analysis.status === "cancelled" ? (
        <Card>
          <CardHeader>
            <CardTitle>Analysis cancelled</CardTitle>
            <CardDescription>This job ended in a cancelled state before the full workflow completed.</CardDescription>
          </CardHeader>
          <CardContent className="text-sm text-muted-foreground">
            {getCancellationMessage("analysis", analysis.error_code)}
          </CardContent>
        </Card>
      ) : null}
    </div>
  );
}
