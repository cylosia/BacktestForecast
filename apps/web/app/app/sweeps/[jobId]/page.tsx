import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import { getSweepJob, getSweepResults } from "@/lib/api/server";
import { formatDateTime, isTerminalStatus, statusLabel } from "@/lib/backtests/format";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { SweepJobPoller } from "@/components/sweeps/sweep-job-poller";
import { SweepResultList } from "@/components/sweeps/sweep-result-list";
import { statusBadgeVariant } from "@/lib/ui/status-badge";

export const dynamic = "force-dynamic";

export default async function SweepDetailPage({
  params,
}: {
  params: Promise<{ jobId: string }>;
}) {
  const { jobId } = await params;

  try {
    const job = await getSweepJob(jobId);
    const isComplete = isTerminalStatus(job.status);

    let results: any[] = [];
    let resultsError: string | null = null;
    if (job.status === "succeeded") {
      try {
        const data = await getSweepResults(jobId);
        results = data.items ?? [];
      } catch (err) {
        resultsError = err instanceof Error ? err.message : "Failed to load results.";
      }
    }

    return (
      <div className="space-y-6">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <Button asChild className="mb-4 px-0" variant="ghost">
              <Link href="/app/sweeps">
                <ArrowLeft className="h-4 w-4" />
                Back to sweeps
              </Link>
            </Button>
            <div className="flex flex-wrap items-center gap-3">
              <h1 className="text-3xl font-semibold tracking-tight">
                {job.symbol} sweep
              </h1>
              <Badge variant={statusBadgeVariant(job.status)}>
                {statusLabel(job.status)}
              </Badge>
            </div>
            <p className="mt-2 text-muted-foreground">
              Created {formatDateTime(job.created_at)}
              {job.completed_at ? ` · Completed ${formatDateTime(job.completed_at)}` : ""}
            </p>
          </div>

          <Button asChild>
            <Link href="/app/sweeps/new">Run another sweep</Link>
          </Button>
        </div>

        {!isComplete ? (
          <SweepJobPoller
            jobId={jobId}
            initialStatus={job.status}
            candidateCount={job.candidate_count}
          />
        ) : null}

        <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">Candidates</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-2xl font-semibold tracking-tight">{job.candidate_count}</p>
              <p className="mt-1 text-xs text-muted-foreground">Parameter combinations</p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">Evaluated</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-2xl font-semibold tracking-tight">{job.evaluated_candidate_count}</p>
              <p className="mt-1 text-xs text-muted-foreground">Backtests completed</p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">Results</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-2xl font-semibold tracking-tight">{job.result_count}</p>
              <p className="mt-1 text-xs text-muted-foreground">Top-ranked configurations</p>
            </CardContent>
          </Card>

          {job.prefetch_summary ? (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">Prefetch</CardTitle>
              </CardHeader>
              <CardContent>
                <p className="text-2xl font-semibold tracking-tight">
                  {job.prefetch_summary.quotes_fetched?.toLocaleString() ?? "—"}
                </p>
                <p className="mt-1 text-xs text-muted-foreground">
                  Quotes cached across {job.prefetch_summary.dates_processed ?? 0} dates
                </p>
              </CardContent>
            </Card>
          ) : (
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">Prefetch</CardTitle>
              </CardHeader>
              <CardContent>
                <p className="text-2xl font-semibold tracking-tight">—</p>
              </CardContent>
            </Card>
          )}
        </div>

        {job.status === "failed" && job.error_message ? (
          <Card>
            <CardHeader>
              <CardTitle>Execution error</CardTitle>
              <CardDescription>Error code: {job.error_code ?? "unknown"}</CardDescription>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-muted-foreground">{job.error_message}</p>
            </CardContent>
          </Card>
        ) : null}

        {(job.warnings ?? []).length > 0 ? (
          <Card>
            <CardHeader>
              <CardTitle>Warnings</CardTitle>
              <CardDescription>{(job.warnings ?? []).length} warning(s) from execution.</CardDescription>
            </CardHeader>
            <CardContent className="space-y-2">
              {(job.warnings ?? []).map((warning: Record<string, unknown>, index: number) => (
                <div key={`warn-${index}`} className="rounded-lg border border-border/70 p-3 text-sm">
                  <pre className="whitespace-pre-wrap break-words text-muted-foreground">
                    {JSON.stringify(warning, null, 2)}
                  </pre>
                </div>
              ))}
            </CardContent>
          </Card>
        ) : null}

        {job.status === "succeeded" && resultsError ? (
          <Card>
            <CardContent className="p-6 text-center text-destructive">{resultsError}</CardContent>
          </Card>
        ) : job.status === "succeeded" ? (
          <SweepResultList items={results} />
        ) : null}
      </div>
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Sweep job could not be loaded.";

    return (
      <div className="space-y-6">
        <Button asChild className="px-0" variant="ghost">
          <Link href="/app/sweeps">
            <ArrowLeft className="h-4 w-4" />
            Back to sweeps
          </Link>
        </Button>
        <Card>
          <CardHeader>
            <CardTitle>Unable to load sweep job</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground">{message}</p>
          </CardContent>
        </Card>
      </div>
    );
  }
}
