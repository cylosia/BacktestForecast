import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import { getScannerJob, getScannerRecommendations } from "@/lib/api/server";
import { formatDateTime, isTerminalStatus, statusLabel } from "@/lib/backtests/format";
import type { ScannerRecommendationResponse } from "@backtestforecast/api-client";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ScannerJobPoller } from "@/components/scanner/scanner-job-poller";
import { RecommendationList } from "@/components/scanner/recommendation-list";

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

export default async function ScannerDetailPage({
  params,
}: {
  params: Promise<{ jobId: string }>;
}) {
  const { jobId } = await params;

  try {
    const job = await getScannerJob(jobId);
    const isComplete = isTerminalStatus(job.status);

    let recommendations: ScannerRecommendationResponse[] = [];
    if (job.status === "succeeded") {
      try {
        const data = await getScannerRecommendations(jobId);
        recommendations = data.items;
      } catch {
        recommendations = [];
      }
    }

    return (
      <div className="space-y-6">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <Button asChild className="mb-4 px-0" variant="ghost">
              <Link href="/app/scanner">
                <ArrowLeft className="h-4 w-4" />
                Back to scanner
              </Link>
            </Button>
            <div className="flex flex-wrap items-center gap-3">
              <h1 className="text-3xl font-semibold tracking-tight">
                {job.name ?? "Scan results"}
              </h1>
              <Badge variant={statusBadgeVariant(job.status)}>
                {statusLabel(job.status)}
              </Badge>
              <Badge variant="secondary">{job.mode}</Badge>
            </div>
            <p className="mt-2 text-muted-foreground">
              Created {formatDateTime(job.created_at)}
              {job.completed_at ? ` · Completed ${formatDateTime(job.completed_at)}` : ""}
            </p>
          </div>

          <Button asChild>
            <Link href="/app/scanner/new">Run another scan</Link>
          </Button>
        </div>

        {!isComplete ? (
          <ScannerJobPoller
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
              <p className="mt-1 text-xs text-muted-foreground">Symbol × strategy × rule-set combinations</p>
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
              <CardTitle className="text-sm font-medium text-muted-foreground">Recommendations</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-2xl font-semibold tracking-tight">{job.recommendation_count}</p>
              <p className="mt-1 text-xs text-muted-foreground">Top-ranked results</p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">Plan snapshot</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-2xl font-semibold tracking-tight capitalize">{job.plan_tier_snapshot}</p>
              <p className="mt-1 text-xs text-muted-foreground">
                {job.refresh_daily ? "Daily refresh enabled" : "One-time scan"}
              </p>
            </CardContent>
          </Card>
        </div>

        {job.status === "failed" && job.error_message ? (
          <Card>
            <CardHeader>
              <CardTitle>Execution error</CardTitle>
              <CardDescription>
                Error code: {job.error_code ?? "unknown"}
              </CardDescription>
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
              {(job.warnings ?? []).map((warning, index) => (
                <div key={`warn-${index}`} className="rounded-lg border border-border/70 p-3 text-sm">
                  <pre className="whitespace-pre-wrap break-words text-muted-foreground">
                    {JSON.stringify(warning, null, 2)}
                  </pre>
                </div>
              ))}
            </CardContent>
          </Card>
        ) : null}

        {job.status === "succeeded" ? (
          <RecommendationList items={recommendations} />
        ) : null}
      </div>
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Scanner job could not be loaded.";

    return (
      <div className="space-y-6">
        <Button asChild className="px-0" variant="ghost">
          <Link href="/app/scanner">
            <ArrowLeft className="h-4 w-4" />
            Back to scanner
          </Link>
        </Button>
        <Card>
          <CardHeader>
            <CardTitle>Unable to load scanner job</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground">{message}</p>
          </CardContent>
        </Card>
      </div>
    );
  }
}
