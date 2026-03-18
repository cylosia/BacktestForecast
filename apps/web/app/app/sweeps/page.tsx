import Link from "next/link";
import { PlusCircle } from "lucide-react";
import { getCurrentUser, getSweepJobs } from "@/lib/api/server";
import type { SweepJobListResponse } from "@backtestforecast/api-client";
import { formatDateTime, statusLabel } from "@/lib/backtests/format";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { UpgradePrompt } from "@/components/billing/upgrade-prompt";
import { statusBadgeVariant } from "@/lib/ui/status-badge";

export const dynamic = "force-dynamic";

export default async function SweepsPage() {
  let user;
  try {
    user = await getCurrentUser();
  } catch {
    return <div className="p-8 text-center text-muted-foreground">Unable to load user data. Please try again.</div>;
  }
  const hasAccess = user.plan_tier !== "free" && user.features.forecasting_access === true;

  let jobs: SweepJobListResponse | null = null;
  let jobsError: string | null = null;
  if (hasAccess) {
    try {
      jobs = await getSweepJobs(20);
    } catch (err) {
      jobs = null;
      jobsError = err instanceof Error ? err.message : "Failed to load sweep history.";
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Sweeps</p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Parameter sweeps</h1>
          <p className="mt-2 text-muted-foreground">
            Run grid or genetic parameter sweeps across strategies, deltas, widths, and
            entry/exit rules to find optimal configurations for a symbol.
          </p>
        </div>
        {hasAccess ? (
          <Button asChild>
            <Link href="/app/sweeps/new">
              <PlusCircle className="h-4 w-4" />
              New sweep
            </Link>
          </Button>
        ) : null}
      </div>

      {!hasAccess ? (
        <UpgradePrompt message="Parameter sweeps require a Pro or Premium plan. Upgrade to run grid and genetic optimization sweeps across strategy parameters." />
      ) : null}

      <div className="grid gap-4 md:grid-cols-2">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Recent sweeps</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-3xl font-semibold tracking-tight">
              {jobs ? (jobs.items.length >= 20 ? "20+" : jobs.items.length) : "—"}
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Modes available</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-3xl font-semibold tracking-tight">Grid + Genetic</p>
            <p className="mt-1 text-sm text-muted-foreground">Named strategies and custom N-leg optimization</p>
          </CardContent>
        </Card>
      </div>

      {hasAccess && jobsError ? (
        <Card>
          <CardContent className="p-6 text-center text-destructive">{jobsError}</CardContent>
        </Card>
      ) : null}

      {hasAccess && jobs ? (
        <Card>
          <CardHeader>
            <CardTitle>Sweep history</CardTitle>
            <CardDescription>
              {jobs.items.length === 0
                ? "No sweep jobs yet. Create your first sweep."
                : `${jobs.items.length} recent sweep job(s).`}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {jobs.items.length === 0 ? (
              <div className="rounded-xl border border-dashed p-10 text-center">
                <p className="text-base font-medium">No sweep jobs yet</p>
                <p className="mt-2 text-sm text-muted-foreground">
                  Create a sweep to optimize strategy parameters for a symbol.
                </p>
                <Button asChild className="mt-4">
                  <Link href="/app/sweeps/new">Create your first sweep</Link>
                </Button>
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Symbol</TableHead>
                    <TableHead>Candidates</TableHead>
                    <TableHead>Results</TableHead>
                    <TableHead>Created</TableHead>
                    <TableHead>Status</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {jobs.items.map((job) => (
                    <TableRow key={job.id}>
                      <TableCell>
                        <Link className="font-medium hover:text-primary" href={`/app/sweeps/${job.id}`}>
                          {job.symbol}
                        </Link>
                      </TableCell>
                      <TableCell>{job.evaluated_candidate_count} / {job.candidate_count}</TableCell>
                      <TableCell>{job.result_count}</TableCell>
                      <TableCell className="text-muted-foreground">{formatDateTime(job.created_at)}</TableCell>
                      <TableCell>
                        <Badge variant={statusBadgeVariant(job.status)}>{statusLabel(job.status)}</Badge>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>
      ) : null}
    </div>
  );
}
