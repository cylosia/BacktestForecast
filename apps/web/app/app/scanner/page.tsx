import Link from "next/link";
import { PlusCircle } from "lucide-react";
import { getCurrentUser, getScannerJobs } from "@/lib/api/server";
import { formatDateTime, statusLabel } from "@/lib/backtests/format";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { UpgradePrompt } from "@/components/billing/upgrade-prompt";

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

export default async function ScannerPage() {
  let user;
  try {
    user = await getCurrentUser();
  } catch {
    return <div className="p-8 text-center text-muted-foreground">Unable to load user data. Please try again.</div>;
  }
  const scannerModes = user.features.scanner_modes ?? [];
  const hasAccess = scannerModes.length > 0;

  let jobs: Awaited<ReturnType<typeof getScannerJobs>> | null = null;
  if (hasAccess) {
    try {
      jobs = await getScannerJobs(20);
    } catch {
      jobs = null;
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Scanner</p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Strategy scanner</h1>
          <p className="mt-2 text-muted-foreground">
            Run multi-symbol, multi-strategy scans and get ranked recommendations
            based on backtest performance, historical patterns, and forecast alignment.
          </p>
        </div>
        {hasAccess ? (
          <Button asChild>
            <Link href="/app/scanner/new">
              <PlusCircle className="h-4 w-4" />
              New scan
            </Link>
          </Button>
        ) : null}
      </div>

      {!hasAccess ? (
        <UpgradePrompt message="Scanner access requires a Pro or Premium plan. Upgrade to run multi-symbol strategy scans with ranked recommendations." />
      ) : null}

      <div className="grid gap-4 md:grid-cols-3">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Scanner modes</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-3xl font-semibold tracking-tight">
              {scannerModes.length > 0 ? scannerModes.join(" + ") : "None"}
            </p>
            <p className="mt-1 text-sm text-muted-foreground">
              {scannerModes.includes("advanced")
                ? "Full strategy and symbol access"
                : scannerModes.includes("basic")
                  ? "Basic scanner — upgrade to Premium for advanced"
                  : "Upgrade to Pro for basic scanner access"}
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Recent scans</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-3xl font-semibold tracking-tight">
              {jobs ? jobs.items.length : "—"}
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">Plan tier</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-3xl font-semibold tracking-tight capitalize">{user.plan_tier}</p>
          </CardContent>
        </Card>
      </div>

      {hasAccess && jobs ? (
        <Card>
          <CardHeader>
            <CardTitle>Scanner history</CardTitle>
            <CardDescription>
              {jobs.items.length === 0
                ? "No scanner jobs yet. Create your first scan."
                : `${jobs.items.length} recent scanner job(s).`}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {jobs.items.length === 0 ? (
              <div className="rounded-xl border border-dashed p-10 text-center">
                <p className="text-base font-medium">No scanner jobs yet</p>
                <p className="mt-2 text-sm text-muted-foreground">
                  Create a scan to evaluate multiple symbols and strategies at once.
                </p>
                <Button asChild className="mt-4">
                  <Link href="/app/scanner/new">Create your first scan</Link>
                </Button>
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Name</TableHead>
                    <TableHead>Mode</TableHead>
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
                        <Link className="font-medium hover:text-primary" href={`/app/scanner/${job.id}`}>
                          {job.name ?? "Unnamed scan"}
                        </Link>
                        <p className="text-xs text-muted-foreground">{job.job_kind}</p>
                      </TableCell>
                      <TableCell><Badge variant="secondary">{job.mode}</Badge></TableCell>
                      <TableCell>{job.evaluated_candidate_count} / {job.candidate_count}</TableCell>
                      <TableCell>{job.recommendation_count}</TableCell>
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
