"use client";

import { useCallback, useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { Loader2 } from "lucide-react";
import { fetchScannerJob } from "@/lib/api/client";
import { isTerminalStatus, statusLabel } from "@/lib/backtests/format";
import type { ScannerJobResponse, ScannerJobStatus } from "@backtestforecast/api-client";
import { useSSE } from "@/hooks/use-sse";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

const POLL_INTERVAL_MS = 3_000;
const MAX_POLLS = 200;

export function ScannerJobPoller({
  jobId,
  initialStatus,
  candidateCount,
}: {
  jobId: string;
  initialStatus: ScannerJobStatus;
  candidateCount: number;
}) {
  const router = useRouter();
  const { getToken } = useAuth();
  const [status, setStatus] = useState<ScannerJobStatus | string>(initialStatus);
  const [evaluated, setEvaluated] = useState(0);

  const fetcher = useCallback(async (signal: AbortSignal) => {
    const token = await getToken();
    if (!token) throw new Error("No token");
    return fetchScannerJob(token, jobId, signal);
  }, [getToken, jobId]);

  const { status: sseStatus } = useSSE<ScannerJobResponse>({
    resourceType: "scans",
    resourceId: jobId,
    onProgress: (data) => {
      const s = data.status as ScannerJobStatus | string;
      if (s) setStatus(s);
      if (typeof data.evaluated_candidate_count === "number") {
        setEvaluated(data.evaluated_candidate_count);
      }
    },
    onComplete: () => router.refresh(),
    isTerminal: isTerminalStatus,
    autoStart: !isTerminalStatus(initialStatus),
    pollingFallback: {
      fetcher,
      onComplete: () => router.refresh(),
      onProgress: (job) => {
        setStatus(job.status);
        setEvaluated(job.evaluated_candidate_count);
      },
      isComplete: (job) => isTerminalStatus(job.status),
      interval: POLL_INTERVAL_MS,
      maxAttempts: MAX_POLLS,
    },
  });

  if (isTerminalStatus(status)) return null;

  const pct = candidateCount > 0 ? Math.round((evaluated / candidateCount) * 100) : 0;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-3">
          <Loader2 className="h-5 w-5 animate-spin text-primary" />
          Scanner {statusLabel(status).toLowerCase()}
        </CardTitle>
        <CardDescription>
          {status === "queued"
            ? "Your scan is queued and will start processing shortly."
            : `Evaluating candidates — ${evaluated} of ${candidateCount} complete (${pct}%).`}
        </CardDescription>
      </CardHeader>
      {candidateCount > 0 && status === "running" ? (
        <CardContent>
          <div className="h-2 w-full overflow-hidden rounded-full bg-muted">
            <div
              className="h-full rounded-full bg-primary transition-all duration-500"
              style={{ width: `${pct}%` }}
            />
          </div>
        </CardContent>
      ) : null}
      {sseStatus === "error" ? (
        <CardContent>
          <p className="text-sm text-muted-foreground">
            Something went wrong while checking status. Refresh the page to see the latest results.
          </p>
        </CardContent>
      ) : null}
    </Card>
  );
}
