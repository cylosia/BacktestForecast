"use client";

import { useCallback, useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { Loader2 } from "lucide-react";
import { fetchSweepJob } from "@/lib/api/client";
import { isTerminalStatus, statusLabel } from "@/lib/backtests/format";
import { useSSE } from "@/hooks/use-sse";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

const POLL_INTERVAL_MS = 3_000;
const MAX_POLLS = 400;

export function SweepJobPoller({
  jobId,
  initialStatus,
  candidateCount,
}: {
  jobId: string;
  initialStatus: string;
  candidateCount: number;
}) {
  const router = useRouter();
  const { getToken } = useAuth();
  const [status, setStatus] = useState(initialStatus);
  const [evaluated, setEvaluated] = useState(0);

  const fetcher = useCallback(async (signal: AbortSignal) => {
    const token = await getToken();
    if (!token) throw new Error("No token");
    return fetchSweepJob(token, jobId, signal);
  }, [getToken, jobId]);

  const { status: sseStatus } = useSSE<any>({
    resourceType: "sweeps",
    resourceId: jobId,
    onProgress: (data: any) => {
      if (data.status) setStatus(data.status);
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
      onProgress: (job: any) => {
        setStatus(job.status);
        setEvaluated(job.evaluated_candidate_count);
      },
      isComplete: (job: any) => isTerminalStatus(job.status),
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
          Sweep {statusLabel(status).toLowerCase()}
        </CardTitle>
        <CardDescription>
          {status === "queued"
            ? "Your sweep is queued and will start processing shortly."
            : `Evaluating candidates — ${evaluated.toLocaleString()} of ${candidateCount.toLocaleString()} (${pct}%).`}
        </CardDescription>
      </CardHeader>
      {candidateCount > 0 && status === "running" ? (
        <CardContent>
          <div className="h-2 w-full overflow-hidden rounded-full bg-muted">
            <div
              className="h-full rounded-full bg-primary transition-all duration-500"
              style={{ width: `${Math.min(pct, 100)}%` }}
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
