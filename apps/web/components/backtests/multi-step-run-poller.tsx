"use client";

import { useCallback, useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { Loader2 } from "lucide-react";
import { fetchMultiStepRunStatus } from "@/lib/api/client";
import { isTerminalStatus, statusLabel } from "@/lib/backtests/format";
import type { MultiStepRunStatusResponse } from "@/lib/api/multi-workflow-types";
import { useSSE } from "@/hooks/use-sse";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

const POLL_INTERVAL_MS = 2_000;
const MAX_POLLS = 120;

export function MultiStepRunPoller({
  runId,
  initialStatus,
}: {
  runId: string;
  initialStatus: string;
}) {
  const router = useRouter();
  const { getToken } = useAuth();
  const [status, setStatus] = useState<string>(initialStatus);

  const fetcher = useCallback(async (signal: AbortSignal) => {
    const token = await getToken();
    if (!token) throw new Error("No token");
    return fetchMultiStepRunStatus(token, runId, signal);
  }, [getToken, runId]);

  const { status: sseStatus } = useSSE<MultiStepRunStatusResponse>({
    resourceType: "multi_step_backtests",
    resourceId: runId,
    onProgress: (data) => {
      if (typeof data.status === "string") setStatus(data.status);
    },
    onComplete: () => router.refresh(),
    isTerminal: isTerminalStatus,
    autoStart: !isTerminalStatus(initialStatus),
    pollingFallback: {
      fetcher,
      onComplete: () => router.refresh(),
      onProgress: (run) => setStatus(run.status),
      isComplete: (run) => isTerminalStatus(run.status),
      interval: POLL_INTERVAL_MS,
      maxAttempts: MAX_POLLS,
    },
  });

  if (isTerminalStatus(status)) return null;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-3">
          <Loader2 className="h-5 w-5 animate-spin text-primary" />
          Multi-step backtest {statusLabel(status).toLowerCase()}
        </CardTitle>
        <CardDescription>
          Your workflow is queued and this page will refresh automatically when the status changes.
        </CardDescription>
      </CardHeader>
      {sseStatus === "error" ? (
        <CardContent>
          <p className="text-sm text-muted-foreground">Unable to stream status updates. Refresh the page for the latest state.</p>
        </CardContent>
      ) : null}
    </Card>
  );
}
