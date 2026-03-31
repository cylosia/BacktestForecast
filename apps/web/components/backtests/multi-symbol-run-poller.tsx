"use client";

import { useCallback, useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { Loader2 } from "lucide-react";
import { fetchMultiSymbolRunStatus } from "@/lib/api/client";
import { isTerminalStatus, statusLabel } from "@/lib/backtests/format";
import type { MultiSymbolRunStatusResponse } from "@/lib/api/multi-workflow-types";
import { useSSE } from "@/hooks/use-sse";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

const POLL_INTERVAL_MS = 2_000;
const MAX_POLLS = 120;

export function MultiSymbolRunPoller({
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
    return fetchMultiSymbolRunStatus(token, runId, signal);
  }, [getToken, runId]);

  const { status: sseStatus } = useSSE<MultiSymbolRunStatusResponse>({
    resourceType: "multi_symbol_backtests",
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
          Multi-symbol backtest {statusLabel(status).toLowerCase()}
        </CardTitle>
        <CardDescription>
          Your run has been queued and will refresh automatically as worker status changes.
        </CardDescription>
      </CardHeader>
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
