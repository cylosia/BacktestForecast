"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { usePolling, type UsePollingOptions } from "@/hooks/use-polling";

export type SSEStatus = "connecting" | "streaming" | "polling" | "done" | "error";

export interface UseSSEOptions<T> {
  /** SSE resource type: "backtests" | "scans" | "exports" | "analyses" */
  resourceType: string;
  /** Resource ID (run_id, job_id, etc.) */
  resourceId: string;
  /** Called with each status update from SSE */
  onProgress?: (data: Record<string, unknown>) => void;
  /** Called when the resource reaches a terminal state */
  onComplete: () => void;
  /** Return true when the status is terminal */
  isTerminal: (status: string) => boolean;
  /** Whether to start automatically */
  autoStart?: boolean;
  /** Polling fallback config */
  pollingFallback: Omit<UsePollingOptions<T>, "autoStart">;
}

export interface UseSSEReturn {
  status: SSEStatus;
  cancel: () => void;
}

export function useSSE<T>({
  resourceType,
  resourceId,
  onProgress,
  onComplete,
  isTerminal,
  autoStart = true,
  pollingFallback,
}: UseSSEOptions<T>): UseSSEReturn {
  const [status, setStatus] = useState<SSEStatus>(autoStart ? "connecting" : "connecting");
  const [useFallback, setUseFallback] = useState(false);
  const esRef = useRef<EventSource | null>(null);
  const mountedRef = useRef(true);
  const onProgressRef = useRef(onProgress);
  const onCompleteRef = useRef(onComplete);
  const isTerminalRef = useRef(isTerminal);

  useEffect(() => { onProgressRef.current = onProgress; }, [onProgress]);
  useEffect(() => { onCompleteRef.current = onComplete; }, [onComplete]);
  useEffect(() => { isTerminalRef.current = isTerminal; }, [isTerminal]);

  const cancel = useCallback(() => {
    if (esRef.current) {
      esRef.current.close();
      esRef.current = null;
    }
  }, []);

  const { status: pollStatus, start: startPolling, cancel: cancelPolling } = usePolling<T>({
    ...pollingFallback,
    autoStart: false,
  });

  useEffect(() => {
    if (!autoStart) return;
    mountedRef.current = true;

    const url = `/api/events/${resourceType}/${resourceId}`;
    const es = new EventSource(url);
    esRef.current = es;
    let opened = false;

    es.onopen = () => {
      if (!mountedRef.current) return;
      opened = true;
      setStatus("streaming");
    };

    es.addEventListener("status", (event) => {
      if (!mountedRef.current) return;
      try {
        const data = JSON.parse(event.data) as Record<string, unknown>;
        onProgressRef.current?.(data);
        const eventStatus = String(data.status ?? "");
        if (isTerminalRef.current(eventStatus)) {
          setStatus("done");
          es.close();
          esRef.current = null;
          onCompleteRef.current();
        }
      } catch {
        // ignore parse errors
      }
    });

    es.addEventListener("done", () => {
      if (!mountedRef.current) return;
      es.close();
      esRef.current = null;
      setStatus("done");
      onCompleteRef.current();
    });

    es.onerror = () => {
      if (!mountedRef.current) return;
      es.close();
      esRef.current = null;
      setStatus("polling");
      setUseFallback(true);
      startPolling();
    };

    return () => {
      mountedRef.current = false;
      es.close();
      esRef.current = null;
      cancelPolling();
    };
  }, [autoStart, resourceType, resourceId, startPolling, cancelPolling]);

  useEffect(() => {
    if (useFallback) {
      if (pollStatus === "done") setStatus("done");
      else if (pollStatus === "error" || pollStatus === "timeout") setStatus("error");
    }
  }, [useFallback, pollStatus]);

  return { status, cancel };
}
