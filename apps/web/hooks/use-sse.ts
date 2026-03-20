/**
 * SSE hook for real-time job status updates with polling fallback.
 *
 * Used by sweep-job-poller for real-time progress tracking. Falls back
 * to polling via `usePolling` if the SSE connection fails.
 *
 * @see hooks/use-polling.ts for the polling fallback mechanism
 */
"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { usePolling, type UsePollingOptions } from "@/hooks/use-polling";

export type SSEStatus = "connecting" | "streaming" | "polling" | "done" | "error";

export interface UseSSEOptions<T> {
  /** SSE resource type: "backtests" | "scans" | "exports" | "analyses" | "sweeps" */
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
  const MAX_RETRIES = 3;
  const HEARTBEAT_TIMEOUT_MS = 45_000;
  const [status, setStatus] = useState<SSEStatus>(autoStart ? "connecting" : "done");
  const [useFallback, setUseFallback] = useState(false);
  const esRef = useRef<EventSource | null>(null);
  const mountedRef = useRef(true);
  const completedRef = useRef(false);
  const retryCountRef = useRef(0);
  const retryTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const lastEventIdRef = useRef<string | null>(null);
  const onProgressRef = useRef(onProgress);
  const onCompleteRef = useRef(onComplete);
  const isTerminalRef = useRef(isTerminal);

  useEffect(() => { onProgressRef.current = onProgress; }, [onProgress]);
  useEffect(() => { onCompleteRef.current = onComplete; }, [onComplete]);
  useEffect(() => { isTerminalRef.current = isTerminal; }, [isTerminal]);

  const { status: pollStatus, start: startPolling, cancel: cancelPolling } = usePolling<T>({
    ...pollingFallback,
    autoStart: false,
  });

  const startPollingRef = useRef(startPolling);
  const cancelPollingRef = useRef(cancelPolling);
  useEffect(() => { startPollingRef.current = startPolling; }, [startPolling]);
  useEffect(() => { cancelPollingRef.current = cancelPolling; }, [cancelPolling]);

  const cancel = useCallback(() => {
    if (esRef.current) {
      esRef.current.close();
      esRef.current = null;
    }
    cancelPollingRef.current();
  }, []);

  useEffect(() => {
    if (!autoStart) return;
    setUseFallback(false);
    completedRef.current = false;
    retryCountRef.current = 0;
    mountedRef.current = true;

    const url = `/api/events/${encodeURIComponent(resourceType)}/${encodeURIComponent(resourceId)}`;
    const es = new EventSource(url);
    esRef.current = es;
    let heartbeatTimer: ReturnType<typeof setTimeout> | null = null;

    const resetHeartbeat = (target: EventSource) => {
      if (heartbeatTimer) clearTimeout(heartbeatTimer);
      heartbeatTimer = setTimeout(() => {
        if (!mountedRef.current) return;
        target.close();
        esRef.current = null;
        setStatus("polling");
        setUseFallback(true);
        startPollingRef.current();
      }, HEARTBEAT_TIMEOUT_MS);
    };

    const attachEventHandlers = (target: EventSource) => {
      target.onopen = () => {
        if (!mountedRef.current) return;
        setStatus("streaming");
        resetHeartbeat(target);
      };

      target.addEventListener("status", (event) => {
        if (!mountedRef.current) return;
        retryCountRef.current = 0;
        resetHeartbeat(target);
        if (event.lastEventId) {
          lastEventIdRef.current = event.lastEventId;
        }
        try {
          const data = JSON.parse(event.data) as Record<string, unknown>;
          onProgressRef.current?.(data);
          const eventStatus = String(data.status ?? "");
          if (isTerminalRef.current(eventStatus)) {
            setStatus("done");
            target.close();
            esRef.current = null;
            if (!completedRef.current) {
              completedRef.current = true;
              onCompleteRef.current();
            }
          }
        } catch (parseErr) {
          if (process.env.NODE_ENV === "development") {
            console.warn("[SSE] Failed to parse event data", parseErr);
          }
        }
      });

      target.addEventListener("done", () => {
        if (!mountedRef.current) return;
        retryCountRef.current = 0;
        if (heartbeatTimer) clearTimeout(heartbeatTimer);
        target.close();
        esRef.current = null;
        setStatus("done");
        if (!completedRef.current) {
          completedRef.current = true;
          onCompleteRef.current();
        }
      });
    };

    attachEventHandlers(es);

    const setupEventSource = (eventSource: EventSource) => {
      eventSource.onerror = () => {
        if (!mountedRef.current) return;
        if (heartbeatTimer) clearTimeout(heartbeatTimer);
        eventSource.close();
        esRef.current = null;
        if (retryCountRef.current < MAX_RETRIES) {
          retryCountRef.current += 1;
          const delay = 1000 * Math.pow(2, retryCountRef.current - 1) * (0.5 + Math.random());
          if (retryTimeoutRef.current) clearTimeout(retryTimeoutRef.current);
          retryTimeoutRef.current = setTimeout(() => {
            if (!mountedRef.current) return;
            if (esRef.current) {
              esRef.current.close();
            }
            let retryUrl = `/api/events/${encodeURIComponent(resourceType)}/${encodeURIComponent(resourceId)}`;
            if (lastEventIdRef.current) {
              retryUrl += `?lastEventId=${encodeURIComponent(lastEventIdRef.current)}`;
            }
            const newEs = new EventSource(retryUrl);
            esRef.current = newEs;
            attachEventHandlers(newEs);
            setupEventSource(newEs);
          }, delay);
        } else {
          setStatus("polling");
          setUseFallback(true);
          startPollingRef.current();
        }
      };
    };

    setupEventSource(es);

    return () => {
      mountedRef.current = false;
      if (heartbeatTimer) clearTimeout(heartbeatTimer);
      if (retryTimeoutRef.current) clearTimeout(retryTimeoutRef.current);
      if (esRef.current) {
        esRef.current.close();
        esRef.current = null;
      }
      cancelPollingRef.current();
    };
  }, [autoStart, resourceType, resourceId]);

  useEffect(() => {
    if (useFallback && mountedRef.current) {
      if (pollStatus === "done") setStatus("done");
      else if (pollStatus === "error" || pollStatus === "timeout") setStatus("error");
    }
  }, [useFallback, pollStatus]);

  return { status, cancel };
}
