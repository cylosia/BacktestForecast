"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError } from "@/lib/api/shared";

export type PollingStatus = "idle" | "polling" | "done" | "timeout" | "error";

export interface UsePollingOptions<T> {
  /** Async function that fetches the latest state. Receives an AbortSignal. */
  fetcher: (signal: AbortSignal) => Promise<T>;
  /** Called once when `isComplete` returns true. */
  onComplete: (result: T) => void;
  /** Return true when the resource has reached a terminal state. */
  isComplete: (result: T) => boolean;
  /** Milliseconds between polls. */
  interval: number;
  /** Maximum number of attempts before giving up. */
  maxAttempts: number;
  /** Optional callback invoked with each intermediate result. */
  onProgress?: (result: T) => void;
  /** If true, polling starts automatically on mount. Default false. */
  autoStart?: boolean;
}

export interface UsePollingReturn {
  status: PollingStatus;
  /** Begin (or restart) the polling loop. */
  start: () => void;
  /** Cancel the current polling loop. */
  cancel: () => void;
  /** Number of polls executed so far. */
  attempts: number;
}

/**
 * Generic polling hook that replaces the four bespoke polling
 * implementations across backtest-run-poller, scanner-job-poller,
 * export-actions, and symbol-analysis-launcher.
 */
export function usePolling<T>({
  fetcher,
  onComplete,
  isComplete,
  interval,
  maxAttempts,
  onProgress,
  autoStart = false,
}: UsePollingOptions<T>): UsePollingReturn {
  const [status, setStatus] = useState<PollingStatus>(autoStart ? "polling" : "idle");
  const [attempts, setAttempts] = useState(0);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const mountedRef = useRef(true);

  const cancel = useCallback(() => {
    if (timerRef.current) {
      clearTimeout(timerRef.current);
      timerRef.current = null;
    }
    abortRef.current?.abort();
    abortRef.current = null;
  }, []);

  const fetcherRef = useRef(fetcher);
  const onCompleteRef = useRef(onComplete);
  const isCompleteRef = useRef(isComplete);
  const onProgressRef = useRef(onProgress);
  useEffect(() => { fetcherRef.current = fetcher; }, [fetcher]);
  useEffect(() => { onCompleteRef.current = onComplete; }, [onComplete]);
  useEffect(() => { isCompleteRef.current = isComplete; }, [isComplete]);
  useEffect(() => { onProgressRef.current = onProgress; }, [onProgress]);

  const attemptsRef = useRef(0);
  const consecutiveErrorsRef = useRef(0);
  const abortRef = useRef<AbortController | null>(null);

  const intervalRef = useRef(interval);
  const maxAttemptsRef = useRef(maxAttempts);
  useEffect(() => { intervalRef.current = interval; }, [interval]);
  useEffect(() => { maxAttemptsRef.current = maxAttempts; }, [maxAttempts]);

  const poll = useCallback(async () => {
    if (!mountedRef.current) return;

    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    try {
      const result = await fetcherRef.current(controller.signal);
      if (!mountedRef.current || controller.signal.aborted) return;

      consecutiveErrorsRef.current = 0;
      onProgressRef.current?.(result);

      if (isCompleteRef.current(result)) {
        try {
          await Promise.resolve(onCompleteRef.current(result));
        } catch (err) {
          console.error("[usePolling] onComplete failed:", err);
        } finally {
          if (mountedRef.current && !controller.signal.aborted) {
            setStatus("done");
          }
          await Promise.resolve(onCompleteRef.current(result));
        } catch (err) {
          console.error("[usePolling] onComplete failed:", err);
        } finally {
          if (mountedRef.current && !controller.signal.aborted) {
            setStatus("done");
          }
        }
        return;
      }

      const next = attemptsRef.current + 1;
      attemptsRef.current = next;
      setAttempts(next);
      if (next >= maxAttemptsRef.current) {
        setStatus("timeout");
      } else {
        timerRef.current = setTimeout(poll, intervalRef.current);
      }
    } catch (err) {
      if (!mountedRef.current || controller.signal.aborted) return;
      if (err instanceof ApiError && (err.status === 401 || err.status === 403)) {
        setStatus("error");
        return;
      }
      consecutiveErrorsRef.current += 1;
      const next = attemptsRef.current + 1;
      attemptsRef.current = next;
      setAttempts(next);
      if (next >= maxAttemptsRef.current) {
        setStatus("error");
      } else {
        const backoff = intervalRef.current * Math.min(2 ** consecutiveErrorsRef.current, 16) * (0.5 + Math.random());
        timerRef.current = setTimeout(poll, backoff);
      }
    }
  }, []);

  const start = useCallback(() => {
    cancel();
    attemptsRef.current = 0;
    consecutiveErrorsRef.current = 0;
    setAttempts(0);
    setStatus("polling");
    timerRef.current = setTimeout(poll, 0);
  }, [cancel, poll]);

  const startedRef = useRef(false);

  useEffect(() => {
    mountedRef.current = true;
    if (autoStart && !startedRef.current) {
      startedRef.current = true;
      start();
    }
    return () => {
      mountedRef.current = false;
      startedRef.current = false;
      cancel();
    };
  }, [autoStart, start, cancel]);

  return { status, start, cancel, attempts };
}
