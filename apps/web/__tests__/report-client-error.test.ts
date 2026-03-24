import { afterEach, describe, expect, it, vi } from "vitest";

import { reportClientError } from "@/lib/observability/report-client-error";

describe("reportClientError", () => {
  const originalConsoleError = console.error;
  const originalDsn = process.env.NEXT_PUBLIC_SENTRY_DSN;

  afterEach(() => {
    console.error = originalConsoleError;
    process.env.NEXT_PUBLIC_SENTRY_DSN = originalDsn;
    vi.unstubAllGlobals();
  });

  it("falls back to console when Sentry is not available", () => {
    const consoleSpy = vi.fn();
    console.error = consoleSpy;
    process.env.NEXT_PUBLIC_SENTRY_DSN = "https://examplePublicKey@example.ingest.sentry.io/1";
    vi.stubGlobal("window", {} as Window);

    reportClientError(new Error("boom"), { boundary: "RootError", digest: "abc123" });

    expect(consoleSpy).toHaveBeenCalledTimes(1);
    expect(consoleSpy.mock.calls[0]?.[0]).toBe("RootError caught");
  });

  it("reports through window.Sentry when configured and loaded", () => {
    const captureException = vi.fn();
    const consoleSpy = vi.fn();
    console.error = consoleSpy;
    process.env.NEXT_PUBLIC_SENTRY_DSN = "https://examplePublicKey@example.ingest.sentry.io/1";
    vi.stubGlobal("window", {
      Sentry: {
        captureException,
      },
    } as Window & { Sentry: { captureException: typeof captureException } });

    reportClientError(new Error("boom"), { boundary: "DashboardError", digest: "digest-1" });

    expect(captureException).toHaveBeenCalledTimes(1);
    expect(consoleSpy).not.toHaveBeenCalled();
  });
});
