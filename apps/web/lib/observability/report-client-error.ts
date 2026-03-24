"use client";

type ReportClientErrorContext = {
  boundary: string;
  digest?: string;
};

type SentryLike = {
  captureException: (
    error: unknown,
    context?: {
      tags?: Record<string, string>;
      extra?: Record<string, string>;
    },
  ) => void;
};

declare global {
  interface Window {
    Sentry?: SentryLike;
  }
}

function getSentry(): SentryLike | undefined {
  if (typeof window === "undefined") {
    return undefined;
  }
  return window.Sentry;
}

export function reportClientError(error: Error, context: ReportClientErrorContext): void {
  const sentryDsn = process.env.NEXT_PUBLIC_SENTRY_DSN;
  const sentry = getSentry();
  const label = `${context.boundary} caught`;

  if (sentryDsn && sentry) {
    sentry.captureException(error, {
      tags: {
        boundary: context.boundary,
      },
      extra: context.digest ? { digest: context.digest } : undefined,
    });
    return;
  }

  console.error(label, {
    error,
    digest: context.digest,
    sentryConfigured: Boolean(sentryDsn),
    sentryLoaded: Boolean(sentry),
  });
}
