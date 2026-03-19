"use client";

import { useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

export default function DashboardError({ error, reset }: { error: Error & { digest?: string }; reset: () => void }) {
  // TODO: Integrate Sentry error reporting here. Call Sentry.captureException(error)
  // with additional context (digest, component: "DashboardError"). Gate behind
  // a NEXT_PUBLIC_SENTRY_DSN env check so local dev still uses console.error.
  // See also: apps/web/app/global-error.tsx for the root-level error boundary.
  useEffect(() => { if (process.env.NODE_ENV === "development") console.error(error); }, [error]);

  const rawMessage = error instanceof Error
    ? error.message
    : (typeof error === "object" && error !== null && "message" in error)
      ? String((error as { message: unknown }).message)
      : null;
  const displayMessage =
    rawMessage && rawMessage.length < 200
      ? rawMessage
      : "An unexpected error occurred. Please try again or contact support.";

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Something went wrong</CardTitle>
          <CardDescription>The dashboard page encountered an unexpected error.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <p role="alert" className="text-sm text-muted-foreground">{displayMessage}</p>
          <Button onClick={reset}>Try again</Button>
        </CardContent>
      </Card>
    </div>
  );
}
