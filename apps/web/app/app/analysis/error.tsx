"use client";

import { useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

export default function AnalysisError({ error, reset }: { error: Error & { digest?: string }; reset: () => void }) {
  useEffect(() => { console.error(error); }, [error]);

  const rawMessage = error instanceof Error
    ? error.message
    : (typeof error === "object" && error !== null && "message" in error)
      ? String((error as { message: unknown }).message)
      : null;
  const GENERIC = "An unexpected error occurred. Please try again or contact support.";
  const looksInternal = rawMessage
    ? /\bat\s+\S|Error:|\/[a-z_]/i.test(rawMessage) ||
      /SELECT\s|INSERT\s|UPDATE\s|DELETE\s|FROM\s|WHERE\s/i.test(rawMessage) ||
      /https?:\/\/(?:localhost|127\.0\.0\.1|10\.|172\.(1[6-9]|2\d|3[01])\.|192\.168\.)/.test(rawMessage)
    : false;
  const displayMessage =
    rawMessage && rawMessage.length < 200 && !looksInternal
      ? rawMessage
      : GENERIC;

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Something went wrong</CardTitle>
          <CardDescription>The analysis page encountered an unexpected error.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <p role="alert" className="text-sm text-muted-foreground">{displayMessage}</p>
          <Button onClick={reset}>Try again</Button>
        </CardContent>
      </Card>
    </div>
  );
}
