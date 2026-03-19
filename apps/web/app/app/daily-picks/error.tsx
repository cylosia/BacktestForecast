"use client";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

function getErrorMessage(error: unknown): string | undefined {
  if (error != null && typeof error === "object" && "message" in error && typeof (error as Record<string, unknown>).message === "string") {
    const msg = (error as Record<string, unknown>).message as string;
    return msg.length < 200 ? msg : undefined;
  }
  return undefined;
}

export default function DailyPicksError({ error, reset }: { error: Error; reset: () => void }) {
  const displayMessage = getErrorMessage(error) ?? "An unexpected error occurred. Please try again or contact support.";

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Something went wrong</CardTitle>
          <CardDescription>The daily picks page encountered an unexpected error.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-sm text-muted-foreground">{displayMessage}</p>
          <Button onClick={reset}>Try again</Button>
        </CardContent>
      </Card>
    </div>
  );
}
