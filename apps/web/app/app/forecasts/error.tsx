"use client";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

export default function ForecastsError({ error, reset }: { error: Error; reset: () => void }) {
  const displayMessage =
    error != null && typeof error === "object" && "status" in error && typeof (error as any).message === "string" && (error as any).message.length < 200
      ? error.message
      : "An unexpected error occurred. Please try again or contact support.";

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Something went wrong</CardTitle>
          <CardDescription>The forecasts page encountered an unexpected error.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-sm text-muted-foreground">{displayMessage}</p>
          <Button onClick={reset}>Try again</Button>
        </CardContent>
      </Card>
    </div>
  );
}
