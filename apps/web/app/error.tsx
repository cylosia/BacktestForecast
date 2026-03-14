"use client";

import { useEffect } from "react";

export default function RootError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error("Root error boundary caught:", error);
  }, [error]);

  return (
    <html lang="en" suppressHydrationWarning>
      <body className="min-h-screen bg-background text-foreground antialiased">
        <div className="flex min-h-screen flex-col items-center justify-center gap-4 text-center p-6">
          <h1 className="text-3xl font-bold tracking-tight">Something went wrong</h1>
          <p className="max-w-md text-gray-600">
            An unexpected error occurred. Please try again or contact support if the
            problem persists.
          </p>
          <button
            type="button"
            onClick={reset}
            className="mt-2 rounded-md bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700"
          >
            Try again
          </button>
        </div>
      </body>
    </html>
  );
}
