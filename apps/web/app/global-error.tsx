"use client";

import { useEffect } from "react";
import { Inter } from "next/font/google";
import { reportClientError } from "@/lib/observability/report-client-error";

const inter = Inter({ subsets: ["latin"], display: "swap" });

export default function GlobalError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    reportClientError(error, { boundary: "GlobalError", digest: error.digest });
  }, [error]);

  return (
    <html lang="en" suppressHydrationWarning className={inter.className}>
      {/* Hardcoded colors intentional: global-error replaces the entire document
          so globals.css (which defines bg-background/text-foreground) may not load. */}
      <body className="min-h-screen bg-white text-gray-900 dark:bg-gray-950 dark:text-gray-100 antialiased">
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
