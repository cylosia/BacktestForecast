"use client";

import { useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";
import { CheckCircle } from "lucide-react";

export function CheckoutSuccessBanner() {
  const searchParams = useSearchParams();
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    if (searchParams.get("checkout") === "success") {
      setVisible(true);
      // Clean the URL param without a full page reload
      try {
        const url = new URL(window.location.href);
        url.searchParams.delete("checkout");
        window.history.replaceState({}, "", url.toString());
      } catch {
        // Ignore URL parsing errors; banner still shows
      }
    }
  }, [searchParams]);

  if (!visible) return null;

  return (
    <div className="rounded-xl border border-emerald-500/30 bg-emerald-500/5 p-4 text-sm">
      <div className="flex items-start gap-3">
        <CheckCircle className="mt-0.5 h-4 w-4 text-emerald-600" />
        <div>
          <p className="font-medium text-emerald-700 dark:text-emerald-400">
            Subscription activated
          </p>
          <p className="mt-1 text-emerald-700/80 dark:text-emerald-400/80">
            Your plan has been updated. It may take a moment for Stripe to process the webhook.
            Refresh the page if your plan tier has not updated yet.
          </p>
        </div>
        <button
          aria-label="Dismiss success banner"
          className="ml-auto text-xs text-muted-foreground hover:text-foreground"
          onClick={() => setVisible(false)}
          type="button"
        >
          Dismiss
        </button>
      </div>
    </div>
  );
}
