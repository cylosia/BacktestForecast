"use client";

import { useEffect, useRef, useState } from "react";
import { useAuth } from "@clerk/nextjs";
import { Loader2 } from "lucide-react";
import { createCheckoutSession } from "@/lib/api/client";
import { ApiError } from "@/lib/api/shared";
import type { BillingInterval, PlanTier } from "@backtestforecast/api-client";
import { Button } from "@/components/ui/button";

export function CheckoutButton({
  tier,
  billingInterval,
  children,
  className,
}: {
  tier: Exclude<PlanTier, "free">;
  billingInterval: BillingInterval;
  children: React.ReactNode;
  className?: string;
}) {
  const { getToken } = useAuth();
  const [status, setStatus] = useState<"idle" | "loading" | "error">("idle");
  const [message, setMessage] = useState<string | null>(null);
  const resetTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (resetTimerRef.current) clearTimeout(resetTimerRef.current);
    };
  }, []);

  async function handleClick() {
    if (resetTimerRef.current) {
      clearTimeout(resetTimerRef.current);
      resetTimerRef.current = null;
    }

    setStatus("loading");
    setMessage(null);

    let redirecting = false;
    try {
      const token = await getToken();
      if (!token) {
        redirecting = true;
        window.location.href = `/sign-in?redirect_url=${encodeURIComponent(window.location.pathname)}`;
        return;
      }
      const result = await createCheckoutSession(token, {
        tier,
        billing_interval: billingInterval,
      });
      if (!result.checkout_url) {
        throw new Error("The checkout URL was not returned. Please try again.");
      }
      const allowed = ["https://checkout.stripe.com", "https://billing.stripe.com"];
      let parsedOrigin: string;
      try {
        parsedOrigin = new URL(result.checkout_url).origin;
      } catch {
        throw new Error("Invalid checkout URL received.");
      }
      if (!allowed.includes(parsedOrigin)) {
        throw new Error("Unexpected checkout URL origin.");
      }
      redirecting = true;
      window.location.href = result.checkout_url;
    } catch (error) {
      const nextMessage =
        error instanceof ApiError
          ? error.message
          : error instanceof Error
            ? error.message
            : "Checkout could not be started.";
      setStatus("error");
      setMessage(nextMessage);
    } finally {
      if (!redirecting) {
        resetTimerRef.current = setTimeout(() => {
          if (document.visibilityState !== "hidden") {
            setStatus((prev) => (prev === "loading" ? "idle" : prev));
          }
        }, 2000);
      }
    }
  }

  return (
    <div className="space-y-2">
      <Button className={className} disabled={status === "loading"} onClick={handleClick} type="button">
        {status === "loading" ? (
          <>
            <Loader2 className="h-4 w-4 animate-spin" />
            Redirecting…
          </>
        ) : (
          children
        )}
      </Button>
      {message ? <p className="text-sm text-destructive">{message}</p> : null}
    </div>
  );
}
