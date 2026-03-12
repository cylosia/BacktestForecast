"use client";

import { useState } from "react";
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

  async function handleClick() {
    setStatus("loading");
    setMessage(null);

    try {
      const token = await getToken();
      if (!token) {
        window.location.href = "/sign-in";
        return;
      }
      const result = await createCheckoutSession(token, {
        tier,
        billing_interval: billingInterval,
      });
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
