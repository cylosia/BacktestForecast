"use client";

import { useState } from "react";
import { useAuth } from "@clerk/nextjs";
import { Loader2 } from "lucide-react";
import { createPortalSession } from "@/lib/api/client";
import { ApiError } from "@/lib/api/shared";
import { Button } from "@/components/ui/button";

export function PortalButton({ children }: { children: React.ReactNode }) {
  const { getToken } = useAuth();
  const [status, setStatus] = useState<"idle" | "loading" | "error">("idle");
  const [message, setMessage] = useState<string | null>(null);

  async function handleClick() {
    setStatus("loading");
    setMessage(null);

    let redirecting = false;
    try {
      const token = await getToken();
      if (!token) {
        throw new Error("Your session token could not be loaded. Please sign in again.");
      }
      const result = await createPortalSession(token, {
        return_path: "/app/settings/billing",
      });
      if (!result.portal_url) {
        throw new Error("The billing portal URL was not returned. Please try again.");
      }
      const allowed = ["https://billing.stripe.com", "https://checkout.stripe.com"];
      let parsedOrigin: string;
      try {
        parsedOrigin = new URL(result.portal_url).origin;
      } catch {
        throw new Error("Invalid portal URL received.");
      }
      if (!allowed.includes(parsedOrigin)) {
        throw new Error("Unexpected portal URL origin.");
      }
      redirecting = true;
      window.location.href = result.portal_url;
    } catch (error) {
      const nextMessage =
        error instanceof ApiError
          ? error.message
          : error instanceof Error
            ? error.message
            : "Billing portal could not be opened.";
      setStatus("error");
      setMessage(nextMessage);
    } finally {
      if (!redirecting) {
        setStatus((prev) => (prev === "loading" ? "idle" : prev));
      }
    }
  }

  return (
    <div className="space-y-2">
      <Button disabled={status === "loading"} onClick={handleClick} type="button" variant="outline">
        {status === "loading" ? (
          <>
            <Loader2 className="h-4 w-4 animate-spin" />
            Opening portal…
          </>
        ) : (
          children
        )}
      </Button>
      {message ? <p className="text-sm text-destructive">{message}</p> : null}
    </div>
  );
}
