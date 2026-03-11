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

    try {
      const token = await getToken();
      if (!token) {
        throw new Error("Your session token could not be loaded. Please sign in again.");
      }
      const result = await createPortalSession(token, {
        return_path: "/app/settings/billing",
      });
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
