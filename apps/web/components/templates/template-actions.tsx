"use client";

import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { Play, Trash2 } from "lucide-react";
import Link from "next/link";
import { deleteTemplate } from "@/lib/api/client";
import { Button } from "@/components/ui/button";

export function TemplateActions({
  templateId,
  templateName,
}: {
  templateId: string;
  templateName: string;
}) {
  const router = useRouter();
  const { getToken } = useAuth();
  const [deleting, setDeleting] = useState(false);
  const [confirming, setConfirming] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    return () => { abortRef.current?.abort(); };
  }, []);

  async function handleDelete() {
    if (!confirming) {
      setConfirming(true);
      return;
    }

    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    setDeleting(true);
    setError(null);
    setConfirming(false);
    try {
      const token = await getToken();
      if (!token) {
        setError("Authentication expired. Please reload.");
        setDeleting(false);
        return;
      }
      await deleteTemplate(token, templateId, controller.signal);
      if (controller.signal.aborted) return;
      setDeleting(false);
      router.refresh();
    } catch {
      if (controller.signal.aborted) return;
      setError("Failed to delete template. Please try again.");
      setDeleting(false);
    }
  }

  return (
    <div className="flex items-center gap-2">
      <Button asChild size="sm" variant="outline">
        <Link href={`/app/backtests/new?template=${templateId}`}>
          <Play className="h-3.5 w-3.5" />
          Apply
        </Link>
      </Button>
      {confirming ? (
        <>
          <span className="text-xs text-destructive">Delete &quot;{templateName}&quot;?</span>
          <Button
            disabled={deleting}
            size="sm"
            variant="destructive"
            onClick={handleDelete}
          >
            Confirm
          </Button>
          <Button
            size="sm"
            variant="ghost"
            onClick={() => setConfirming(false)}
          >
            Cancel
          </Button>
        </>
      ) : (
        <Button
          aria-label="Delete template"
          disabled={deleting}
          size="sm"
          variant="ghost"
          onClick={handleDelete}
        >
          <Trash2 className="h-3.5 w-3.5 text-destructive" />
        </Button>
      )}
      {error && <p className="text-sm text-destructive">{error}</p>}
    </div>
  );
}
