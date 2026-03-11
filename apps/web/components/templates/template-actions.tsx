"use client";

import { useState } from "react";
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

  async function handleDelete() {
    if (!confirm(`Delete template "${templateName}"? This cannot be undone.`)) {
      return;
    }

    setDeleting(true);
    try {
      const token = await getToken();
      if (!token) return;
      await deleteTemplate(token, templateId);
      router.refresh();
    } catch {
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
      <Button
        disabled={deleting}
        size="sm"
        variant="ghost"
        onClick={handleDelete}
      >
        <Trash2 className="h-3.5 w-3.5 text-destructive" />
      </Button>
    </div>
  );
}
