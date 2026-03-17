"use client";

import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { updateTemplate } from "@/lib/api/client";
import { Button } from "@/components/ui/button";

export function EditTemplateDialog({
  templateId,
  initialName,
  initialDescription,
  open,
  onClose,
}: {
  templateId: string;
  initialName: string;
  initialDescription: string;
  open: boolean;
  onClose: () => void;
}) {
  const router = useRouter();
  const { getToken } = useAuth();
  const [name, setName] = useState(initialName);
  const [description, setDescription] = useState(initialDescription);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    if (open) {
      setName(initialName);
      setDescription(initialDescription);
      setError(null);
    }
  }, [open, initialName, initialDescription]);

  useEffect(() => {
    return () => {
      abortRef.current?.abort();
    };
  }, []);

  if (!open) return null;

  async function handleSave() {
    if (!name.trim()) {
      setError("Name is required.");
      return;
    }

    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    setSaving(true);
    setError(null);
    try {
      const token = await getToken();
      if (!token) {
        setError("Authentication expired. Please reload.");
        setSaving(false);
        return;
      }
      await updateTemplate(token, templateId, { name: name.trim(), description: description.trim() || undefined }, controller.signal);
      if (controller.signal.aborted) return;
      setSaving(false);
      onClose();
      router.refresh();
    } catch {
      if (controller.signal.aborted) return;
      setError("Failed to update template. Please try again.");
      setSaving(false);
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="fixed inset-0 bg-black/50" onClick={onClose} />
      <div className="relative z-10 w-full max-w-md rounded-xl border border-border bg-background p-6 shadow-lg">
        <h2 className="text-lg font-semibold">Edit template</h2>

        <div className="mt-4 space-y-4">
          <div>
            <label htmlFor="template-name" className="text-sm font-medium">
              Name <span className="text-destructive">*</span>
            </label>
            <input
              id="template-name"
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              placeholder="Template name"
            />
          </div>
          <div>
            <label htmlFor="template-description" className="text-sm font-medium">
              Description
            </label>
            <textarea
              id="template-description"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              rows={3}
              className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring resize-none"
              placeholder="Optional description"
            />
          </div>
        </div>

        {error && <p className="mt-3 text-sm text-destructive">{error}</p>}

        <div className="mt-6 flex justify-end gap-2">
          <Button variant="ghost" size="sm" onClick={onClose} disabled={saving}>
            Cancel
          </Button>
          <Button size="sm" onClick={handleSave} disabled={saving}>
            {saving ? "Saving…" : "Save"}
          </Button>
        </div>
      </div>
    </div>
  );
}
