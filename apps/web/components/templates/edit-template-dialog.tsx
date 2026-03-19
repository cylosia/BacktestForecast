"use client";

import { useCallback, useEffect, useRef, useState } from "react";
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
  const dialogRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<Element | null>(null);
  const savingRef = useRef(false);

  useEffect(() => {
    if (open) {
      triggerRef.current = document.activeElement;
      setName(initialName);
      setDescription(initialDescription);
      setError(null);
    } else if (triggerRef.current instanceof HTMLElement) {
      triggerRef.current.focus();
      triggerRef.current = null;
    }
  }, [open, initialName, initialDescription]);

  useEffect(() => {
    if (!open) return;
    const firstInput = dialogRef.current?.querySelector<HTMLElement>(
      "input, textarea, button, [tabindex]",
    );
    firstInput?.focus();

    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => { document.body.style.overflow = prev; };
  }, [open]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Escape") {
        onClose();
        return;
      }
      if (e.key !== "Tab" || !dialogRef.current) return;
      const focusable = dialogRef.current.querySelectorAll<HTMLElement>(
        'a[href], input, textarea, button, [tabindex]:not([tabindex="-1"])',
      );
      if (focusable.length === 0) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (e.shiftKey && document.activeElement === first) {
        e.preventDefault();
        last.focus();
      } else if (!e.shiftKey && document.activeElement === last) {
        e.preventDefault();
        first.focus();
      }
    },
    [onClose],
  );

  useEffect(() => {
    return () => {
      abortRef.current?.abort();
    };
  }, []);

  const handleSave = useCallback(async () => {
    if (!name.trim()) {
      setError("Name is required.");
      return;
    }
    if (savingRef.current || saving) return;
    savingRef.current = true;

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
      savingRef.current = false;
      setSaving(false);
      onClose();
      router.refresh();
    } catch (err) {
      if (controller.signal.aborted) return;
      console.error("[EditTemplateDialog] update failed:", err);
      const message = err instanceof Error ? err.message : "Failed to update template. Please try again.";
      setError(message);
      savingRef.current = false;
      setSaving(false);
    }
  }, [name, description, saving, templateId, getToken, onClose, router]);

  useEffect(() => {
    if (open) {
      document.body.style.overflow = "hidden";
    } else {
      document.body.style.overflow = "";
    }
    return () => { document.body.style.overflow = ""; };
  }, [open]);

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center" role="dialog" aria-modal="true" aria-labelledby="edit-template-title" onKeyDown={handleKeyDown}>
      <div className="fixed inset-0 bg-black/50" onClick={onClose} onMouseDown={(e) => e.stopPropagation()} />
      <div ref={dialogRef} className="relative z-10 w-full max-w-md rounded-xl border border-border bg-background p-6 shadow-lg">
        <h2 id="edit-template-title" className="text-lg font-semibold">Edit template</h2>

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
              maxLength={120}
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
              maxLength={500}
              className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring resize-none"
              placeholder="Optional description"
            />
          </div>
        </div>

        {error && <p role="alert" aria-live="polite" className="mt-3 text-sm text-destructive">{error}</p>}

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
