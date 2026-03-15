"use client";

import { FileText } from "lucide-react";
import type { TemplateResponse } from "@backtestforecast/api-client";
import { strategyLabel } from "@/lib/backtests/format";
import type { BacktestFormValues } from "@/lib/backtests/validation";
import { templateToFormValues } from "@/lib/templates/parse";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

export { isValidTemplateConfig, templateToFormValues } from "@/lib/templates/parse";

export function TemplatePicker({
  templates,
  onApply,
}: {
  templates: TemplateResponse[];
  onApply: (patch: Partial<BacktestFormValues>) => void;
}) {
  if (templates.length === 0) {
    return null;
  }

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-base">Apply a template</CardTitle>
        <CardDescription>Pre-fill the form from a saved configuration.</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-wrap gap-2">
          {templates.map((template) => (
            <button
              key={template.id}
              type="button"
              className="inline-flex items-center gap-2 rounded-lg border border-border/70 bg-background px-3 py-2 text-sm font-medium transition-colors hover:bg-accent hover:text-accent-foreground"
              onClick={() => {
                const patch = templateToFormValues(template);
                if (patch) onApply(patch);
              }}
            >
              <FileText className="h-3.5 w-3.5 text-muted-foreground" />
              <span>{template.name}</span>
              <span className="text-xs text-muted-foreground">
                {strategyLabel(template.strategy_type)}
              </span>
            </button>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
