"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { Bookmark, Loader2 } from "lucide-react";
import { createTemplate } from "@/lib/api/client";
import { ApiError } from "@/lib/api/shared";
import type { BacktestFormValues } from "@/lib/backtests/validation";
import type { CreateTemplateRequest, EntryRule, StrategyType } from "@backtestforecast/api-client";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

function formValuesToTemplateConfig(
  values: BacktestFormValues,
): CreateTemplateRequest["config"] {
  const entryRules: EntryRule[] = [];

  if (values.rsiEnabled) {
    entryRules.push({
      type: "rsi",
      operator: values.rsiOperator,
      threshold: Number(values.rsiThreshold),
      period: Number(values.rsiPeriod),
    });
  }

  if (values.movingAverageEnabled) {
    entryRules.push({
      type: values.movingAverageType,
      fast_period: Number(values.fastPeriod),
      slow_period: Number(values.slowPeriod),
      direction: values.crossoverDirection,
    } as EntryRule);
  }

  return {
    strategy_type: values.strategyType as StrategyType,
    target_dte: Number(values.targetDte),
    dte_tolerance_days: Number(values.dteToleranceDays),
    max_holding_days: Number(values.maxHoldingDays),
    account_size: Number(values.accountSize),
    risk_per_trade_pct: Number(values.riskPerTradePct),
    commission_per_contract: Number(values.commissionPerContract),
    entry_rules: entryRules,
    default_symbol: values.symbol || null,
  };
}

export function SaveAsTemplate({ values }: { values: BacktestFormValues }) {
  const router = useRouter();
  const { getToken } = useAuth();
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState<string | null>(null);

  async function handleSave() {
    if (!name.trim()) {
      setMessage("Template name is required.");
      return;
    }

    setSaving(true);
    setMessage(null);

    try {
      const token = await getToken();
      if (!token) {
        setMessage("Session expired. Please sign in again.");
        setSaving(false);
        return;
      }

      await createTemplate(token, {
        name: name.trim(),
        description: description.trim() || null,
        config: formValuesToTemplateConfig(values),
      });

      setMessage("Template saved.");
      setSaving(false);
      setOpen(false);
      setName("");
      setDescription("");
      router.refresh();
    } catch (error) {
      const msg =
        error instanceof ApiError ? error.message : "Could not save template.";
      setMessage(msg);
      setSaving(false);
    }
  }

  if (!open) {
    return (
      <Button type="button" variant="outline" size="sm" onClick={() => setOpen(true)}>
        <Bookmark className="h-4 w-4" />
        Save as template
      </Button>
    );
  }

  return (
    <div className="rounded-xl border border-border/70 bg-muted/40 p-4 space-y-3">
      <p className="text-sm font-medium">Save current inputs as a template</p>

      <div className="space-y-2">
        <Label htmlFor="templateName">Template name</Label>
        <Input
          id="templateName"
          maxLength={120}
          placeholder="e.g. Conservative SPY Long Call"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />
      </div>

      <div className="space-y-2">
        <Label htmlFor="templateDesc">Description (optional)</Label>
        <Input
          id="templateDesc"
          maxLength={500}
          placeholder="Brief note about this configuration"
          value={description}
          onChange={(e) => setDescription(e.target.value)}
        />
      </div>

      {message ? (
        <p className="text-sm text-muted-foreground">{message}</p>
      ) : null}

      <div className="flex items-center gap-2">
        <Button type="button" size="sm" disabled={saving} onClick={handleSave}>
          {saving ? (
            <>
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
              Saving...
            </>
          ) : (
            "Save template"
          )}
        </Button>
        <Button
          type="button"
          size="sm"
          variant="ghost"
          onClick={() => {
            setOpen(false);
            setMessage(null);
          }}
        >
          Cancel
        </Button>
      </div>
    </div>
  );
}
