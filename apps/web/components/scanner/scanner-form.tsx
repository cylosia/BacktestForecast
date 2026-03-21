"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { Loader2 } from "lucide-react";
import type { StrategyCatalogGroup } from "@backtestforecast/api-client";
import { createScannerJob } from "@/lib/api/client";
import { ApiError } from "@/lib/api/shared";
import { getOrCreatePendingIdempotencyKey } from "@/lib/idempotency";
import type { CreateScannerJobRequest, ScannerMode, StrategyType } from "@backtestforecast/api-client";
import { isPlanLimitError, UpgradePrompt } from "@/components/billing/upgrade-prompt";
import { getScannerLimits, parseSymbols, validateScannerForm, type PlanTier } from "@/lib/scanner/validation";
import { getScannerWindowHelpText } from "@/lib/scanner/constants";
import { daysAgoET } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";

const BASIC_STRATEGIES = [
  { value: "long_call", label: "Long Call" },
  { value: "long_put", label: "Long Put" },
  { value: "covered_call", label: "Covered Call" },
  { value: "cash_secured_put", label: "Cash-Secured Put" },
  { value: "bull_call_debit_spread", label: "Bull Call Spread" },
  { value: "bear_put_debit_spread", label: "Bear Put Spread" },
] as const;

const ADVANCED_STRATEGY_GROUPS: Array<{
  category: string;
  strategies: ReadonlyArray<{ value: string; label: string }>;
}> = [
  { category: "Basic", strategies: BASIC_STRATEGIES },
  {
    category: "Credit Spreads",
    strategies: [
      { value: "bull_put_credit_spread", label: "Bull Put Credit Spread" },
      { value: "bear_call_credit_spread", label: "Bear Call Credit Spread" },
    ],
  },
  {
    category: "Multi-Leg",
    strategies: [
      { value: "iron_condor", label: "Iron Condor" },
      { value: "iron_butterfly", label: "Iron Butterfly" },
      { value: "jade_lizard", label: "Jade Lizard" },
    ],
  },
  {
    category: "Volatility",
    strategies: [
      { value: "long_straddle", label: "Long Straddle" },
      { value: "long_strangle", label: "Long Strangle" },
      { value: "short_straddle", label: "Short Straddle" },
      { value: "short_strangle", label: "Short Strangle" },
    ],
  },
  {
    category: "Diagonal/Calendar",
    strategies: [
      { value: "poor_mans_covered_call", label: "Poor Man's Covered Call" },
      { value: "diagonal_spread", label: "Diagonal Spread" },
      { value: "double_diagonal", label: "Double Diagonal" },
      { value: "calendar_spread", label: "Calendar Spread" },
    ],
  },
  {
    category: "Stock+Options",
    strategies: [
      { value: "collar", label: "Collar" },
      { value: "covered_strangle", label: "Covered Strangle" },
    ],
  },
  {
    category: "Ratio",
    strategies: [
      { value: "ratio_call_backspread", label: "Ratio Call Backspread" },
      { value: "ratio_put_backspread", label: "Ratio Put Backspread" },
    ],
  },
  {
    category: "Naked",
    strategies: [
      { value: "naked_call", label: "Naked Call" },
      { value: "naked_put", label: "Naked Put" },
    ],
  },
  {
    category: "Butterfly",
    strategies: [{ value: "butterfly", label: "Butterfly" }],
  },
  {
    category: "Wheel",
    strategies: [{ value: "wheel_strategy", label: "Wheel Strategy" }],
  },
  {
    category: "Synthetic/Conversion",
    strategies: [
      { value: "synthetic_put", label: "Synthetic Put" },
      { value: "reverse_conversion", label: "Reverse Conversion" },
    ],
  },
  {
    category: "Custom N-Leg",
    strategies: [
      { value: "custom_2_leg", label: "Custom 2-Leg" },
      { value: "custom_3_leg", label: "Custom 3-Leg" },
      { value: "custom_4_leg", label: "Custom 4-Leg" },
      { value: "custom_5_leg", label: "Custom 5-Leg" },
      { value: "custom_6_leg", label: "Custom 6-Leg" },
      { value: "custom_7_leg", label: "Custom 7-Leg" },
      { value: "custom_8_leg", label: "Custom 8-Leg" },
    ],
  },
];

const ADVANCED_STRATEGIES = ADVANCED_STRATEGY_GROUPS.flatMap((g) => g.strategies);

function buildFallbackStrategyGroups(mode: ScannerMode): StrategyCatalogGroup[] {
  if (mode === "advanced") {
    return ADVANCED_STRATEGY_GROUPS.map((group) => ({
      category: group.category.toLowerCase().replace(/[^a-z0-9]+/g, "_"),
      category_label: group.category,
      strategies: group.strategies.map((strategy) => ({
        strategy_type: strategy.value as StrategyType,
        label: strategy.label,
        short_description: "",
        category: "all",
        bias: "neutral",
        leg_count: 1,
        min_tier: "free",
        max_loss_description: "",
        notes: "",
        tags: [],
      })),
    }));
  }

  return [{
    category: "basic",
    category_label: "Basic",
    strategies: BASIC_STRATEGIES.map((strategy) => ({
      strategy_type: strategy.value as StrategyType,
      label: strategy.label,
      short_description: "",
      category: "all",
      bias: "neutral",
      leg_count: 1,
      min_tier: "free",
      max_loss_description: "",
      notes: "",
      tags: [],
    })),
  }];
}

export function ScannerForm({
  scannerModes,
  planTier = "free",
  catalogGroups = [],
  basicAllowedStrategyTypes = [],
  advancedAllowedStrategyTypes = [],
  maxScannerWindowDays,
}: {
  scannerModes: Array<"basic" | "advanced">;
  planTier?: PlanTier;
  catalogGroups?: StrategyCatalogGroup[];
  basicAllowedStrategyTypes?: string[];
  advancedAllowedStrategyTypes?: string[];
  maxScannerWindowDays?: number;
}) {
  const router = useRouter();
  const { getToken } = useAuth();
  const submitAbortRef = useRef<AbortController | null>(null);
  const submittingRef = useRef(false);
  const pendingIdempotencyKeyRef = useRef<string | null>(null);

  useEffect(() => {
    return () => { submitAbortRef.current?.abort(); };
  }, []);

  const defaultMode: ScannerMode = scannerModes.includes("advanced") ? "advanced" : "basic";

  interface FormFields {
    mode: ScannerMode;
    name: string;
    symbolsText: string;
    ruleSetName: string;
    rsiEnabled: boolean;
    rsiOperator: string;
    rsiThreshold: string;
    rsiPeriod: string;
    startDate: string;
    endDate: string;
    targetDte: string;
    dteTolerance: string;
    maxHolding: string;
    accountSize: string;
    riskPct: string;
    commission: string;
    maxRecs: string;
  }

  const [form, setForm] = useState<FormFields>({
    mode: defaultMode,
    name: "",
    symbolsText: "SPY, QQQ, AAPL",
    ruleSetName: "RSI oversold",
    rsiEnabled: true,
    rsiOperator: "lt",
    rsiThreshold: "35",
    rsiPeriod: "14",
    startDate: daysAgoET(365),
    endDate: daysAgoET(0),
    targetDte: "30",
    dteTolerance: "5",
    maxHolding: "10",
    accountSize: "10000",
    riskPct: "2",
    commission: "0.65",
    maxRecs: "10",
  });

  function setField<K extends keyof FormFields>(key: K, value: FormFields[K]) {
    setForm((prev) => ({ ...prev, [key]: value }));
  }

  const [selectedStrategies, setSelectedStrategies] = useState<Set<string>>(
    new Set(["long_call", "long_put"]),
  );

  const allowedStrategySet = new Set(
    (
      form.mode === "advanced"
        ? (advancedAllowedStrategyTypes.length > 0
          ? advancedAllowedStrategyTypes
          : ADVANCED_STRATEGIES.map((strategy) => strategy.value))
        : (basicAllowedStrategyTypes.length > 0
          ? basicAllowedStrategyTypes
          : BASIC_STRATEGIES.map((strategy) => strategy.value))
    ),
  );
  const strategyGroups = (catalogGroups.length > 0 ? catalogGroups : buildFallbackStrategyGroups(form.mode))
    .map((group) => ({
      ...group,
      strategies: group.strategies.filter((strategy) => allowedStrategySet.has(strategy.strategy_type)),
    }))
    .filter((group) => group.strategies.length > 0);

  const [status, setStatus] = useState<"idle" | "submitting" | "success" | "error">("idle");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [errorCode, setErrorCode] = useState<string | undefined>();
  const [requiredTier, setRequiredTier] = useState<string | undefined>();

  useEffect(() => {
    setSelectedStrategies((prev) => {
      const pruned = new Set([...prev].filter((v) => allowedStrategySet.has(v)));
      return pruned.size === prev.size ? prev : pruned;
    });
  }, [allowedStrategySet]);

  const toggleStrategy = useCallback((value: string) => {
    setSelectedStrategies((prev) => {
      const next = new Set(prev);
      if (next.has(value)) {
        next.delete(value);
      } else {
        next.add(value);
      }
      return next;
    });
  }, []);

  const handleSubmit = useCallback(async (e: React.FormEvent) => {
    e.preventDefault();
    if (submittingRef.current) return;

    const effectiveStrategies = new Set([...selectedStrategies].filter((v) => allowedStrategySet.has(v)));

    const symbols = parseSymbols(form.symbolsText);

    const errors = validateScannerForm({
      mode: form.mode,
      symbolsText: form.symbolsText,
      selectedStrategies: effectiveStrategies,
      maxScannerWindowDays,
      startDate: form.startDate,
      endDate: form.endDate,
      targetDte: form.targetDte,
      dteTolerance: form.dteTolerance,
      maxHolding: form.maxHolding,
      accountSize: form.accountSize,
      riskPct: form.riskPct,
      commission: form.commission,
      maxRecs: form.maxRecs,
    }, planTier);

    if (errors.length > 0) {
      setStatus("error");
      setErrorMessage(errors.join(" • "));
      return;
    }

    const entryRules: CreateScannerJobRequest["rule_sets"][0]["entry_rules"] = [];
    if (form.rsiEnabled) {
      const rsiThresholdNum = Number(form.rsiThreshold);
      const rsiPeriodNum = Number(form.rsiPeriod);
      if (!Number.isFinite(rsiThresholdNum) || rsiThresholdNum < 0 || rsiThresholdNum > 100) {
        setStatus("error");
        setErrorMessage("RSI threshold must be a number between 0 and 100.");
        return;
      }
      if (!Number.isFinite(rsiPeriodNum) || rsiPeriodNum < 2 || rsiPeriodNum > 100) {
        setStatus("error");
        setErrorMessage("RSI period must be a number between 2 and 100.");
        return;
      }
      entryRules.push({
        type: "rsi" as const,
        operator: form.rsiOperator as "lt" | "lte" | "gt" | "gte",
        threshold: rsiThresholdNum,
        period: rsiPeriodNum,
      });
    }

    if (entryRules.length === 0) {
      setStatus("error");
      setErrorMessage("At least one entry rule is required in the rule set.");
      return;
    }

    const idempotencyKey = getOrCreatePendingIdempotencyKey(pendingIdempotencyKeyRef.current, "scan");
    const payload: CreateScannerJobRequest = {
      name: form.name.trim() || null,
      mode: form.mode,
      symbols,
      strategy_types: Array.from(effectiveStrategies) as StrategyType[],
      rule_sets: [{ name: form.ruleSetName.trim() || "Default", entry_rules: entryRules }],
      start_date: form.startDate,
      end_date: form.endDate,
      target_dte: Number(form.targetDte),
      dte_tolerance_days: Number(form.dteTolerance),
      max_holding_days: Number(form.maxHolding),
      account_size: Number(form.accountSize),
      risk_per_trade_pct: Number(form.riskPct),
      commission_per_contract: Number(form.commission),
      max_recommendations: Number(form.maxRecs),
      refresh_daily: false,
      refresh_priority: 0,
      idempotency_key: idempotencyKey,
    };
    pendingIdempotencyKeyRef.current = idempotencyKey;

    setStatus("submitting");
    setErrorMessage(null);
    setErrorCode(undefined);
    submittingRef.current = true;

    try {
      const token = await getToken();
      if (!token) throw new Error("Session expired.");

      submitAbortRef.current?.abort();
      submitAbortRef.current = new AbortController();
      const job = await createScannerJob(token, payload, submitAbortRef.current.signal);
      setStatus("success");
      pendingIdempotencyKeyRef.current = null;
      router.replace(`/app/scanner/${job.id}`);
      router.refresh();
    } catch (error) {
      const msg = error instanceof ApiError ? error.message : error instanceof Error ? error.message : "Scan could not be created.";
      const code = error instanceof ApiError ? error.code : undefined;
      const reqTier = error instanceof ApiError ? error.requiredTier : undefined;
      setStatus("error");
      setErrorMessage(msg);
      setErrorCode(code);
      setRequiredTier(reqTier);
    } finally {
      submittingRef.current = false;
    }
  }, [allowedStrategySet, form, getToken, maxScannerWindowDays, planTier, router, selectedStrategies]);

  return (
    <form className="space-y-6" noValidate onSubmit={handleSubmit} aria-label="Scanner configuration">
      {errorMessage && isPlanLimitError(errorCode) ? (
        <UpgradePrompt message={errorMessage} requiredTier={requiredTier} />
      ) : errorMessage ? (
        <div role="alert" className="rounded-xl border border-destructive/40 bg-destructive/5 p-4 text-sm text-destructive whitespace-pre-line">
          {errorMessage}
        </div>
      ) : null}

      <Card>
        <CardHeader>
          <CardTitle>Scan setup</CardTitle>
          <CardDescription>Define the symbols, strategies, and timeframe for this scanner job.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="grid gap-4 lg:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="scanName">Scan name (optional)</Label>
              <Input id="scanName" maxLength={120} placeholder="e.g. Weekly momentum scan" value={form.name} onChange={(e) => setField("name", e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="scanMode">Scanner mode</Label>
              <Select
                id="scanMode"
                value={form.mode}
                options={scannerModes.map((m) => ({ value: m, label: m === "basic" ? "Basic" : "Advanced" }))}
                onChange={(e) => setField("mode", e.target.value as ScannerMode)}
              />
            </div>
          </div>

          <div className="space-y-2">
            <Label htmlFor="symbols">Symbols (comma-separated)</Label>
            <Input id="symbols" maxLength={500} placeholder="SPY, QQQ, AAPL, MSFT" value={form.symbolsText} onChange={(e) => setField("symbolsText", e.target.value)} />
          </div>

          <div className="space-y-2">
            <Label>Strategy types</Label>
            {form.mode === "advanced" ? (
              <div className="space-y-4">
                {strategyGroups.map((group) => (
                  <div key={group.category}>
                    <p className="mb-2 text-xs font-medium uppercase tracking-wider text-muted-foreground">
                      {group.category_label}
                    </p>
                    <div className="flex flex-wrap gap-2">
                      {group.strategies.map((strategy) => (
                        <label
                          key={strategy.strategy_type}
                          className="inline-flex items-center gap-2 rounded-lg border border-border/70 px-3 py-2 text-sm cursor-pointer hover:bg-accent"
                        >
                          <input
                            type="checkbox"
                            className="h-4 w-4 rounded border-input"
                            checked={selectedStrategies.has(strategy.strategy_type)}
                            onChange={() => toggleStrategy(strategy.strategy_type)}
                          />
                          {strategy.label}
                        </label>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="flex flex-wrap gap-2">
                {strategyGroups.flatMap((group) => group.strategies).map((strategy) => (
                  <label
                    key={strategy.strategy_type}
                    className="inline-flex items-center gap-2 rounded-lg border border-border/70 px-3 py-2 text-sm cursor-pointer hover:bg-accent"
                  >
                    <input
                      type="checkbox"
                      className="h-4 w-4 rounded border-input"
                      checked={selectedStrategies.has(strategy.strategy_type)}
                      onChange={() => toggleStrategy(strategy.strategy_type)}
                    />
                    {strategy.label}
                  </label>
                ))}
              </div>
            )}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Rule set</CardTitle>
          <CardDescription>Define at least one named rule set with entry conditions.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="ruleSetName">Rule set name</Label>
            <Input id="ruleSetName" maxLength={120} placeholder="RSI oversold" value={form.ruleSetName} onChange={(e) => setField("ruleSetName", e.target.value)} />
          </div>

          <div className="rounded-xl border border-border/70 p-4">
            <div className="flex items-start justify-between">
              <div>
                <p className="font-medium">RSI rule</p>
                <p className="text-sm text-muted-foreground">Trigger entries when RSI crosses a threshold.</p>
              </div>
              <label className="inline-flex items-center gap-2 text-sm font-medium">
                <input type="checkbox" className="h-4 w-4 rounded border-input" checked={form.rsiEnabled} onChange={(e) => setField("rsiEnabled", e.target.checked)} />
                Enabled
              </label>
            </div>
            {form.rsiEnabled ? (
              <div className="mt-4 grid gap-4 sm:grid-cols-3">
                <div className="space-y-2">
                  <Label htmlFor="rsiOp">Operator</Label>
                  <Select id="rsiOp" value={form.rsiOperator} options={[
                    { value: "lt", label: "Less than" },
                    { value: "lte", label: "≤" },
                    { value: "gt", label: "Greater than" },
                    { value: "gte", label: "≥" },
                  ]} onChange={(e) => setField("rsiOperator", e.target.value)} />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="rsiThresh">Threshold</Label>
                  <Input id="rsiThresh" inputMode="decimal" value={form.rsiThreshold} onChange={(e) => setField("rsiThreshold", e.target.value)} />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="rsiPer">Period</Label>
                  <Input id="rsiPer" inputMode="numeric" value={form.rsiPeriod} onChange={(e) => setField("rsiPeriod", e.target.value)} />
                </div>
              </div>
            ) : null}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Timeframe and risk</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <p className="text-sm text-muted-foreground">{getScannerWindowHelpText(maxScannerWindowDays)}</p>
          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="scanStart">Start date</Label>
              <Input id="scanStart" type="date" value={form.startDate} onChange={(e) => setField("startDate", e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="scanEnd">End date</Label>
              <Input id="scanEnd" type="date" value={form.endDate} onChange={(e) => setField("endDate", e.target.value)} />
            </div>
          </div>
          <div className="grid gap-4 sm:grid-cols-3 lg:grid-cols-6">
            <div className="space-y-2">
              <Label htmlFor="scanDte">Target DTE</Label>
              <Input id="scanDte" inputMode="numeric" value={form.targetDte} onChange={(e) => setField("targetDte", e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="scanDteTol">DTE tolerance</Label>
              <Input id="scanDteTol" inputMode="numeric" value={form.dteTolerance} onChange={(e) => setField("dteTolerance", e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="scanHold">Max hold</Label>
              <Input id="scanHold" inputMode="numeric" value={form.maxHolding} onChange={(e) => setField("maxHolding", e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="scanAcct">Account</Label>
              <Input id="scanAcct" inputMode="decimal" value={form.accountSize} onChange={(e) => setField("accountSize", e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="scanRisk">Risk %</Label>
              <Input id="scanRisk" inputMode="decimal" value={form.riskPct} onChange={(e) => setField("riskPct", e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="scanComm">Commission</Label>
              <Input id="scanComm" inputMode="decimal" value={form.commission} onChange={(e) => setField("commission", e.target.value)} />
            </div>
          </div>
          <div className="space-y-2">
            <Label htmlFor="scanMaxRecs">Max recommendations</Label>
            <Input id="scanMaxRecs" inputMode="numeric" className="max-w-32" value={form.maxRecs} onChange={(e) => setField("maxRecs", e.target.value)} />
            <p className="text-xs text-muted-foreground">1 to {getScannerLimits(planTier, form.mode).maxRecommendations}. Top-ranked combinations returned after evaluation.</p>
          </div>
        </CardContent>
      </Card>

      <div className="flex justify-end">
        <Button disabled={status === "submitting" || status === "success"} size="lg" type="submit">
          {status === "submitting" ? (
            <>
              <Loader2 className="h-4 w-4 animate-spin" />
              Launching scan...
            </>
          ) : status === "success" ? (
            "Scan launched — redirecting…"
          ) : (
            "Launch scan"
          )}
        </Button>
      </div>
    </form>
  );
}
