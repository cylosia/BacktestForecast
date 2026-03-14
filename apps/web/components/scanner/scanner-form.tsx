"use client";

import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { Loader2 } from "lucide-react";
import { createScannerJob } from "@/lib/api/client";
import { ApiError } from "@/lib/api/shared";
import type { CreateScannerJobRequest, ScannerMode, StrategyType } from "@backtestforecast/api-client";
import { isPlanLimitError, UpgradePrompt } from "@/components/billing/upgrade-prompt";
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
];

const ADVANCED_STRATEGIES = ADVANCED_STRATEGY_GROUPS.flatMap((g) => g.strategies);

function daysAgo(n: number): string {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d.toISOString().slice(0, 10);
}

export function ScannerForm({
  scannerModes,
}: {
  scannerModes: Array<"basic" | "advanced">;
}) {
  const router = useRouter();
  const { getToken } = useAuth();
  const submitAbortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    return () => { submitAbortRef.current?.abort(); };
  }, []);

  const defaultMode: ScannerMode = scannerModes.includes("advanced") ? "advanced" : "basic";

  const [mode, setMode] = useState<ScannerMode>(defaultMode);
  const [name, setName] = useState("");
  const [symbolsText, setSymbolsText] = useState("SPY, QQQ, AAPL");
  const [selectedStrategies, setSelectedStrategies] = useState<Set<string>>(
    new Set(["long_call", "long_put"]),
  );
  const [ruleSetName, setRuleSetName] = useState("RSI oversold");
  const [rsiEnabled, setRsiEnabled] = useState(true);
  const [rsiOperator, setRsiOperator] = useState("lt");
  const [rsiThreshold, setRsiThreshold] = useState("35");
  const [rsiPeriod, setRsiPeriod] = useState("14");

  const [startDate, setStartDate] = useState(daysAgo(365));
  const [endDate, setEndDate] = useState(daysAgo(0));
  const [targetDte, setTargetDte] = useState("30");
  const [dteTolerance, setDteTolerance] = useState("5");
  const [maxHolding, setMaxHolding] = useState("10");
  const [accountSize, setAccountSize] = useState("10000");
  const [riskPct, setRiskPct] = useState("2");
  const [commission, setCommission] = useState("0.65");
  const [maxRecs, setMaxRecs] = useState("10");

  const [status, setStatus] = useState<"idle" | "submitting" | "error">("idle");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [errorCode, setErrorCode] = useState<string | undefined>();

  useEffect(() => {
    const allowed: Set<string> = new Set(
      (mode === "advanced" ? ADVANCED_STRATEGIES : BASIC_STRATEGIES).map((s) => s.value),
    );
    setSelectedStrategies((prev) => {
      const pruned = new Set([...prev].filter((v) => allowed.has(v)));
      return pruned.size === prev.size ? prev : pruned;
    });
  }, [mode]);

  function toggleStrategy(value: string) {
    setSelectedStrategies((prev) => {
      const next = new Set(prev);
      if (next.has(value)) {
        next.delete(value);
      } else {
        next.add(value);
      }
      return next;
    });
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();

    const symbols = symbolsText
      .split(/[,\s]+/)
      .map((s) => s.trim().toUpperCase())
      .filter(Boolean);

    const errors: string[] = [];

    if (symbols.length === 0) {
      errors.push("At least one symbol is required.");
    }
    if (selectedStrategies.size === 0) {
      errors.push("At least one strategy type is required.");
    }

    const maxSymbols = mode === "advanced" ? 25 : 5;
    const maxStrategies = mode === "advanced" ? 14 : 6;
    if (symbols.length > maxSymbols) {
      errors.push(`${mode === "advanced" ? "Advanced" : "Basic"} mode allows at most ${maxSymbols} symbols.`);
    }
    if (selectedStrategies.size > maxStrategies) {
      errors.push(`${mode === "advanced" ? "Advanced" : "Basic"} mode allows at most ${maxStrategies} strategies.`);
    }

    if (startDate && endDate && new Date(startDate) >= new Date(endDate)) {
      errors.push("Start date must be before end date.");
    }

    const numericChecks: Array<{ label: string; value: number; min: number; max?: number; integer?: boolean }> = [
      { label: "Target DTE", value: Number(targetDte), min: 7, max: 365, integer: true },
      { label: "DTE tolerance", value: Number(dteTolerance), min: 0, max: 60, integer: true },
      { label: "Max holding days", value: Number(maxHolding), min: 1, max: 120, integer: true },
      { label: "Account size", value: Number(accountSize), min: 100 },
      { label: "Risk %", value: Number(riskPct), min: 0.1, max: 100 },
      { label: "Commission", value: Number(commission), min: 0 },
      { label: "Max recommendations", value: Number(maxRecs), min: 1, max: 30, integer: true },
    ];
    for (const check of numericChecks) {
      if (!Number.isFinite(check.value) || check.value < check.min || (check.max !== undefined && check.value > check.max)) {
        errors.push(`${check.label} must be a number between ${check.min} and ${check.max ?? "∞"}.`);
      } else if (check.integer && !Number.isInteger(check.value)) {
        errors.push(`${check.label} must be a whole number.`);
      }
    }

    if (errors.length > 0) {
      setStatus("error");
      setErrorMessage(errors.length === 1 ? errors[0] : errors.map((e, i) => `${i + 1}. ${e}`).join("\n"));
      return;
    }

    const entryRules: CreateScannerJobRequest["rule_sets"][0]["entry_rules"] = [];
    if (rsiEnabled) {
      const rsiThresholdNum = Number(rsiThreshold);
      const rsiPeriodNum = Number(rsiPeriod);
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
        operator: rsiOperator as "lt" | "lte" | "gt" | "gte",
        threshold: rsiThresholdNum,
        period: rsiPeriodNum,
      });
    }

    if (entryRules.length === 0) {
      setStatus("error");
      setErrorMessage("At least one entry rule is required in the rule set.");
      return;
    }

    const payload: CreateScannerJobRequest = {
      name: name.trim() || null,
      mode,
      symbols,
      strategy_types: Array.from(selectedStrategies) as StrategyType[],
      rule_sets: [{ name: ruleSetName.trim() || "Default", entry_rules: entryRules }],
      start_date: startDate,
      end_date: endDate,
      target_dte: Number(targetDte),
      dte_tolerance_days: Number(dteTolerance),
      max_holding_days: Number(maxHolding),
      account_size: Number(accountSize),
      risk_per_trade_pct: Number(riskPct),
      commission_per_contract: Number(commission),
      max_recommendations: Number(maxRecs),
      refresh_daily: false,
      refresh_priority: 50,
      idempotency_key: crypto.randomUUID(),
    };

    setStatus("submitting");
    setErrorMessage(null);
    setErrorCode(undefined);

    try {
      const token = await getToken();
      if (!token) throw new Error("Session expired.");

      submitAbortRef.current?.abort();
      submitAbortRef.current = new AbortController();
      const job = await createScannerJob(token, payload, submitAbortRef.current.signal);
      router.push(`/app/scanner/${job.id}`);
    } catch (error) {
      const msg = error instanceof ApiError ? error.message : error instanceof Error ? error.message : "Scan could not be created.";
      const code = error instanceof ApiError ? error.code : undefined;
      setStatus("error");
      setErrorMessage(msg);
      setErrorCode(code);
    }
  }

  return (
    <form className="space-y-6" onSubmit={handleSubmit}>
      {errorMessage && isPlanLimitError(errorCode) ? (
        <UpgradePrompt message={errorMessage} />
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
              <Input id="scanName" maxLength={120} placeholder="e.g. Weekly momentum scan" value={name} onChange={(e) => setName(e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="scanMode">Scanner mode</Label>
              <Select
                id="scanMode"
                value={mode}
                options={scannerModes.map((m) => ({ value: m, label: m === "basic" ? "Basic" : "Advanced" }))}
                onChange={(e) => setMode(e.target.value as ScannerMode)}
              />
            </div>
          </div>

          <div className="space-y-2">
            <Label htmlFor="symbols">Symbols (comma-separated)</Label>
            <Input id="symbols" maxLength={500} placeholder="SPY, QQQ, AAPL, MSFT" value={symbolsText} onChange={(e) => setSymbolsText(e.target.value)} />
          </div>

          <div className="space-y-2">
            <Label>Strategy types</Label>
            {mode === "advanced" ? (
              <div className="space-y-4">
                {ADVANCED_STRATEGY_GROUPS.map((group) => (
                  <div key={group.category}>
                    <p className="mb-2 text-xs font-medium uppercase tracking-wider text-muted-foreground">
                      {group.category}
                    </p>
                    <div className="flex flex-wrap gap-2">
                      {group.strategies.map((strategy) => (
                        <label
                          key={strategy.value}
                          className="inline-flex items-center gap-2 rounded-lg border border-border/70 px-3 py-2 text-sm cursor-pointer hover:bg-accent"
                        >
                          <input
                            type="checkbox"
                            className="h-4 w-4 rounded border-input"
                            checked={selectedStrategies.has(strategy.value)}
                            onChange={() => toggleStrategy(strategy.value)}
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
                {BASIC_STRATEGIES.map((strategy) => (
                  <label
                    key={strategy.value}
                    className="inline-flex items-center gap-2 rounded-lg border border-border/70 px-3 py-2 text-sm cursor-pointer hover:bg-accent"
                  >
                    <input
                      type="checkbox"
                      className="h-4 w-4 rounded border-input"
                      checked={selectedStrategies.has(strategy.value)}
                      onChange={() => toggleStrategy(strategy.value)}
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
            <Input id="ruleSetName" maxLength={120} placeholder="RSI oversold" value={ruleSetName} onChange={(e) => setRuleSetName(e.target.value)} />
          </div>

          <div className="rounded-xl border border-border/70 p-4">
            <div className="flex items-start justify-between">
              <div>
                <p className="font-medium">RSI rule</p>
                <p className="text-sm text-muted-foreground">Trigger entries when RSI crosses a threshold.</p>
              </div>
              <label className="inline-flex items-center gap-2 text-sm font-medium">
                <input type="checkbox" className="h-4 w-4 rounded border-input" checked={rsiEnabled} onChange={(e) => setRsiEnabled(e.target.checked)} />
                Enabled
              </label>
            </div>
            {rsiEnabled ? (
              <div className="mt-4 grid gap-4 sm:grid-cols-3">
                <div className="space-y-2">
                  <Label htmlFor="rsiOp">Operator</Label>
                  <Select id="rsiOp" value={rsiOperator} options={[
                    { value: "lt", label: "Less than" },
                    { value: "lte", label: "≤" },
                    { value: "gt", label: "Greater than" },
                    { value: "gte", label: "≥" },
                  ]} onChange={(e) => setRsiOperator(e.target.value)} />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="rsiThresh">Threshold</Label>
                  <Input id="rsiThresh" inputMode="decimal" value={rsiThreshold} onChange={(e) => setRsiThreshold(e.target.value)} />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="rsiPer">Period</Label>
                  <Input id="rsiPer" inputMode="numeric" value={rsiPeriod} onChange={(e) => setRsiPeriod(e.target.value)} />
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
          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="scanStart">Start date</Label>
              <Input id="scanStart" type="date" value={startDate} onChange={(e) => setStartDate(e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="scanEnd">End date</Label>
              <Input id="scanEnd" type="date" value={endDate} onChange={(e) => setEndDate(e.target.value)} />
            </div>
          </div>
          <div className="grid gap-4 sm:grid-cols-3 lg:grid-cols-6">
            <div className="space-y-2">
              <Label htmlFor="scanDte">Target DTE</Label>
              <Input id="scanDte" inputMode="numeric" value={targetDte} onChange={(e) => setTargetDte(e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="scanDteTol">DTE tolerance</Label>
              <Input id="scanDteTol" inputMode="numeric" value={dteTolerance} onChange={(e) => setDteTolerance(e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="scanHold">Max hold</Label>
              <Input id="scanHold" inputMode="numeric" value={maxHolding} onChange={(e) => setMaxHolding(e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="scanAcct">Account</Label>
              <Input id="scanAcct" inputMode="decimal" value={accountSize} onChange={(e) => setAccountSize(e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="scanRisk">Risk %</Label>
              <Input id="scanRisk" inputMode="decimal" value={riskPct} onChange={(e) => setRiskPct(e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="scanComm">Commission</Label>
              <Input id="scanComm" inputMode="decimal" value={commission} onChange={(e) => setCommission(e.target.value)} />
            </div>
          </div>
          <div className="space-y-2">
            <Label htmlFor="scanMaxRecs">Max recommendations</Label>
            <Input id="scanMaxRecs" inputMode="numeric" className="max-w-32" value={maxRecs} onChange={(e) => setMaxRecs(e.target.value)} />
            <p className="text-xs text-muted-foreground">1 to 30. Top-ranked combinations returned after evaluation.</p>
          </div>
        </CardContent>
      </Card>

      <div className="flex justify-end">
        <Button disabled={status === "submitting"} size="lg" type="submit">
          {status === "submitting" ? (
            <>
              <Loader2 className="h-4 w-4 animate-spin" />
              Launching scan...
            </>
          ) : (
            "Launch scan"
          )}
        </Button>
      </div>
    </form>
  );
}
