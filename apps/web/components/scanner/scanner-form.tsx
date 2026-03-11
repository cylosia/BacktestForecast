"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { Loader2 } from "lucide-react";
import { createScannerJob } from "@/lib/api/client";
import { ApiError } from "@/lib/api/shared";
import type { CreateScannerJobRequest, ScannerMode } from "@/lib/backtests/types";
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

const ADVANCED_STRATEGIES = [
  ...BASIC_STRATEGIES,
  { value: "bull_put_credit_spread", label: "Bull Put Credit Spread" },
  { value: "bear_call_credit_spread", label: "Bear Call Credit Spread" },
  { value: "iron_condor", label: "Iron Condor" },
  { value: "long_straddle", label: "Long Straddle" },
  { value: "long_strangle", label: "Long Strangle" },
  { value: "calendar_spread", label: "Calendar Spread" },
  { value: "butterfly", label: "Butterfly" },
  { value: "wheel_strategy", label: "Wheel Strategy" },
] as const;

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

  const activeStrategies = mode === "advanced" ? ADVANCED_STRATEGIES : BASIC_STRATEGIES;

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

    if (symbols.length === 0) {
      setStatus("error");
      setErrorMessage("At least one symbol is required.");
      return;
    }
    if (selectedStrategies.size === 0) {
      setStatus("error");
      setErrorMessage("At least one strategy type is required.");
      return;
    }

    const numericChecks = [
      { label: "Target DTE", value: Number(targetDte), min: 1, max: 365 },
      { label: "DTE tolerance", value: Number(dteTolerance), min: 1, max: 60 },
      { label: "Max holding days", value: Number(maxHolding), min: 1, max: 365 },
      { label: "Account size", value: Number(accountSize), min: 100 },
      { label: "Risk %", value: Number(riskPct), min: 0.1, max: 100 },
      { label: "Commission", value: Number(commission), min: 0 },
      { label: "Max recommendations", value: Number(maxRecs), min: 1, max: 30 },
    ];
    for (const check of numericChecks) {
      if (!Number.isFinite(check.value) || check.value < check.min || (check.max !== undefined && check.value > check.max)) {
        setStatus("error");
        setErrorMessage(`${check.label} must be a number between ${check.min} and ${check.max ?? "∞"}.`);
        return;
      }
    }

    const entryRules: CreateScannerJobRequest["rule_sets"][0]["entry_rules"] = [];
    if (rsiEnabled) {
      entryRules.push({
        type: "rsi" as const,
        operator: rsiOperator as "lt" | "lte" | "gt" | "gte",
        threshold: Number(rsiThreshold),
        period: Number(rsiPeriod),
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
      strategy_types: Array.from(selectedStrategies),
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
      idempotency_key: crypto.randomUUID(),
    };

    setStatus("submitting");
    setErrorMessage(null);
    setErrorCode(undefined);

    try {
      const token = await getToken();
      if (!token) throw new Error("Session expired.");

      const job = await createScannerJob(token, payload);
      router.push(`/app/scanner/${job.id}`);
      router.refresh();
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
        <div className="rounded-xl border border-destructive/40 bg-destructive/5 p-4 text-sm text-destructive">
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
            <Input id="symbols" placeholder="SPY, QQQ, AAPL, MSFT" value={symbolsText} onChange={(e) => setSymbolsText(e.target.value)} />
          </div>

          <div className="space-y-2">
            <Label>Strategy types</Label>
            <div className="flex flex-wrap gap-2">
              {activeStrategies.map((strategy) => (
                <label key={strategy.value} className="inline-flex items-center gap-2 rounded-lg border border-border/70 px-3 py-2 text-sm cursor-pointer hover:bg-accent">
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
