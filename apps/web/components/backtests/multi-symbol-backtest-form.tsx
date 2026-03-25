"use client";

import { useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { Loader2, Plus, Trash2 } from "lucide-react";
import { createMultiSymbolRun } from "@/lib/api/client";
import type {
  CreateMultiSymbolRunRequest,
  CrossSymbolRule,
  MultiSymbolDefinition,
  MultiSymbolLegDefinition,
} from "@/lib/api/multi-workflow-types";
import { ApiError } from "@/lib/api/shared";
import { getOrCreatePendingIdempotencyKey } from "@/lib/idempotency";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";

type EditableSymbol = {
  symbol: string;
  risk_per_trade_pct: string;
  max_open_positions: string;
  capital_allocation_pct: string;
};

type EditableLeg = {
  symbol: string;
  strategy_type: string;
  target_dte: string;
  dte_tolerance_days: string;
  max_holding_days: string;
  quantity_mode: "risk_based" | "fixed_contracts";
  fixed_contracts: string;
};

type EditableRule = {
  left_symbol: string;
  left_indicator: string;
  operator: string;
  right_symbol: string;
  right_indicator: string;
  threshold: string;
  lookback_period: string;
};

const STRATEGY_OPTIONS = [
  { value: "long_call", label: "Long Call" },
  { value: "long_put", label: "Long Put" },
  { value: "cash_secured_put", label: "Cash-Secured Put" },
  { value: "covered_call", label: "Covered Call" },
  { value: "calendar_spread", label: "Calendar Spread" },
  { value: "diagonal_spread", label: "Diagonal Spread" },
];

const INDICATOR_OPTIONS = [
  { value: "close", label: "Close" },
  { value: "rsi_14", label: "RSI 14" },
  { value: "ema_8", label: "EMA 8" },
  { value: "ema_21", label: "EMA 21" },
  { value: "sma_50", label: "SMA 50" },
  { value: "sma_200", label: "SMA 200" },
];

const OPERATOR_OPTIONS = [
  { value: "lt", label: "<" },
  { value: "lte", label: "<=" },
  { value: "gt", label: ">" },
  { value: "gte", label: ">=" },
  { value: "eq", label: "=" },
  { value: "neq", label: "!=" },
];

function createSymbol(symbol = "", allocation = ""): EditableSymbol {
  return {
    symbol,
    risk_per_trade_pct: "2",
    max_open_positions: "1",
    capital_allocation_pct: allocation,
  };
}

function createLeg(symbol = ""): EditableLeg {
  return {
    symbol,
    strategy_type: "long_call",
    target_dte: "21",
    dte_tolerance_days: "5",
    max_holding_days: "10",
    quantity_mode: "risk_based",
    fixed_contracts: "1",
  };
}

function createRule(leftSymbol = "", threshold = "35"): EditableRule {
  return {
    left_symbol: leftSymbol,
    left_indicator: "rsi_14",
    operator: "lt",
    right_symbol: "",
    right_indicator: "close",
    threshold,
    lookback_period: "",
  };
}

export function MultiSymbolBacktestForm() {
  const router = useRouter();
  const { getToken } = useAuth();
  const [name, setName] = useState("");
  const [startDate, setStartDate] = useState("2024-01-01");
  const [endDate, setEndDate] = useState("2025-12-31");
  const [accountSize, setAccountSize] = useState("100000");
  const [commissionPerContract, setCommissionPerContract] = useState("0.65");
  const [slippagePct, setSlippagePct] = useState("0.25");
  const [groupName, setGroupName] = useState("Primary group");
  const [capitalAllocationMode, setCapitalAllocationMode] = useState<"equal_weight" | "explicit">("explicit");
  const [symbols, setSymbols] = useState<EditableSymbol[]>([
    createSymbol("UVXY", "40"),
    createSymbol("VIX", "30"),
    createSymbol("SPY", "30"),
  ]);
  const [legs, setLegs] = useState<EditableLeg[]>([
    createLeg("UVXY"),
    createLeg("VIX"),
    createLeg("SPY"),
  ]);
  const [entryRules, setEntryRules] = useState<EditableRule[]>([createRule("UVXY")]);
  const [exitRules, setExitRules] = useState<EditableRule[]>([]);
  const [status, setStatus] = useState<"idle" | "submitting" | "error">("idle");
  const [message, setMessage] = useState<string | null>(null);

  const symbolOptions = useMemo(
    () =>
      symbols
        .map((item) => item.symbol.trim().toUpperCase())
        .filter(Boolean)
        .map((symbol) => ({ value: symbol, label: symbol })),
    [symbols],
  );

  function updateSymbol(index: number, patch: Partial<EditableSymbol>) {
    setSymbols((current) => current.map((item, itemIndex) => (itemIndex === index ? { ...item, ...patch } : item)));
  }

  function updateLeg(index: number, patch: Partial<EditableLeg>) {
    setLegs((current) => current.map((item, itemIndex) => (itemIndex === index ? { ...item, ...patch } : item)));
  }

  function updateRule(kind: "entry" | "exit", index: number, patch: Partial<EditableRule>) {
    const setter = kind === "entry" ? setEntryRules : setExitRules;
    setter((current) => current.map((item, itemIndex) => (itemIndex === index ? { ...item, ...patch } : item)));
  }

  function buildSymbolPayload(): MultiSymbolDefinition[] {
    const activeSymbols = symbols
      .map((item) => ({
        symbol: item.symbol.trim().toUpperCase(),
        risk_per_trade_pct: item.risk_per_trade_pct,
        max_open_positions: Number(item.max_open_positions),
        capital_allocation_pct:
          capitalAllocationMode === "explicit" ? item.capital_allocation_pct || null : null,
      }))
      .filter((item) => item.symbol.length > 0);

    if (activeSymbols.length < 2 || activeSymbols.length > 3) {
      throw new Error("Multi-Symbol requires between 2 and 3 symbols.");
    }

    return activeSymbols;
  }

  function buildLegPayload(): MultiSymbolLegDefinition[] {
    const activeLegs = legs
      .map((leg) => ({
        symbol: leg.symbol.trim().toUpperCase(),
        strategy_type: leg.strategy_type,
        target_dte: Number(leg.target_dte),
        dte_tolerance_days: Number(leg.dte_tolerance_days),
        max_holding_days: Number(leg.max_holding_days),
        quantity_mode: leg.quantity_mode,
        fixed_contracts: leg.quantity_mode === "fixed_contracts" ? Number(leg.fixed_contracts) : null,
      }))
      .filter((leg) => leg.symbol.length > 0);

    if (activeLegs.length === 0) {
      throw new Error("Add at least one option leg to the strategy group.");
    }

    return activeLegs;
  }

  function buildRules(rules: EditableRule[]): CrossSymbolRule[] {
    const normalized: CrossSymbolRule[] = [];
    for (const rule of rules) {
      const leftSymbol = rule.left_symbol.trim().toUpperCase();
      const rightSymbol = rule.right_symbol.trim().toUpperCase();
      const threshold = rule.threshold.trim();
      if (!leftSymbol) continue;
      if (!rightSymbol && !threshold) {
        throw new Error(`Rule for ${leftSymbol} needs either a comparison symbol or a threshold.`);
      }
      normalized.push({
        left_symbol: leftSymbol,
        left_indicator: rule.left_indicator,
        operator: rule.operator,
        right_symbol: rightSymbol || null,
        right_indicator: rightSymbol ? rule.right_indicator : null,
        threshold: rightSymbol ? null : threshold,
        lookback_period: rule.lookback_period.trim() ? Number(rule.lookback_period) : null,
      });
    }
    return normalized;
  }

  async function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setStatus("submitting");
    setMessage(null);

    try {
      const token = await getToken();
      if (!token) throw new Error("Missing session token.");

      const payload: CreateMultiSymbolRunRequest = {
        name: name.trim() || null,
        symbols: buildSymbolPayload(),
        strategy_groups: [
          {
            name: groupName.trim() || "Primary group",
            synchronous_entry: true,
            legs: buildLegPayload(),
          },
        ],
        entry_rules: buildRules(entryRules),
        exit_rules: buildRules(exitRules),
        start_date: startDate,
        end_date: endDate,
        account_size: accountSize,
        capital_allocation_mode: capitalAllocationMode,
        commission_per_contract: commissionPerContract,
        slippage_pct: slippagePct,
        idempotency_key: getOrCreatePendingIdempotencyKey(null, "multi-symbol-backtest"),
      };

      const run = await createMultiSymbolRun(token, payload);
      router.push(`/app/multi-symbol-backtests/${run.id}`);
      router.refresh();
    } catch (error) {
      setStatus("error");
      setMessage(error instanceof ApiError ? error.message : error instanceof Error ? error.message : "Unable to create multi-symbol run.");
    }
  }

  return (
    <form className="space-y-6" onSubmit={handleSubmit}>
      {message ? (
        <div className="rounded-xl border border-destructive/40 bg-destructive/5 p-4 text-sm text-destructive">
          {message}
        </div>
      ) : null}

      <Card>
        <CardHeader>
          <CardTitle>Multi-Symbol workflow builder</CardTitle>
          <CardDescription>
            Coordinate 2-3 symbols, attach legs to any symbol, and define cross-symbol entry and exit logic.
          </CardDescription>
        </CardHeader>
        <CardContent className="grid gap-4 lg:grid-cols-2">
          <div className="grid gap-2">
            <Label htmlFor="ms-name">Run name</Label>
            <Input id="ms-name" value={name} onChange={(event) => setName(event.target.value)} />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="ms-group">Strategy group name</Label>
            <Input id="ms-group" value={groupName} onChange={(event) => setGroupName(event.target.value)} />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="ms-start">Start date</Label>
            <Input id="ms-start" type="date" value={startDate} onChange={(event) => setStartDate(event.target.value)} />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="ms-end">End date</Label>
            <Input id="ms-end" type="date" value={endDate} onChange={(event) => setEndDate(event.target.value)} />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="ms-account">Account size</Label>
            <Input id="ms-account" value={accountSize} onChange={(event) => setAccountSize(event.target.value)} />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="ms-mode">Capital allocation mode</Label>
            <Select
              id="ms-mode"
              value={capitalAllocationMode}
              onChange={(event) => setCapitalAllocationMode(event.target.value as "equal_weight" | "explicit")}
              options={[
                { value: "explicit", label: "Explicit" },
                { value: "equal_weight", label: "Equal weight" },
              ]}
            />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="ms-commission">Commission per contract</Label>
            <Input id="ms-commission" value={commissionPerContract} onChange={(event) => setCommissionPerContract(event.target.value)} />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="ms-slippage">Slippage %</Label>
            <Input id="ms-slippage" value={slippagePct} onChange={(event) => setSlippagePct(event.target.value)} />
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Symbols</CardTitle>
          <CardDescription>Risk and capital are tracked per symbol.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {symbols.map((symbol, index) => (
            <div key={`symbol-${index}`} className="grid gap-3 rounded-xl border border-border/70 p-4 lg:grid-cols-[1.2fr_1fr_1fr_1fr_auto]">
              <div className="grid gap-2">
                <Label htmlFor={`ms-symbol-${index}`}>Symbol</Label>
                <Input
                  id={`ms-symbol-${index}`}
                  value={symbol.symbol}
                  onChange={(event) => updateSymbol(index, { symbol: event.target.value.toUpperCase() })}
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor={`ms-symbol-risk-${index}`}>Risk %</Label>
                <Input
                  id={`ms-symbol-risk-${index}`}
                  value={symbol.risk_per_trade_pct}
                  onChange={(event) => updateSymbol(index, { risk_per_trade_pct: event.target.value })}
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor={`ms-symbol-max-${index}`}>Max open</Label>
                <Input
                  id={`ms-symbol-max-${index}`}
                  value={symbol.max_open_positions}
                  onChange={(event) => updateSymbol(index, { max_open_positions: event.target.value })}
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor={`ms-symbol-allocation-${index}`}>Allocation %</Label>
                <Input
                  id={`ms-symbol-allocation-${index}`}
                  value={symbol.capital_allocation_pct}
                  onChange={(event) => updateSymbol(index, { capital_allocation_pct: event.target.value })}
                  disabled={capitalAllocationMode !== "explicit"}
                />
              </div>
              <div className="flex items-end">
                <Button
                  type="button"
                  variant="ghost"
                  onClick={() => setSymbols((current) => current.filter((_, itemIndex) => itemIndex !== index))}
                  disabled={symbols.length <= 2}
                >
                  <Trash2 className="h-4 w-4" />
                </Button>
              </div>
            </div>
          ))}
          <Button
            type="button"
            variant="outline"
            onClick={() => setSymbols((current) => (current.length >= 3 ? current : [...current, createSymbol("", "")]))}
            disabled={symbols.length >= 3}
          >
            <Plus className="h-4 w-4" />
            Add symbol
          </Button>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Strategy legs</CardTitle>
          <CardDescription>Any declared symbol can carry legs inside the synchronous entry group.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {legs.map((leg, index) => (
            <div key={`leg-${index}`} className="grid gap-3 rounded-xl border border-border/70 p-4 lg:grid-cols-6">
              <div className="grid gap-2">
                <Label htmlFor={`ms-leg-symbol-${index}`}>Symbol</Label>
                <Select
                  id={`ms-leg-symbol-${index}`}
                  value={leg.symbol}
                  onChange={(event) => updateLeg(index, { symbol: event.target.value })}
                  options={symbolOptions.length > 0 ? symbolOptions : [{ value: "", label: "Select symbol" }]}
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor={`ms-leg-strategy-${index}`}>Strategy</Label>
                <Select
                  id={`ms-leg-strategy-${index}`}
                  value={leg.strategy_type}
                  onChange={(event) => updateLeg(index, { strategy_type: event.target.value })}
                  options={STRATEGY_OPTIONS}
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor={`ms-leg-dte-${index}`}>Target DTE</Label>
                <Input
                  id={`ms-leg-dte-${index}`}
                  value={leg.target_dte}
                  onChange={(event) => updateLeg(index, { target_dte: event.target.value })}
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor={`ms-leg-tol-${index}`}>Tolerance</Label>
                <Input
                  id={`ms-leg-tol-${index}`}
                  value={leg.dte_tolerance_days}
                  onChange={(event) => updateLeg(index, { dte_tolerance_days: event.target.value })}
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor={`ms-leg-hold-${index}`}>Max hold</Label>
                <Input
                  id={`ms-leg-hold-${index}`}
                  value={leg.max_holding_days}
                  onChange={(event) => updateLeg(index, { max_holding_days: event.target.value })}
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor={`ms-leg-qty-mode-${index}`}>Quantity</Label>
                <div className="flex gap-2">
                  <Select
                    id={`ms-leg-qty-mode-${index}`}
                    className="min-w-0"
                    value={leg.quantity_mode}
                    onChange={(event) => updateLeg(index, { quantity_mode: event.target.value as EditableLeg["quantity_mode"] })}
                    options={[
                      { value: "risk_based", label: "Risk-based" },
                      { value: "fixed_contracts", label: "Fixed" },
                    ]}
                  />
                  {leg.quantity_mode === "fixed_contracts" ? (
                    <Input
                      value={leg.fixed_contracts}
                      onChange={(event) => updateLeg(index, { fixed_contracts: event.target.value })}
                    />
                  ) : null}
                  <Button
                    type="button"
                    variant="ghost"
                    onClick={() => setLegs((current) => current.filter((_, itemIndex) => itemIndex !== index))}
                    disabled={legs.length <= 1}
                  >
                    <Trash2 className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            </div>
          ))}
          <Button type="button" variant="outline" onClick={() => setLegs((current) => [...current, createLeg(symbolOptions[0]?.value ?? "")])}>
            <Plus className="h-4 w-4" />
            Add leg
          </Button>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Cross-symbol rules</CardTitle>
          <CardDescription>Rules can compare a symbol indicator to either another symbol or a threshold.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-6">
          {([
            ["Entry rules", entryRules, setEntryRules, "entry"] as const,
            ["Exit rules", exitRules, setExitRules, "exit"] as const,
          ]).map(([title, rules, setRules, kind]) => (
            <div key={kind} className="space-y-4">
              <div className="flex items-center justify-between gap-4">
                <h3 className="text-sm font-semibold uppercase tracking-[0.16em] text-muted-foreground">{title}</h3>
                <Button type="button" variant="outline" onClick={() => setRules((current) => [...current, createRule(symbolOptions[0]?.value ?? "")])}>
                  <Plus className="h-4 w-4" />
                  Add rule
                </Button>
              </div>
              {rules.length === 0 ? (
                <p className="text-sm text-muted-foreground">No {kind} rules configured.</p>
              ) : (
                rules.map((rule, index) => (
                  <div key={`${kind}-${index}`} className="grid gap-3 rounded-xl border border-border/70 p-4 lg:grid-cols-[1fr_1fr_90px_1fr_1fr_1fr_auto]">
                    <Select
                      value={rule.left_symbol}
                      onChange={(event) => updateRule(kind, index, { left_symbol: event.target.value })}
                      options={symbolOptions.length > 0 ? symbolOptions : [{ value: "", label: "Left symbol" }]}
                    />
                    <Select
                      value={rule.left_indicator}
                      onChange={(event) => updateRule(kind, index, { left_indicator: event.target.value })}
                      options={INDICATOR_OPTIONS}
                    />
                    <Select
                      value={rule.operator}
                      onChange={(event) => updateRule(kind, index, { operator: event.target.value })}
                      options={OPERATOR_OPTIONS}
                    />
                    <Select
                      value={rule.right_symbol}
                      onChange={(event) => updateRule(kind, index, { right_symbol: event.target.value })}
                      options={[{ value: "", label: "Threshold" }, ...symbolOptions]}
                    />
                    <Select
                      value={rule.right_indicator}
                      onChange={(event) => updateRule(kind, index, { right_indicator: event.target.value })}
                      options={INDICATOR_OPTIONS}
                      disabled={!rule.right_symbol}
                    />
                    <Input
                      placeholder={rule.right_symbol ? "Unused" : "Threshold"}
                      value={rule.threshold}
                      disabled={Boolean(rule.right_symbol)}
                      onChange={(event) => updateRule(kind, index, { threshold: event.target.value })}
                    />
                    <Button
                      type="button"
                      variant="ghost"
                      onClick={() => setRules((current) => current.filter((_, itemIndex) => itemIndex !== index))}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </div>
                ))
              )}
            </div>
          ))}
        </CardContent>
      </Card>

      <Button type="submit" disabled={status === "submitting"}>
        {status === "submitting" ? (
          <>
            <Loader2 className="h-4 w-4 animate-spin" />
            Creating multi-symbol run...
          </>
        ) : (
          "Create multi-symbol run"
        )}
      </Button>
    </form>
  );
}
