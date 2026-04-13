"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { Loader2, Plus, Trash2 } from "lucide-react";
import { createMultiStepRun } from "@/lib/api/client";
import type { CreateMultiStepRunRequest, WorkflowStepDefinition } from "@/lib/api/multi-workflow-types";
import { ApiError } from "@/lib/api/shared";
import { getOrCreatePendingIdempotencyKey } from "@/lib/idempotency";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";

type EditableIndicatorRule = {
  type: string;
  operator: string;
  threshold: string;
  period: string;
};

type EditableStep = {
  name: string;
  action: WorkflowStepDefinition["action"];
  triggerMode: "date_offset" | "after_expiration" | "rule_match" | "event_and_rule";
  days_after_prior_step: string;
  require_prior_step_status: "" | "filled" | "expired" | "closed";
  strategy_type: string;
  target_dte: string;
  dte_tolerance_days: string;
  max_holding_days: string;
  triggerRules: EditableIndicatorRule[];
};

const ACTION_OPTIONS = [
  { value: "open_position", label: "Open position" },
  { value: "sell_premium", label: "Sell premium" },
  { value: "roll", label: "Roll" },
  { value: "hedge", label: "Hedge" },
  { value: "close_position", label: "Close position" },
];

const TRIGGER_OPTIONS = [
  { value: "rule_match", label: "Rule match" },
  { value: "after_expiration", label: "After expiration" },
  { value: "date_offset", label: "Date offset" },
  { value: "event_and_rule", label: "Event and rule" },
];

const STRATEGY_OPTIONS = [
  { value: "calendar_spread", label: "Calendar Spread" },
  { value: "put_calendar_spread", label: "Put Calendar Spread" },
  { value: "diagonal_spread", label: "Diagonal Spread" },
  { value: "covered_call", label: "Covered Call" },
  { value: "cash_secured_put", label: "Cash-Secured Put" },
  { value: "long_call", label: "Long Call" },
  { value: "long_put", label: "Long Put" },
];

const RULE_TYPE_OPTIONS = [
  { value: "rsi", label: "RSI" },
  { value: "ema", label: "EMA" },
  { value: "sma", label: "SMA" },
  { value: "close", label: "Close" },
];

const OPERATOR_OPTIONS = [
  { value: "lt", label: "<" },
  { value: "lte", label: "<=" },
  { value: "gt", label: ">" },
  { value: "gte", label: ">=" },
];

function createIndicatorRule(): EditableIndicatorRule {
  return {
    type: "rsi",
    operator: "lt",
    threshold: "35",
    period: "14",
  };
}

function createStep(name: string, action: EditableStep["action"], triggerMode: EditableStep["triggerMode"]): EditableStep {
  return {
    name,
    action,
    triggerMode,
    days_after_prior_step: "0",
    require_prior_step_status: triggerMode === "after_expiration" ? "expired" : "",
    strategy_type: "calendar_spread",
    target_dte: action === "sell_premium" ? "14" : "21",
    dte_tolerance_days: "5",
    max_holding_days: "7",
    triggerRules: [createIndicatorRule()],
  };
}

function normalizeIndicatorRules(rules: EditableIndicatorRule[]) {
  return rules.map((rule) => ({
    type: rule.type,
    operator: rule.operator,
    threshold: Number(rule.threshold),
    period: Number(rule.period),
  }));
}

export function MultiStepBacktestForm() {
  const router = useRouter();
  const { getToken } = useAuth();
  const [name, setName] = useState("");
  const [symbol, setSymbol] = useState("SPY");
  const [workflowType, setWorkflowType] = useState("calendar_roll_premium");
  const [startDate, setStartDate] = useState("2024-01-01");
  const [endDate, setEndDate] = useState("2025-12-31");
  const [accountSize, setAccountSize] = useState("100000");
  const [riskPerTradePct, setRiskPerTradePct] = useState("2");
  const [commissionPerContract, setCommissionPerContract] = useState("0.65");
  const [slippagePct, setSlippagePct] = useState("0.25");
  const [initialRules, setInitialRules] = useState<EditableIndicatorRule[]>([createIndicatorRule()]);
  const [steps, setSteps] = useState<EditableStep[]>([
    createStep("Open calendar", "open_position", "rule_match"),
    createStep("Sell second weekly", "sell_premium", "after_expiration"),
  ]);
  const [status, setStatus] = useState<"idle" | "submitting" | "error">("idle");
  const [message, setMessage] = useState<string | null>(null);

  function updateInitialRule(index: number, patch: Partial<EditableIndicatorRule>) {
    setInitialRules((current) => current.map((item, itemIndex) => (itemIndex === index ? { ...item, ...patch } : item)));
  }

  function updateStep(index: number, patch: Partial<EditableStep>) {
    setSteps((current) => current.map((item, itemIndex) => (itemIndex === index ? { ...item, ...patch } : item)));
  }

  function updateStepRule(stepIndex: number, ruleIndex: number, patch: Partial<EditableIndicatorRule>) {
    setSteps((current) =>
      current.map((step, currentStepIndex) =>
        currentStepIndex === stepIndex
          ? {
              ...step,
              triggerRules: step.triggerRules.map((rule, currentRuleIndex) =>
                currentRuleIndex === ruleIndex ? { ...rule, ...patch } : rule,
              ),
            }
          : step,
      ),
    );
  }

  async function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setStatus("submitting");
    setMessage(null);

    try {
      const token = await getToken();
      if (!token) throw new Error("Missing session token.");

      const payload: CreateMultiStepRunRequest = {
        name: name.trim() || null,
        symbol: symbol.trim().toUpperCase(),
        workflow_type: workflowType.trim(),
        start_date: startDate,
        end_date: endDate,
        account_size: accountSize,
        risk_per_trade_pct: riskPerTradePct,
        commission_per_contract: commissionPerContract,
        slippage_pct: slippagePct,
        initial_entry_rules: normalizeIndicatorRules(initialRules),
        steps: steps.map((step, index) => ({
          step_number: index + 1,
          name: step.name.trim() || `Step ${index + 1}`,
          action: step.action,
          trigger: {
            mode: step.triggerMode,
            days_after_prior_step: step.triggerMode === "date_offset" ? Number(step.days_after_prior_step) : null,
            rules:
              step.triggerMode === "rule_match" || step.triggerMode === "event_and_rule"
                ? normalizeIndicatorRules(step.triggerRules)
                : [],
            require_prior_step_status: step.require_prior_step_status || null,
          },
          contract_selection: {
            strategy_type: step.strategy_type,
            target_dte: Number(step.target_dte),
            dte_tolerance_days: Number(step.dte_tolerance_days),
            max_holding_days: Number(step.max_holding_days),
          },
          failure_policy: "liquidate",
        })),
        idempotency_key: getOrCreatePendingIdempotencyKey(null, "multi-step-backtest"),
      };

      const run = await createMultiStepRun(token, payload);
      router.push(`/app/multi-step-backtests/${run.id}`);
      router.refresh();
    } catch (error) {
      setStatus("error");
      setMessage(error instanceof ApiError ? error.message : error instanceof Error ? error.message : "Unable to create multi-step run.");
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
          <CardTitle>Multi-Step workflow builder</CardTitle>
          <CardDescription>
            Build staged same-symbol workflows with ordered triggers, leg transitions, and liquidation-on-failure behavior.
          </CardDescription>
        </CardHeader>
        <CardContent className="grid gap-4 lg:grid-cols-2">
          <div className="grid gap-2">
            <Label htmlFor="mst-name">Run name</Label>
            <Input id="mst-name" value={name} onChange={(event) => setName(event.target.value)} />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="mst-symbol">Symbol</Label>
            <Input id="mst-symbol" value={symbol} onChange={(event) => setSymbol(event.target.value.toUpperCase())} />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="mst-workflow">Workflow type</Label>
            <Input id="mst-workflow" value={workflowType} onChange={(event) => setWorkflowType(event.target.value)} />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="mst-account">Account size</Label>
            <Input id="mst-account" value={accountSize} onChange={(event) => setAccountSize(event.target.value)} />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="mst-start">Start date</Label>
            <Input id="mst-start" type="date" value={startDate} onChange={(event) => setStartDate(event.target.value)} />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="mst-end">End date</Label>
            <Input id="mst-end" type="date" value={endDate} onChange={(event) => setEndDate(event.target.value)} />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="mst-risk">Risk per trade %</Label>
            <Input id="mst-risk" value={riskPerTradePct} onChange={(event) => setRiskPerTradePct(event.target.value)} />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="mst-commission">Commission per contract</Label>
            <Input id="mst-commission" value={commissionPerContract} onChange={(event) => setCommissionPerContract(event.target.value)} />
          </div>
          <div className="grid gap-2 lg:col-span-2">
            <Label htmlFor="mst-slippage">Slippage %</Label>
            <Input id="mst-slippage" value={slippagePct} onChange={(event) => setSlippagePct(event.target.value)} />
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Initial entry rules</CardTitle>
          <CardDescription>These rules decide when the workflow can start.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {initialRules.map((rule, index) => (
            <div key={`initial-rule-${index}`} className="grid gap-3 rounded-xl border border-border/70 p-4 lg:grid-cols-[1fr_100px_1fr_1fr_auto]">
              <Select
                value={rule.type}
                onChange={(event) => updateInitialRule(index, { type: event.target.value })}
                options={RULE_TYPE_OPTIONS}
              />
              <Select
                value={rule.operator}
                onChange={(event) => updateInitialRule(index, { operator: event.target.value })}
                options={OPERATOR_OPTIONS}
              />
              <Input value={rule.threshold} onChange={(event) => updateInitialRule(index, { threshold: event.target.value })} />
              <Input value={rule.period} onChange={(event) => updateInitialRule(index, { period: event.target.value })} />
              <Button
                type="button"
                variant="ghost"
                onClick={() => setInitialRules((current) => current.filter((_, itemIndex) => itemIndex !== index))}
                disabled={initialRules.length <= 1}
              >
                <Trash2 className="h-4 w-4" />
              </Button>
            </div>
          ))}
          <Button type="button" variant="outline" onClick={() => setInitialRules((current) => [...current, createIndicatorRule()])}>
            <Plus className="h-4 w-4" />
            Add initial rule
          </Button>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Workflow steps</CardTitle>
          <CardDescription>Define ordered transitions for opening, rolling, hedging, and closing inventory.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {steps.map((step, index) => (
            <div key={`step-${index}`} className="space-y-4 rounded-xl border border-border/70 p-4">
              <div className="flex items-start justify-between gap-4">
                <div>
                  <p className="text-sm font-semibold uppercase tracking-[0.16em] text-muted-foreground">Step {index + 1}</p>
                  <p className="mt-1 text-lg font-semibold">{step.name}</p>
                </div>
                <Button
                  type="button"
                  variant="ghost"
                  onClick={() => setSteps((current) => current.filter((_, itemIndex) => itemIndex !== index))}
                  disabled={steps.length <= 2}
                >
                  <Trash2 className="h-4 w-4" />
                </Button>
              </div>

              <div className="grid gap-4 lg:grid-cols-3">
                <div className="grid gap-2">
                  <Label htmlFor={`mst-step-name-${index}`}>Step name</Label>
                  <Input
                    id={`mst-step-name-${index}`}
                    value={step.name}
                    onChange={(event) => updateStep(index, { name: event.target.value })}
                  />
                </div>
                <div className="grid gap-2">
                  <Label htmlFor={`mst-step-action-${index}`}>Action</Label>
                  <Select
                    id={`mst-step-action-${index}`}
                    value={step.action}
                    onChange={(event) => updateStep(index, { action: event.target.value as EditableStep["action"] })}
                    options={ACTION_OPTIONS}
                  />
                </div>
                <div className="grid gap-2">
                  <Label htmlFor={`mst-step-trigger-${index}`}>Trigger</Label>
                  <Select
                    id={`mst-step-trigger-${index}`}
                    value={step.triggerMode}
                    onChange={(event) => updateStep(index, { triggerMode: event.target.value as EditableStep["triggerMode"] })}
                    options={TRIGGER_OPTIONS}
                  />
                </div>
                <div className="grid gap-2">
                  <Label htmlFor={`mst-step-strategy-${index}`}>Strategy</Label>
                  <Select
                    id={`mst-step-strategy-${index}`}
                    value={step.strategy_type}
                    onChange={(event) => updateStep(index, { strategy_type: event.target.value })}
                    options={STRATEGY_OPTIONS}
                  />
                </div>
                <div className="grid gap-2">
                  <Label htmlFor={`mst-step-dte-${index}`}>Target DTE</Label>
                  <Input
                    id={`mst-step-dte-${index}`}
                    value={step.target_dte}
                    onChange={(event) => updateStep(index, { target_dte: event.target.value })}
                  />
                </div>
                <div className="grid gap-2">
                  <Label htmlFor={`mst-step-hold-${index}`}>Max holding days</Label>
                  <Input
                    id={`mst-step-hold-${index}`}
                    value={step.max_holding_days}
                    onChange={(event) => updateStep(index, { max_holding_days: event.target.value })}
                  />
                </div>
                <div className="grid gap-2">
                  <Label htmlFor={`mst-step-tolerance-${index}`}>DTE tolerance</Label>
                  <Input
                    id={`mst-step-tolerance-${index}`}
                    value={step.dte_tolerance_days}
                    onChange={(event) => updateStep(index, { dte_tolerance_days: event.target.value })}
                  />
                </div>
                <div className="grid gap-2">
                  <Label htmlFor={`mst-step-days-${index}`}>Days after prior step</Label>
                  <Input
                    id={`mst-step-days-${index}`}
                    value={step.days_after_prior_step}
                    onChange={(event) => updateStep(index, { days_after_prior_step: event.target.value })}
                    disabled={step.triggerMode !== "date_offset"}
                  />
                </div>
                <div className="grid gap-2">
                  <Label htmlFor={`mst-step-status-${index}`}>Required prior status</Label>
                  <Select
                    id={`mst-step-status-${index}`}
                    value={step.require_prior_step_status}
                    onChange={(event) => updateStep(index, { require_prior_step_status: event.target.value as EditableStep["require_prior_step_status"] })}
                    options={[
                      { value: "", label: "None" },
                      { value: "filled", label: "Filled" },
                      { value: "expired", label: "Expired" },
                      { value: "closed", label: "Closed" },
                    ]}
                  />
                </div>
              </div>

              {step.triggerMode === "rule_match" || step.triggerMode === "event_and_rule" ? (
                <div className="space-y-3">
                  <div className="flex items-center justify-between gap-4">
                    <p className="text-sm font-semibold uppercase tracking-[0.16em] text-muted-foreground">Trigger rules</p>
                    <Button
                      type="button"
                      variant="outline"
                      onClick={() =>
                        setSteps((current) =>
                          current.map((item, itemIndex) =>
                            itemIndex === index ? { ...item, triggerRules: [...item.triggerRules, createIndicatorRule()] } : item,
                          ),
                        )
                      }
                    >
                      <Plus className="h-4 w-4" />
                      Add trigger rule
                    </Button>
                  </div>
                  {step.triggerRules.map((rule, ruleIndex) => (
                    <div key={`step-${index}-rule-${ruleIndex}`} className="grid gap-3 rounded-xl border border-border/60 p-3 lg:grid-cols-[1fr_100px_1fr_1fr_auto]">
                      <Select
                        value={rule.type}
                        onChange={(event) => updateStepRule(index, ruleIndex, { type: event.target.value })}
                        options={RULE_TYPE_OPTIONS}
                      />
                      <Select
                        value={rule.operator}
                        onChange={(event) => updateStepRule(index, ruleIndex, { operator: event.target.value })}
                        options={OPERATOR_OPTIONS}
                      />
                      <Input value={rule.threshold} onChange={(event) => updateStepRule(index, ruleIndex, { threshold: event.target.value })} />
                      <Input value={rule.period} onChange={(event) => updateStepRule(index, ruleIndex, { period: event.target.value })} />
                      <Button
                        type="button"
                        variant="ghost"
                        onClick={() =>
                          setSteps((current) =>
                            current.map((item, itemIndex) =>
                              itemIndex === index
                                ? { ...item, triggerRules: item.triggerRules.filter((_, currentRuleIndex) => currentRuleIndex !== ruleIndex) }
                                : item,
                            ),
                          )
                        }
                        disabled={step.triggerRules.length <= 1}
                      >
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    </div>
                  ))}
                </div>
              ) : null}
            </div>
          ))}

          <Button type="button" variant="outline" onClick={() => setSteps((current) => [...current, createStep("New step", "roll", "date_offset")])}>
            <Plus className="h-4 w-4" />
            Add step
          </Button>
        </CardContent>
      </Card>

      <Button type="submit" disabled={status === "submitting"}>
        {status === "submitting" ? (
          <>
            <Loader2 className="h-4 w-4 animate-spin" />
            Creating multi-step run...
          </>
        ) : (
          "Create multi-step run"
        )}
      </Button>
    </form>
  );
}
