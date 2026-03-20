"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { AlertTriangle, Loader2 } from "lucide-react";
import { createBacktestRun } from "@/lib/api/client";
import { ApiError } from "@/lib/api/shared";
import type { BacktestQuota } from "@/lib/backtests/quota";
import type { StrategyCatalogGroup, TemplateResponse } from "@backtestforecast/api-client";
import {
  getDefaultBacktestFormValues,
  type BacktestFormErrors,
  type BacktestFormValues,
  validateBacktestForm,
} from "@/lib/backtests/validation";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { RiskControls } from "@/components/backtests/risk-controls";
import { StrategySelector } from "@/components/backtests/strategy-selector";
import { SymbolInput } from "@/components/backtests/symbol-input";
import { TaRuleControls } from "@/components/backtests/ta-rule-controls";
import { TimeframeControls } from "@/components/backtests/timeframe-controls";
import { TemplatePicker, templateToFormValues } from "@/components/templates/template-picker";
import { SaveAsTemplate } from "@/components/templates/save-as-template";
import { isPlanLimitError, UpgradePrompt } from "@/components/billing/upgrade-prompt";

export function BacktestForm({
  quota,
  templates = [],
  catalogGroups,
  initialTemplateId,
}: {
  quota: BacktestQuota;
  templates?: TemplateResponse[];
  catalogGroups?: StrategyCatalogGroup[];
  initialTemplateId?: string;
}) {
  const router = useRouter();
  const { getToken } = useAuth();
  const [values, setValues] = useState<BacktestFormValues>(() => getDefaultBacktestFormValues());
  const [errors, setErrors] = useState<BacktestFormErrors>({});
  const [status, setStatus] = useState<"idle" | "submitting" | "success" | "error">("idle");
  const [serverMessage, setServerMessage] = useState<string | null>(null);
  const [errorCode, setErrorCode] = useState<string | undefined>(undefined);
  const [requiredTier, setRequiredTier] = useState<string | undefined>(undefined);
  const [submittedCount, setSubmittedCount] = useState(0);
  const submitAbortRef = useRef<AbortController | null>(null);
  const submittingRef = useRef(false);
  const valuesRef = useRef(values);
  valuesRef.current = values;

  useEffect(() => {
    return () => { submitAbortRef.current?.abort(); };
  }, []);

  const templateAppliedRef = useRef(false);
  useEffect(() => {
    if (!initialTemplateId || templateAppliedRef.current) return;
    const match = templates.find((t) => t.id === initialTemplateId);
    if (match) {
      const patch = templateToFormValues(match);
      if (patch) {
        setValues((current) => ({ ...current, ...patch }));
        templateAppliedRef.current = true;
      }
    }
  }, [initialTemplateId, templates]);

  const effectiveUsed = quota.used + submittedCount;
  const effectiveRemaining = quota.remaining !== null ? Math.max(quota.remaining - submittedCount, 0) : null;
  const effectiveReached = quota.reached || (quota.limit !== null && effectiveUsed >= quota.limit);

  const submitDisabled = useMemo(() => effectiveReached || status === "submitting", [effectiveReached, status]);

  const handleSubmit = useCallback(async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    if (submittingRef.current) return;

    if (quota.reached) {
      setStatus("error");
      setErrorCode("quota_exceeded");
      setServerMessage(
        `${quota.tier.charAt(0).toUpperCase() + quota.tier.slice(1)} plan limit reached for this month. You have already used ${quota.used}${quota.limit !== null ? ` of ${quota.limit}` : ""}.`,
      );
      return;
    }

    const validation = validateBacktestForm(valuesRef.current);
    setErrors(validation.errors);

    if (!validation.payload) {
      setStatus("error");
      setErrorCode(undefined);
      setServerMessage(
        validation.errors.form ?? "Please fix the highlighted inputs and try again."
      );
      return;
    }

    setStatus("submitting");
    setServerMessage(null);
    setErrorCode(undefined);
    submittingRef.current = true;

    try {
      const token = await getToken();
      if (!token) {
        throw new Error("Your session token could not be loaded. Please sign in again.");
      }

      submitAbortRef.current?.abort();
      submitAbortRef.current = new AbortController();
      const payloadWithKey = { ...validation.payload, idempotency_key: crypto.randomUUID() };
      const run = await createBacktestRun(token, payloadWithKey, submitAbortRef.current.signal);
      setStatus("success");
      setServerMessage("Backtest queued. Opening run details...");
      setSubmittedCount((prev) => prev + 1);
      router.push(`/app/backtests/${run.id}`);
      router.refresh();
    } catch (error) {
      const message =
        error instanceof ApiError
          ? error.message
          : error instanceof Error
            ? error.message
            : "The backtest could not be created.";
      const code = error instanceof ApiError ? error.code : undefined;
      const reqTier = error instanceof ApiError ? error.requiredTier : undefined;

      setStatus("error");
      setServerMessage(message);
      setErrorCode(code);
      setRequiredTier(reqTier);
    } finally {
      submittingRef.current = false;
    }
  }, [quota, getToken, router]);

  const updateValues = useCallback((patch: Partial<BacktestFormValues>) => {
    setValues((current) => ({ ...current, ...patch }));
    setStatus((prev) => {
      if (prev === "error") {
        setServerMessage(null);
        setErrorCode(undefined);
        return "idle";
      }
      return prev;
    });
  }, []);

  return (
    <form className="space-y-6" noValidate onSubmit={handleSubmit} aria-label="Backtest configuration">
      <TemplatePicker templates={templates} onApply={updateValues} />

      {effectiveReached ? (
        <UpgradePrompt
          message={`This account has used ${effectiveUsed}${quota.limit !== null ? ` of ${quota.limit}` : ""} backtests this month. Upgrade for unlimited backtests.`}
          requiredTier="pro"
        />
      ) : effectiveRemaining !== null && effectiveRemaining <= 1 && effectiveRemaining > 0 ? (
        <div className="rounded-xl border border-amber-500/40 bg-amber-500/5 p-4 text-sm">
          <div className="flex items-start gap-3">
            <AlertTriangle className="mt-0.5 h-4 w-4 text-amber-600" />
            <div>
              <p className="font-medium text-amber-700 dark:text-amber-400">Last backtest on this plan</p>
              <p className="mt-1 text-amber-700/80 dark:text-amber-400/80">
                {effectiveUsed} of {quota.limit} used. After this run you will need to upgrade or wait until next month.
              </p>
            </div>
          </div>
        </div>
      ) : (
        <div className="rounded-xl border border-border/70 bg-muted/40 p-4 text-sm text-muted-foreground">
          <p>{quota.limit === null
            ? `${effectiveUsed} backtests used this month. This plan currently has no monthly cap.`
            : `${effectiveUsed} of ${quota.limit} monthly backtests used. ${effectiveRemaining ?? 0} remaining in the current month.`}</p>
          <p className="mt-1 text-xs opacity-70">Usage shown as of page load. Actual limits are enforced server-side.</p>
        </div>
      )}

      {serverMessage && isPlanLimitError(errorCode) ? (
        <UpgradePrompt message={serverMessage} requiredTier={requiredTier} />
      ) : serverMessage ? (
        <div
          id="backtest-form-feedback"
          role={status === "error" ? "alert" : "status"}
          className={`rounded-xl border p-4 text-sm ${
            status === "error"
              ? "border-destructive/40 bg-destructive/5 text-destructive"
              : status === "success"
                ? "border-emerald-500/30 bg-emerald-500/5 text-emerald-700 dark:text-emerald-400"
                : "border-border/70 bg-muted/40 text-muted-foreground"
          }`}
        >
          {serverMessage}
        </div>
      ) : null}

      <Card>
        <CardHeader>
          <CardTitle>Backtest setup</CardTitle>
          <CardDescription>
            Configure the underlying, strategy, and time window for this manual run.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="grid gap-4 lg:grid-cols-2">
            <SymbolInput
              error={errors.symbol}
              value={values.symbol}
              onChange={(symbol) => updateValues({ symbol })}
            />
            <StrategySelector
              error={errors.strategyType}
              value={values.strategyType}
              onChange={(strategyType) => updateValues({ strategyType })}
              catalogGroups={catalogGroups}
            />
          </div>

          <TimeframeControls
            errors={{
              startDate: errors.startDate,
              endDate: errors.endDate,
              targetDte: errors.targetDte,
              dteToleranceDays: errors.dteToleranceDays,
              maxHoldingDays: errors.maxHoldingDays,
            }}
            values={{
              startDate: values.startDate,
              endDate: values.endDate,
              targetDte: values.targetDte,
              dteToleranceDays: values.dteToleranceDays,
              maxHoldingDays: values.maxHoldingDays,
            }}
            onChange={updateValues}
          />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Technical analysis rules</CardTitle>
          <CardDescription>
            Configure any entry rules currently supported by the backend request schema.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <TaRuleControls
            errors={{
              rsiThreshold: errors.rsiThreshold,
              rsiPeriod: errors.rsiPeriod,
              fastPeriod: errors.fastPeriod,
              slowPeriod: errors.slowPeriod,
              macdFastPeriod: errors.macdFastPeriod,
              macdSlowPeriod: errors.macdSlowPeriod,
              macdSignalPeriod: errors.macdSignalPeriod,
              bollingerPeriod: errors.bollingerPeriod,
              bollingerStdDev: errors.bollingerStdDev,
              ivRankThreshold: errors.ivRankThreshold,
              ivRankLookbackDays: errors.ivRankLookbackDays,
              ivPercentileThreshold: errors.ivPercentileThreshold,
              ivPercentileLookbackDays: errors.ivPercentileLookbackDays,
              volumeSpikeMultiplier: errors.volumeSpikeMultiplier,
              volumeSpikePeriod: errors.volumeSpikePeriod,
              supportResistancePeriod: errors.supportResistancePeriod,
              supportResistanceTolerancePct: errors.supportResistanceTolerancePct,
              avoidEarningsDaysBefore: errors.avoidEarningsDaysBefore,
              avoidEarningsDaysAfter: errors.avoidEarningsDaysAfter,
            }}
            values={{
              rsiEnabled: values.rsiEnabled,
              rsiOperator: values.rsiOperator,
              rsiThreshold: values.rsiThreshold,
              rsiPeriod: values.rsiPeriod,
              movingAverageEnabled: values.movingAverageEnabled,
              movingAverageType: values.movingAverageType,
              fastPeriod: values.fastPeriod,
              slowPeriod: values.slowPeriod,
              crossoverDirection: values.crossoverDirection,
              macdEnabled: values.macdEnabled,
              macdFastPeriod: values.macdFastPeriod,
              macdSlowPeriod: values.macdSlowPeriod,
              macdSignalPeriod: values.macdSignalPeriod,
              macdDirection: values.macdDirection,
              bollingerEnabled: values.bollingerEnabled,
              bollingerPeriod: values.bollingerPeriod,
              bollingerStdDev: values.bollingerStdDev,
              bollingerBand: values.bollingerBand as "lower" | "middle" | "upper",
              bollingerOperator: values.bollingerOperator,
              ivRankEnabled: values.ivRankEnabled,
              ivRankOperator: values.ivRankOperator,
              ivRankThreshold: values.ivRankThreshold,
              ivRankLookbackDays: values.ivRankLookbackDays,
              ivPercentileEnabled: values.ivPercentileEnabled,
              ivPercentileOperator: values.ivPercentileOperator,
              ivPercentileThreshold: values.ivPercentileThreshold,
              ivPercentileLookbackDays: values.ivPercentileLookbackDays,
              volumeSpikeEnabled: values.volumeSpikeEnabled,
              volumeSpikeOperator: values.volumeSpikeOperator,
              volumeSpikeMultiplier: values.volumeSpikeMultiplier,
              volumeSpikePeriod: values.volumeSpikePeriod,
              supportResistanceEnabled: values.supportResistanceEnabled,
              supportResistanceMode: values.supportResistanceMode,
              supportResistancePeriod: values.supportResistancePeriod,
              supportResistanceTolerancePct: values.supportResistanceTolerancePct,
              avoidEarningsEnabled: values.avoidEarningsEnabled,
              avoidEarningsDaysBefore: values.avoidEarningsDaysBefore,
              avoidEarningsDaysAfter: values.avoidEarningsDaysAfter,
            }}
            onChange={updateValues}
          />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Risk and cost inputs</CardTitle>
          <CardDescription>
            These values map directly to backend pricing, exit, and performance-calculation inputs.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <RiskControls
            errors={{
              accountSize: errors.accountSize,
              riskPerTradePct: errors.riskPerTradePct,
              commissionPerContract: errors.commissionPerContract,
              slippagePct: errors.slippagePct,
              profitTargetPct: errors.profitTargetPct,
              stopLossPct: errors.stopLossPct,
              riskFreeRate: errors.riskFreeRate,
            }}
            values={{
              accountSize: values.accountSize,
              riskPerTradePct: values.riskPerTradePct,
              commissionPerContract: values.commissionPerContract,
              slippagePct: values.slippagePct,
              profitTargetEnabled: values.profitTargetEnabled,
              profitTargetPct: values.profitTargetPct,
              stopLossEnabled: values.stopLossEnabled,
              stopLossPct: values.stopLossPct,
              riskFreeRate: values.riskFreeRate,
            }}
            onChange={updateValues}
          />
        </CardContent>
      </Card>

      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex items-center gap-3">
          <p className="text-sm text-muted-foreground">
            Runs are immutable after submission.
          </p>
          <SaveAsTemplate values={values} />
        </div>
        <Button disabled={submitDisabled} size="lg" type="submit">
          {status === "submitting" ? (
            <>
              <Loader2 className="h-4 w-4 animate-spin" />
              Creating backtest...
            </>
          ) : (
            "Create backtest"
          )}
        </Button>
      </div>
    </form>
  );
}
