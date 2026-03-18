"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { Loader2 } from "lucide-react";
import { createSweepJob } from "@/lib/api/client";
import { ApiError } from "@/lib/api/shared";
import { daysAgo } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

const STRATEGY_OPTIONS = [
  { value: "bull_put_credit_spread", label: "Bull Put Credit Spread" },
  { value: "bear_call_credit_spread", label: "Bear Call Credit Spread" },
  { value: "iron_condor", label: "Iron Condor" },
  { value: "iron_butterfly", label: "Iron Butterfly" },
  { value: "covered_call", label: "Covered Call" },
  { value: "cash_secured_put", label: "Cash-Secured Put" },
  { value: "long_call", label: "Long Call" },
  { value: "long_put", label: "Long Put" },
  { value: "short_straddle", label: "Short Straddle" },
  { value: "short_strangle", label: "Short Strangle" },
] as const;

const LEG_COUNT_OPTIONS = [2, 3, 4, 5, 6, 8] as const;

type SweepMode = "grid" | "genetic";

interface FormState {
  mode: SweepMode;
  symbol: string;
  startDate: string;
  endDate: string;
  targetDte: string;
  dteTolerance: string;
  maxHoldingDays: string;
  accountSize: string;
  riskPct: string;
  commission: string;
  slippage: string;
  deltas: string;
  numLegs: number;
  populationSize: string;
  maxGenerations: string;
  mutationRate: string;
  crossoverRate: string;
  maxResults: string;
}

export function SweepForm() {
  const router = useRouter();
  const { getToken } = useAuth();
  const [status, setStatus] = useState<"idle" | "submitting" | "error">("idle");
  const [errorMessage, setErrorMessage] = useState("");
  const [selectedStrategies, setSelectedStrategies] = useState<Set<string>>(
    new Set(["bull_put_credit_spread", "bear_call_credit_spread"]),
  );

  const [form, setForm] = useState<FormState>({
    mode: "grid",
    symbol: "TSLA",
    startDate: daysAgo(365),
    endDate: daysAgo(1),
    targetDte: "8",
    dteTolerance: "2",
    maxHoldingDays: "8",
    accountSize: "10000",
    riskPct: "5",
    commission: "0.65",
    slippage: "0",
    deltas: "16, 20, 25, 30, 35, 40, 45",
    numLegs: 2,
    populationSize: "100",
    maxGenerations: "30",
    mutationRate: "0.3",
    crossoverRate: "0.7",
    maxResults: "20",
  });

  const update = (field: keyof FormState, value: string | number) =>
    setForm((prev) => ({ ...prev, [field]: value }));

  const toggleStrategy = (val: string) => {
    setSelectedStrategies((prev) => {
      const next = new Set(prev);
      if (next.has(val)) next.delete(val);
      else next.add(val);
      return next;
    });
  };

  const abortRef = useRef<AbortController | null>(null);
  const submittingRef = useRef(false);

  useEffect(() => {
    return () => {
      abortRef.current?.abort();
    };
  }, []);

  const handleSubmit = useCallback(async (e: React.FormEvent) => {
    e.preventDefault();
    if (submittingRef.current) return;

    const symbol = form.symbol.trim().toUpperCase();
    if (!symbol || !/^[A-Z][A-Z0-9./^-]{0,15}$/.test(symbol)) {
      setStatus("error");
      setErrorMessage("Enter a valid ticker symbol (1-16 characters, letters/digits/./-/^).");
      return;
    }
    if (!form.startDate || !form.endDate || form.startDate >= form.endDate) {
      setStatus("error");
      setErrorMessage("Start date must be before end date.");
      return;
    }
    const accountSize = Number(form.accountSize);
    const riskPct = Number(form.riskPct);
    const commission = Number(form.commission);
    if (!Number.isFinite(accountSize) || accountSize <= 0) {
      setStatus("error");
      setErrorMessage("Account size must be a positive number.");
      return;
    }
    if (!Number.isFinite(riskPct) || riskPct <= 0 || riskPct > 100) {
      setStatus("error");
      setErrorMessage("Risk per trade must be between 0 and 100%.");
      return;
    }
    if (!Number.isFinite(commission) || commission < 0) {
      setStatus("error");
      setErrorMessage("Commission must be zero or a positive number.");
      return;
    }
    const targetDte = Number(form.targetDte);
    if (!Number.isFinite(targetDte) || targetDte < 1 || targetDte > 365) {
      setStatus("error");
      setErrorMessage("Target DTE must be an integer between 1 and 365.");
      return;
    }
    const dteTolerance = Number(form.dteTolerance);
    if (!Number.isFinite(dteTolerance) || dteTolerance < 0 || dteTolerance > 60) {
      setStatus("error");
      setErrorMessage("DTE tolerance must be between 0 and 60.");
      return;
    }
    const maxHoldingDays = Number(form.maxHoldingDays);
    if (!Number.isFinite(maxHoldingDays) || maxHoldingDays < 1 || maxHoldingDays > 120) {
      setStatus("error");
      setErrorMessage("Max holding days must be between 1 and 120.");
      return;
    }
    const slippage = Number(form.slippage);
    if (!Number.isFinite(slippage) || slippage < 0 || slippage > 5) {
      setStatus("error");
      setErrorMessage("Slippage must be between 0 and 5%.");
      return;
    }
    const maxResults = Number(form.maxResults);
    if (!Number.isFinite(maxResults) || maxResults < 1 || maxResults > 100) {
      setStatus("error");
      setErrorMessage("Max results must be between 1 and 100.");
      return;
    }
    if (form.mode === "genetic") {
      const populationSize = Number(form.populationSize);
      if (!Number.isFinite(populationSize) || populationSize < 20 || populationSize > 500) {
        setStatus("error");
        setErrorMessage("Population size must be between 20 and 500.");
        return;
      }
      const maxGenerations = Number(form.maxGenerations);
      if (!Number.isFinite(maxGenerations) || maxGenerations < 5 || maxGenerations > 200) {
        setStatus("error");
        setErrorMessage("Max generations must be between 5 and 200.");
        return;
      }
      const mutationRate = Number(form.mutationRate);
      if (!Number.isFinite(mutationRate) || mutationRate < 0.05 || mutationRate > 1) {
        setStatus("error");
        setErrorMessage("Mutation rate must be between 0.05 and 1.");
        return;
      }
      const crossoverRate = Number(form.crossoverRate);
      if (!Number.isFinite(crossoverRate) || crossoverRate < 0.1 || crossoverRate > 1) {
        setStatus("error");
        setErrorMessage("Crossover rate must be between 0.1 and 1.");
        return;
      }
    }
    if (form.mode === "grid" && selectedStrategies.size === 0) {
      setStatus("error");
      setErrorMessage("Select at least one strategy for the grid sweep.");
      return;
    }

    submittingRef.current = true;
    setStatus("submitting");
    setErrorMessage("");

    const controller = new AbortController();
    abortRef.current = controller;

    try {
      const token = await getToken();
      if (!token) throw new Error("Authentication required.");

      const payload: Record<string, unknown> = {
        mode: form.mode,
        symbol,
        start_date: form.startDate,
        end_date: form.endDate,
        target_dte: Number(form.targetDte),
        dte_tolerance_days: Number(form.dteTolerance),
        max_holding_days: Number(form.maxHoldingDays),
        account_size: accountSize,
        risk_per_trade_pct: riskPct,
        commission_per_contract: commission,
        slippage_pct: Number(form.slippage),
        max_results: Number(form.maxResults),
        entry_rule_sets: [{ name: "no_filter", entry_rules: [] }],
      };

      if (form.mode === "grid") {
        payload.strategy_types = Array.from(selectedStrategies);
        const deltaParts = form.deltas.split(",").map((s) => s.trim()).filter(Boolean);
        for (const part of deltaParts) {
          if (!Number.isFinite(Number(part))) {
            setStatus("error");
            setErrorMessage(`Invalid delta grid entry: "${part}". All entries must be valid numbers.`);
            submittingRef.current = false;
            return;
          }
        }
        const deltas = deltaParts.map((s) => ({ value: Number(s) }));
        if (deltas.length > 0) payload.delta_grid = deltas;
      } else {
        const legType = `custom_${form.numLegs}_leg`;
        payload.strategy_types = [legType];
        payload.genetic_config = {
          num_legs: form.numLegs,
          population_size: Number(form.populationSize),
          max_generations: Number(form.maxGenerations),
          mutation_rate: Number(form.mutationRate),
          crossover_rate: Number(form.crossoverRate),
          max_workers: 10,
        };
      }

      const job = await createSweepJob(token, payload, controller.signal);
      router.replace(`/app/sweeps/${job.id}`);
    } catch (err) {
      if (controller.signal.aborted) return;
      setStatus("error");
      if (err instanceof ApiError) {
        setErrorMessage(err.message);
      } else {
        setErrorMessage(err instanceof Error ? err.message : "An unexpected error occurred.");
      }
    } finally {
      submittingRef.current = false;
    }
  }, [form, selectedStrategies, getToken, router]);

  return (
    <form onSubmit={handleSubmit} className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Sweep mode</CardTitle>
          <CardDescription>Grid sweeps test all parameter combinations. Genetic uses evolutionary optimization for custom leg strategies.</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex gap-4">
            <Button
              type="button"
              variant={form.mode === "grid" ? "default" : "outline"}
              onClick={() => update("mode", "grid")}
            >
              Grid sweep
            </Button>
            <Button
              type="button"
              variant={form.mode === "genetic" ? "default" : "outline"}
              onClick={() => update("mode", "genetic")}
            >
              Genetic optimizer
            </Button>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Symbol and timeframe</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          <div className="space-y-2">
            <Label htmlFor="symbol">Symbol</Label>
            <Input id="symbol" value={form.symbol} onChange={(e) => update("symbol", e.target.value)} />
          </div>
          <div className="space-y-2">
            <Label htmlFor="startDate">Start date</Label>
            <Input id="startDate" type="date" value={form.startDate} onChange={(e) => update("startDate", e.target.value)} />
          </div>
          <div className="space-y-2">
            <Label htmlFor="endDate">End date</Label>
            <Input id="endDate" type="date" value={form.endDate} onChange={(e) => update("endDate", e.target.value)} />
          </div>
          <div className="space-y-2">
            <Label htmlFor="targetDte">Target DTE</Label>
            <Input id="targetDte" type="number" min={0} value={form.targetDte} onChange={(e) => update("targetDte", e.target.value)} />
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Risk and sizing</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4 sm:grid-cols-2 lg:grid-cols-5">
          <div className="space-y-2">
            <Label htmlFor="dteTolerance">DTE tolerance</Label>
            <Input id="dteTolerance" type="number" min={0} value={form.dteTolerance} onChange={(e) => update("dteTolerance", e.target.value)} />
          </div>
          <div className="space-y-2">
            <Label htmlFor="maxHoldingDays">Max holding days</Label>
            <Input id="maxHoldingDays" type="number" min={1} value={form.maxHoldingDays} onChange={(e) => update("maxHoldingDays", e.target.value)} />
          </div>
          <div className="space-y-2">
            <Label htmlFor="accountSize">Account size</Label>
            <Input id="accountSize" type="number" min={1} value={form.accountSize} onChange={(e) => update("accountSize", e.target.value)} />
          </div>
          <div className="space-y-2">
            <Label htmlFor="riskPct">Risk per trade %</Label>
            <Input id="riskPct" type="number" min={0.1} step={0.1} value={form.riskPct} onChange={(e) => update("riskPct", e.target.value)} />
          </div>
          <div className="space-y-2">
            <Label htmlFor="commission">Commission/contract</Label>
            <Input id="commission" type="number" min={0} step={0.01} value={form.commission} onChange={(e) => update("commission", e.target.value)} />
          </div>
        </CardContent>
      </Card>

      {form.mode === "grid" ? (
        <>
          <Card>
            <CardHeader>
              <CardTitle>Strategies</CardTitle>
              <CardDescription>Select strategies to include in the sweep.</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
                {STRATEGY_OPTIONS.map((s) => (
                  <label key={s.value} className="flex items-center gap-2 text-sm cursor-pointer">
                    <input
                      type="checkbox"
                      checked={selectedStrategies.has(s.value)}
                      onChange={() => toggleStrategy(s.value)}
                      className="h-4 w-4 rounded border-border"
                    />
                    {s.label}
                  </label>
                ))}
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Delta grid</CardTitle>
              <CardDescription>Comma-separated delta values to test (e.g., 16, 25, 30, 45).</CardDescription>
            </CardHeader>
            <CardContent>
              <Input value={form.deltas} onChange={(e) => update("deltas", e.target.value)} placeholder="16, 25, 30, 45" />
            </CardContent>
          </Card>
        </>
      ) : (
        <Card>
          <CardHeader>
            <CardTitle>Genetic optimizer settings</CardTitle>
            <CardDescription>Configure the evolutionary search for custom leg strategies.</CardDescription>
          </CardHeader>
          <CardContent className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            <div className="space-y-2">
              <Label htmlFor="numLegs">Number of legs</Label>
              <select
                id="numLegs"
                className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                value={form.numLegs}
                onChange={(e) => update("numLegs", Number(e.target.value))}
              >
                {LEG_COUNT_OPTIONS.map((n) => (
                  <option key={n} value={n}>{n} legs</option>
                ))}
              </select>
            </div>
            <div className="space-y-2">
              <Label htmlFor="populationSize">Population size</Label>
              <Input id="populationSize" type="number" min={20} max={500} value={form.populationSize} onChange={(e) => update("populationSize", e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="maxGenerations">Max generations</Label>
              <Input id="maxGenerations" type="number" min={5} max={200} value={form.maxGenerations} onChange={(e) => update("maxGenerations", e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="mutationRate">Mutation rate</Label>
              <Input id="mutationRate" type="number" min={0.05} max={1} step={0.05} value={form.mutationRate} onChange={(e) => update("mutationRate", e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="crossoverRate">Crossover rate</Label>
              <Input id="crossoverRate" type="number" min={0.1} max={1} step={0.05} value={form.crossoverRate} onChange={(e) => update("crossoverRate", e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="maxResults">Max results to keep</Label>
              <Input id="maxResults" type="number" min={1} max={100} value={form.maxResults} onChange={(e) => update("maxResults", e.target.value)} />
            </div>
          </CardContent>
        </Card>
      )}

      {status === "error" ? (
        <Card>
          <CardContent className="p-4 text-destructive text-sm">{errorMessage}</CardContent>
        </Card>
      ) : null}

      <Button type="submit" disabled={status === "submitting"} className="w-full sm:w-auto">
        {status === "submitting" ? (
          <>
            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            Creating sweep...
          </>
        ) : (
          "Run sweep"
        )}
      </Button>
    </form>
  );
}
