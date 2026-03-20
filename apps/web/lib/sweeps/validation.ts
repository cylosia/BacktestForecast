import type { StrategyType, SweepMode } from "@backtestforecast/api-client";
import { daysAgoET } from "@/lib/utils";
import {
  ACCOUNT_SIZE_MAX,
  ACCOUNT_SIZE_MIN,
  DTE_TOLERANCE_MAX,
  MAX_BACKTEST_WINDOW_DAYS,
  MAX_HOLDING_DAYS_MAX,
  MAX_HOLDING_DAYS_MIN,
  SWEEP_MIN_WINDOW_DAYS,
  TARGET_DTE_MAX,
  TARGET_DTE_MIN,
  TICKER_RE,
} from "@/lib/validation-constants";

export interface SweepFormValues {
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

export interface SweepValidationResult {
  error?: string;
  symbol?: string;
  accountSize?: number;
  riskPct?: number;
  commission?: number;
}

export const DEFAULT_SWEEP_FORM_VALUES: SweepFormValues = {
  mode: "grid",
  symbol: "TSLA",
  startDate: daysAgoET(365),
  endDate: daysAgoET(1),
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
};

export function validateSweepForm(
  form: SweepFormValues,
  selectedStrategies: Set<StrategyType>,
): SweepValidationResult {
  const symbol = form.symbol.trim().toUpperCase();
  if (!symbol || !TICKER_RE.test(symbol)) {
    return {
      error:
        "Enter a valid ticker symbol (1-16 characters, letters/digits/./-/^).",
    };
  }
  if (!form.startDate || !form.endDate || form.startDate >= form.endDate) {
    return { error: "Start date must be before end date." };
  }

  const diffMs =
    new Date(form.endDate).getTime() - new Date(form.startDate).getTime();
  const diffDays = diffMs / (1000 * 60 * 60 * 24);
  if (diffDays < SWEEP_MIN_WINDOW_DAYS) {
    return {
      error: `Date range must be at least ${SWEEP_MIN_WINDOW_DAYS} days for sweeps.`,
    };
  }
  if (diffDays > MAX_BACKTEST_WINDOW_DAYS) {
    return {
      error: `Date range cannot exceed 5 years (${MAX_BACKTEST_WINDOW_DAYS} days).`,
    };
  }
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  if (new Date(form.endDate) > today) {
    return { error: "End date cannot be in the future." };
  }

  const accountSize = Number(form.accountSize);
  const riskPct = Number(form.riskPct);
  const commission = Number(form.commission);
  if (
    !Number.isFinite(accountSize) ||
    accountSize < ACCOUNT_SIZE_MIN ||
    accountSize > ACCOUNT_SIZE_MAX
  ) {
    return {
      error: `Account size must be between ${ACCOUNT_SIZE_MIN.toLocaleString()} and ${ACCOUNT_SIZE_MAX.toLocaleString()}.`,
    };
  }
  if (!Number.isFinite(riskPct) || riskPct <= 0 || riskPct > 100) {
    return { error: "Risk per trade must be between 0 and 100%." };
  }
  if (!Number.isFinite(commission) || commission < 0 || commission > 100) {
    return { error: "Commission must be between 0 and 100." };
  }
  const targetDte = Number(form.targetDte);
  if (
    !Number.isFinite(targetDte) ||
    !Number.isInteger(targetDte) ||
    targetDte < TARGET_DTE_MIN ||
    targetDte > TARGET_DTE_MAX
  ) {
    return {
      error: `Target DTE must be an integer between ${TARGET_DTE_MIN} and ${TARGET_DTE_MAX}.`,
    };
  }
  const dteTolerance = Number(form.dteTolerance);
  if (
    !Number.isFinite(dteTolerance) ||
    dteTolerance < 0 ||
    dteTolerance > DTE_TOLERANCE_MAX
  ) {
    return {
      error: `DTE tolerance must be a whole number between 0 and ${DTE_TOLERANCE_MAX}.`,
    };
  }
  if (!Number.isInteger(dteTolerance)) {
    return { error: "DTE tolerance must be a whole number (no decimals)." };
  }
  if (dteTolerance >= targetDte) {
    return { error: "DTE tolerance must be less than target DTE." };
  }
  const maxHoldingDays = Number(form.maxHoldingDays);
  if (
    !Number.isFinite(maxHoldingDays) ||
    !Number.isInteger(maxHoldingDays) ||
    maxHoldingDays < MAX_HOLDING_DAYS_MIN ||
    maxHoldingDays > MAX_HOLDING_DAYS_MAX
  ) {
    return {
      error: `Max holding days must be between ${MAX_HOLDING_DAYS_MIN} and ${MAX_HOLDING_DAYS_MAX}.`,
    };
  }
  const slippage = Number(form.slippage);
  if (!Number.isFinite(slippage) || slippage < 0 || slippage > 5) {
    return { error: "Slippage must be between 0 and 5%." };
  }
  const maxResults = Number(form.maxResults);
  if (
    !Number.isFinite(maxResults) ||
    !Number.isInteger(maxResults) ||
    maxResults < 1 ||
    maxResults > 100
  ) {
    return { error: "Max results must be between 1 and 100." };
  }
  if (form.mode === "genetic") {
    const populationSize = Number(form.populationSize);
    if (
      !Number.isFinite(populationSize) ||
      !Number.isInteger(populationSize) ||
      populationSize < 20 ||
      populationSize > 500
    ) {
      return {
        error: "Population size must be an integer between 20 and 500.",
      };
    }
    const maxGenerations = Number(form.maxGenerations);
    if (
      !Number.isFinite(maxGenerations) ||
      !Number.isInteger(maxGenerations) ||
      maxGenerations < 5 ||
      maxGenerations > 200
    ) {
      return { error: "Max generations must be an integer between 5 and 200." };
    }
    const mutationRate = Number(form.mutationRate);
    if (
      !Number.isFinite(mutationRate) ||
      mutationRate < 0.05 ||
      mutationRate > 1
    ) {
      return { error: "Mutation rate must be between 0.05 and 1." };
    }
    const crossoverRate = Number(form.crossoverRate);
    if (
      !Number.isFinite(crossoverRate) ||
      crossoverRate < 0.1 ||
      crossoverRate > 1
    ) {
      return { error: "Crossover rate must be between 0.1 and 1." };
    }
  }
  if (form.mode === "grid" && selectedStrategies.size === 0) {
    return { error: "Select at least one strategy for the grid sweep." };
  }

  return { symbol, accountSize, riskPct, commission };
}
