import type { PlanTier, ScannerMode } from "@backtestforecast/api-client";
import { currentEasternDate } from "@/lib/utils";
import { ACCOUNT_SIZE_MIN, TICKER_RE } from "@/lib/validation-constants";
import {
  MIN_SCANNER_WINDOW_DAYS,
  getScannerWindowTooShortError,
} from "@/lib/scanner/constants";

export interface ScannerFormInput {
  mode: ScannerMode;
  symbolsText: string;
  selectedStrategies: Set<string>;
  maxScannerWindowDays?: number;
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

interface ScannerLimits {
  maxSymbols: number;
  maxStrategies: number;
  maxRecommendations: number;
}

const SCANNER_LIMITS: Record<string, ScannerLimits> = {
  "pro:basic":        { maxSymbols: 5,  maxStrategies: 4,  maxRecommendations: 10 },
  "premium:basic":    { maxSymbols: 10, maxStrategies: 6,  maxRecommendations: 15 },
  "premium:advanced": { maxSymbols: 25, maxStrategies: 14, maxRecommendations: 30 },
};

export function getScannerSupportError(planTier: PlanTier, mode: ScannerMode): string | null {
  if (SCANNER_LIMITS[`${planTier}:${mode}`]) {
    return null;
  }
  if (planTier === "free") {
    return "Scanner access requires Pro or Premium.";
  }
  if (planTier === "pro" && mode === "advanced") {
    return "Advanced scanner access requires Premium.";
  }
  return "Scanner access is not available for the current entitlement.";
}

export function getScannerLimits(planTier: PlanTier, mode: ScannerMode): ScannerLimits {
  const direct = SCANNER_LIMITS[`${planTier}:${mode}`];
  if (direct) return direct;
  return SCANNER_LIMITS["pro:basic"];
}

export function parseSymbols(text: string): string[] {
  const raw = text
    .split(/[,\s]+/)
    .map((s) => s.trim().toUpperCase())
    .filter(Boolean);
  return [...new Set(raw)];
}

export function validateScannerForm(input: ScannerFormInput, planTier: PlanTier = "pro"): string[] {
  const symbols = parseSymbols(input.symbolsText);
  const errors: string[] = [];
  const supportError = getScannerSupportError(planTier, input.mode);

  if (symbols.length === 0) {
    errors.push("At least one symbol is required.");
  }
  const invalid = symbols.filter((s) => !TICKER_RE.test(s));
  if (invalid.length > 0) {
    errors.push(`Invalid ticker format: ${invalid.slice(0, 3).join(", ")}${invalid.length > 3 ? "…" : ""}`);
  }
  if (input.selectedStrategies.size === 0) {
    errors.push("At least one strategy type is required.");
  }
  if (supportError) {
    errors.push(supportError);
  }

  const limits = getScannerLimits(planTier, input.mode);
  if (symbols.length > limits.maxSymbols) {
    errors.push(
      `${input.mode === "advanced" ? "Advanced" : "Basic"} mode allows at most ${limits.maxSymbols} symbols for your plan.`,
    );
  }
  if (input.selectedStrategies.size > limits.maxStrategies) {
    errors.push(
      `${input.mode === "advanced" ? "Advanced" : "Basic"} mode allows at most ${limits.maxStrategies} strategies for your plan.`,
    );
  }

  if (!input.startDate) {
    errors.push("Start date is required.");
  }
  if (!input.endDate) {
    errors.push("End date is required.");
  }
  if (
    input.startDate &&
    input.endDate &&
    new Date(input.startDate) >= new Date(input.endDate)
  ) {
    errors.push("Start date must be before end date.");
  }
  const todayEt = currentEasternDate();
  if (input.endDate && input.endDate > todayEt) {
    errors.push("End date cannot be in the future (US Eastern time).");
  }
  if (input.startDate && input.startDate > todayEt) {
    errors.push("Start date cannot be in the future (US Eastern time).");
  }
  if (input.startDate && input.endDate && errors.length === 0) {
    const [sy, sm, sd] = input.startDate.split("-").map(Number);
    const [ey, em, ed] = input.endDate.split("-").map(Number);
    const startUtc = Date.UTC(sy, sm - 1, sd);
    const endUtc = Date.UTC(ey, em - 1, ed);
    const diffDays = (endUtc - startUtc) / (1000 * 60 * 60 * 24);

    if (diffDays < MIN_SCANNER_WINDOW_DAYS) {
      errors.push(getScannerWindowTooShortError());
    }
    if (input.maxScannerWindowDays !== undefined && diffDays > input.maxScannerWindowDays) {
      errors.push(`Scanner window cannot exceed ${input.maxScannerWindowDays} days.`);
    }
  }

  const numericChecks: Array<{
    label: string;
    value: number;
    min: number;
    max?: number;
    exclusiveMin?: boolean;
    integer?: boolean;
  }> = [
    { label: "Target DTE", value: Number(input.targetDte), min: 1, max: 365, integer: true },
    { label: "DTE tolerance", value: Number(input.dteTolerance), min: 0, max: 60, integer: true },
    { label: "Max holding days", value: Number(input.maxHolding), min: 1, max: 120, integer: true },
    { label: "Account size", value: Number(input.accountSize), min: ACCOUNT_SIZE_MIN, max: 100_000_000 },
    { label: "Risk %", value: Number(input.riskPct), min: 0, max: 100, exclusiveMin: true },
    { label: "Commission", value: Number(input.commission), min: 0, max: 100 },
    { label: "Max recommendations", value: Number(input.maxRecs), min: 1, max: limits.maxRecommendations, integer: true },
  ];
  for (const check of numericChecks) {
    const tooLow = check.exclusiveMin ? check.value <= check.min : check.value < check.min;
    if (
      !Number.isFinite(check.value) ||
      tooLow ||
      (check.max !== undefined && check.value > check.max)
    ) {
      errors.push(
        `${check.label} must be ${check.exclusiveMin ? "greater than" : "at least"} ${check.min}${check.max !== undefined ? ` and at most ${check.max}` : ""}.`,
      );
    } else if (check.integer && !Number.isInteger(check.value)) {
      errors.push(`${check.label} must be a whole number.`);
    }
  }

  const dteToleranceNum = Number(input.dteTolerance);
  const targetDteNum = Number(input.targetDte);
  if (
    Number.isFinite(dteToleranceNum) &&
    Number.isFinite(targetDteNum) &&
    dteToleranceNum >= targetDteNum
  ) {
    errors.push("DTE tolerance must be less than target DTE.");
  }

  return errors;
}


export type { PlanTier };
