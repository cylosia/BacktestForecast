import type { ScannerMode } from "@backtestforecast/api-client";

export interface ScannerFormInput {
  mode: ScannerMode;
  symbolsText: string;
  selectedStrategies: Set<string>;
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

const TICKER_RE = /^[A-Z][A-Z0-9./^-]{0,15}$/;

export function parseSymbols(text: string): string[] {
  return text
    .split(/[,\s]+/)
    .map((s) => s.trim().toUpperCase())
    .filter(Boolean);
}

export function validateScannerForm(input: ScannerFormInput): string[] {
  const symbols = parseSymbols(input.symbolsText);
  const errors: string[] = [];

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

  const maxSymbols = input.mode === "advanced" ? 25 : 5;
  const maxStrategies = input.mode === "advanced" ? 14 : 6;
  if (symbols.length > maxSymbols) {
    errors.push(
      `${input.mode === "advanced" ? "Advanced" : "Basic"} mode allows at most ${maxSymbols} symbols.`,
    );
  }
  if (input.selectedStrategies.size > maxStrategies) {
    errors.push(
      `${input.mode === "advanced" ? "Advanced" : "Basic"} mode allows at most ${maxStrategies} strategies.`,
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

  const numericChecks: Array<{
    label: string;
    value: number;
    min: number;
    max?: number;
    integer?: boolean;
  }> = [
    { label: "Target DTE", value: Number(input.targetDte), min: 7, max: 365, integer: true },
    { label: "DTE tolerance", value: Number(input.dteTolerance), min: 0, max: 60, integer: true },
    { label: "Max holding days", value: Number(input.maxHolding), min: 1, max: 120, integer: true },
    { label: "Account size", value: Number(input.accountSize), min: 100, max: 100_000_000 },
    { label: "Risk %", value: Number(input.riskPct), min: 0.1, max: 100 },
    { label: "Commission", value: Number(input.commission), min: 0, max: 1000 },
    { label: "Max recommendations", value: Number(input.maxRecs), min: 1, max: 30, integer: true },
  ];
  for (const check of numericChecks) {
    if (
      !Number.isFinite(check.value) ||
      check.value < check.min ||
      (check.max !== undefined && check.value > check.max)
    ) {
      errors.push(
        `${check.label} must be a number between ${check.min} and ${check.max ?? "∞"}.`,
      );
    } else if (check.integer && !Number.isInteger(check.value)) {
      errors.push(`${check.label} must be a whole number.`);
    }
  }

  if (Number(input.dteTolerance) >= Number(input.targetDte)) {
    errors.push("DTE tolerance must be less than target DTE.");
  }

  return errors;
}
