import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

interface RiskValues {
  accountSize: string;
  riskPerTradePct: string;
  commissionPerContract: string;
  slippagePct: string;
  profitTargetEnabled: boolean;
  profitTargetPct: string;
  stopLossEnabled: boolean;
  stopLossPct: string;
  riskFreeRate: string;
}

interface RiskErrors {
  accountSize?: string;
  riskPerTradePct?: string;
  commissionPerContract?: string;
  slippagePct?: string;
  profitTargetPct?: string;
  stopLossPct?: string;
  riskFreeRate?: string;
}

export function RiskControls({
  values,
  errors,
  onChange,
}: {
  values: RiskValues;
  errors: RiskErrors;
  onChange: (patch: Partial<RiskValues>) => void;
}) {
  return (
    <div className="grid gap-4 sm:grid-cols-3">
      <div className="space-y-2">
        <Label htmlFor="accountSize">Account size</Label>
        <Input
          id="accountSize"
          inputMode="decimal"
          value={values.accountSize}
          aria-invalid={!!errors.accountSize}
          aria-describedby={errors.accountSize ? "accountSize-error" : undefined}
          onChange={(event) => onChange({ accountSize: event.target.value })}
        />
        <p className="text-xs text-muted-foreground">The starting capital for the run.</p>
        {errors.accountSize ? <p id="accountSize-error" className="text-sm text-destructive">{errors.accountSize}</p> : null}
      </div>

      <div className="space-y-2">
        <Label htmlFor="riskPerTradePct">Risk per trade %</Label>
        <Input
          id="riskPerTradePct"
          inputMode="decimal"
          value={values.riskPerTradePct}
          aria-invalid={!!errors.riskPerTradePct}
          aria-describedby={errors.riskPerTradePct ? "riskPerTradePct-error" : undefined}
          onChange={(event) => onChange({ riskPerTradePct: event.target.value })}
        />
        <p className="text-xs text-muted-foreground">Greater than 0 and up to 100.</p>
        {errors.riskPerTradePct ? (
          <p id="riskPerTradePct-error" className="text-sm text-destructive">{errors.riskPerTradePct}</p>
        ) : null}
      </div>

      <div className="space-y-2">
        <Label htmlFor="commissionPerContract">Commission / contract</Label>
        <Input
          id="commissionPerContract"
          inputMode="decimal"
          value={values.commissionPerContract}
          aria-invalid={!!errors.commissionPerContract}
          aria-describedby={errors.commissionPerContract ? "commissionPerContract-error" : undefined}
          onChange={(event) => onChange({ commissionPerContract: event.target.value })}
        />
        <p className="text-xs text-muted-foreground">
          Schwab-style online pricing is typically $0.65/contract. Buy-to-close orders at $0.05 or less and assignment/expiration exits are modeled with zero option commission.
        </p>
        {errors.commissionPerContract ? (
          <p id="commissionPerContract-error" className="text-sm text-destructive">{errors.commissionPerContract}</p>
        ) : null}
      </div>

      <div className="space-y-2">
        <Label htmlFor="slippagePct">Slippage %</Label>
        <Input
          id="slippagePct"
          inputMode="decimal"
          value={values.slippagePct}
          aria-invalid={!!errors.slippagePct}
          onChange={(event) => onChange({ slippagePct: event.target.value })}
        />
        <p className="text-xs text-muted-foreground">Simulated slippage on entry and exit (0–5%).</p>
        {errors.slippagePct ? <p className="text-sm text-destructive">{errors.slippagePct}</p> : null}
      </div>

      <div className="space-y-2">
        <Label htmlFor="riskFreeRate">Risk-free rate</Label>
        <Input
          id="riskFreeRate"
          inputMode="decimal"
          value={values.riskFreeRate}
          aria-invalid={!!errors.riskFreeRate}
          onChange={(event) => onChange({ riskFreeRate: event.target.value })}
        />
        <p className="text-xs text-muted-foreground">Annualized rate for Sharpe/Sortino (e.g. 0.045 = 4.5%). Set to 0 for ZIRP-era backtests.</p>
        {errors.riskFreeRate ? <p className="text-sm text-destructive">{errors.riskFreeRate}</p> : null}
      </div>

      <div className="space-y-2">
        <label className="inline-flex items-center gap-2 text-sm font-medium">
          <input
            checked={values.profitTargetEnabled}
            className="h-4 w-4 rounded border-input"
            type="checkbox"
            onChange={(event) => onChange({ profitTargetEnabled: event.target.checked })}
          />
          Profit target
        </label>
        {values.profitTargetEnabled ? (
          <>
            <Input
              id="profitTargetPct"
              inputMode="decimal"
              value={values.profitTargetPct}
              aria-invalid={!!errors.profitTargetPct}
              onChange={(event) => onChange({ profitTargetPct: event.target.value })}
            />
            <p className="text-xs text-muted-foreground">Close position at this % of capital at risk (1–500%).</p>
            {errors.profitTargetPct ? <p className="text-sm text-destructive">{errors.profitTargetPct}</p> : null}
          </>
        ) : null}
      </div>

      <div className="space-y-2">
        <label className="inline-flex items-center gap-2 text-sm font-medium">
          <input
            checked={values.stopLossEnabled}
            className="h-4 w-4 rounded border-input"
            type="checkbox"
            onChange={(event) => onChange({ stopLossEnabled: event.target.checked })}
          />
          Stop loss
        </label>
        {values.stopLossEnabled ? (
          <>
            <Input
              id="stopLossPct"
              inputMode="decimal"
              value={values.stopLossPct}
              aria-invalid={!!errors.stopLossPct}
              onChange={(event) => onChange({ stopLossPct: event.target.value })}
            />
            <p className="text-xs text-muted-foreground">Close position at this % loss of capital at risk (1–100%).</p>
            {errors.stopLossPct ? <p className="text-sm text-destructive">{errors.stopLossPct}</p> : null}
          </>
        ) : null}
      </div>
    </div>
  );
}
