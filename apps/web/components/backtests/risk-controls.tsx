import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

interface RiskValues {
  accountSize: string;
  riskPerTradePct: string;
  commissionPerContract: string;
}

interface RiskErrors {
  accountSize?: string;
  riskPerTradePct?: string;
  commissionPerContract?: string;
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
        <p className="text-xs text-muted-foreground">Use the all-in per-contract commission estimate.</p>
        {errors.commissionPerContract ? (
          <p id="commissionPerContract-error" className="text-sm text-destructive">{errors.commissionPerContract}</p>
        ) : null}
      </div>
    </div>
  );
}
