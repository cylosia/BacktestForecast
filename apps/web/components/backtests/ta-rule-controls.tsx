import type {
  ComparisonOperator,
  CrossoverDirection,
  MovingAverageRuleType,
} from "@backtestforecast/api-client";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";

interface TaRuleValues {
  rsiEnabled: boolean;
  rsiOperator: ComparisonOperator;
  rsiThreshold: string;
  rsiPeriod: string;
  movingAverageEnabled: boolean;
  movingAverageType: MovingAverageRuleType;
  fastPeriod: string;
  slowPeriod: string;
  crossoverDirection: CrossoverDirection;
}

interface TaRuleErrors {
  rsiThreshold?: string;
  rsiPeriod?: string;
  fastPeriod?: string;
  slowPeriod?: string;
}

export function TaRuleControls({
  values,
  errors,
  onChange,
}: {
  values: TaRuleValues;
  errors: TaRuleErrors;
  onChange: (patch: Partial<TaRuleValues>) => void;
}) {
  return (
    <div className="space-y-6">
      <div className="rounded-xl border border-border/70 p-4">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <p className="font-medium">RSI rule</p>
            <p className="text-sm text-muted-foreground">
              Trigger entries when RSI crosses your threshold condition.
            </p>
          </div>
          <label className="inline-flex items-center gap-2 text-sm font-medium">
            <input
              checked={values.rsiEnabled}
              className="h-4 w-4 rounded border-input"
              type="checkbox"
              onChange={(event) => onChange({ rsiEnabled: event.target.checked })}
            />
            Enable RSI rule
          </label>
        </div>

        {values.rsiEnabled ? (
          <div className="mt-4 grid gap-4 sm:grid-cols-3">
            <div className="space-y-2">
              <Label htmlFor="rsiOperator">Operator</Label>
              <Select
                id="rsiOperator"
                value={values.rsiOperator}
                options={[
                  { value: "lt", label: "Less than" },
                  { value: "lte", label: "Less than or equal" },
                  { value: "gt", label: "Greater than" },
                  { value: "gte", label: "Greater than or equal" },
                ]}
                onChange={(event) => onChange({ rsiOperator: event.target.value as ComparisonOperator })}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="rsiThreshold">Threshold</Label>
              <Input
                id="rsiThreshold"
                inputMode="decimal"
                value={values.rsiThreshold}
                aria-invalid={!!errors.rsiThreshold}
                aria-describedby={errors.rsiThreshold ? "rsiThreshold-error" : undefined}
                onChange={(event) => onChange({ rsiThreshold: event.target.value })}
              />
              {errors.rsiThreshold ? <p id="rsiThreshold-error" className="text-sm text-destructive">{errors.rsiThreshold}</p> : null}
            </div>

            <div className="space-y-2">
              <Label htmlFor="rsiPeriod">Period</Label>
              <Input
                id="rsiPeriod"
                inputMode="numeric"
                value={values.rsiPeriod}
                aria-invalid={!!errors.rsiPeriod}
                aria-describedby={errors.rsiPeriod ? "rsiPeriod-error" : undefined}
                onChange={(event) => onChange({ rsiPeriod: event.target.value })}
              />
              {errors.rsiPeriod ? <p id="rsiPeriod-error" className="text-sm text-destructive">{errors.rsiPeriod}</p> : null}
            </div>
          </div>
        ) : null}
      </div>

      <div className="rounded-xl border border-border/70 p-4">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <p className="font-medium">Moving-average crossover rule</p>
            <p className="text-sm text-muted-foreground">
              Add either SMA or EMA crossover confirmation to the entry logic.
            </p>
          </div>
          <label className="inline-flex items-center gap-2 text-sm font-medium">
            <input
              checked={values.movingAverageEnabled}
              className="h-4 w-4 rounded border-input"
              type="checkbox"
              onChange={(event) => onChange({ movingAverageEnabled: event.target.checked })}
            />
            Enable crossover rule
          </label>
        </div>

        {values.movingAverageEnabled ? (
          <div className="mt-4 grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <div className="space-y-2">
              <Label htmlFor="movingAverageType">Rule type</Label>
              <Select
                id="movingAverageType"
                value={values.movingAverageType}
                options={[
                  { value: "sma_crossover", label: "SMA crossover" },
                  { value: "ema_crossover", label: "EMA crossover" },
                ]}
                onChange={(event) =>
                  onChange({ movingAverageType: event.target.value as MovingAverageRuleType })
                }
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="fastPeriod">Fast period</Label>
              <Input
                id="fastPeriod"
                inputMode="numeric"
                value={values.fastPeriod}
                aria-invalid={!!errors.fastPeriod}
                aria-describedby={errors.fastPeriod ? "fastPeriod-error" : undefined}
                onChange={(event) => onChange({ fastPeriod: event.target.value })}
              />
              {errors.fastPeriod ? <p id="fastPeriod-error" className="text-sm text-destructive">{errors.fastPeriod}</p> : null}
            </div>

            <div className="space-y-2">
              <Label htmlFor="slowPeriod">Slow period</Label>
              <Input
                id="slowPeriod"
                inputMode="numeric"
                value={values.slowPeriod}
                aria-invalid={!!errors.slowPeriod}
                aria-describedby={errors.slowPeriod ? "slowPeriod-error" : undefined}
                onChange={(event) => onChange({ slowPeriod: event.target.value })}
              />
              {errors.slowPeriod ? <p id="slowPeriod-error" className="text-sm text-destructive">{errors.slowPeriod}</p> : null}
            </div>

            <div className="space-y-2">
              <Label htmlFor="crossoverDirection">Direction</Label>
              <Select
                id="crossoverDirection"
                value={values.crossoverDirection}
                options={[
                  { value: "bullish", label: "Bullish" },
                  { value: "bearish", label: "Bearish" },
                ]}
                onChange={(event) =>
                  onChange({ crossoverDirection: event.target.value as CrossoverDirection })
                }
              />
            </div>
          </div>
        ) : null}
      </div>
    </div>
  );
}
