import type {
  ComparisonOperator,
  CrossoverDirection,
  MovingAverageRuleType,
  BollingerBand,
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
  macdEnabled: boolean;
  macdFastPeriod: string;
  macdSlowPeriod: string;
  macdSignalPeriod: string;
  macdDirection: CrossoverDirection;
  bollingerEnabled: boolean;
  bollingerPeriod: string;
  bollingerStdDev: string;
  bollingerBand: BollingerBand;
  bollingerOperator: ComparisonOperator;
  ivRankEnabled: boolean;
  ivRankOperator: ComparisonOperator;
  ivRankThreshold: string;
  ivRankLookbackDays: string;
  ivPercentileEnabled: boolean;
  ivPercentileOperator: ComparisonOperator;
  ivPercentileThreshold: string;
  ivPercentileLookbackDays: string;
  volumeSpikeEnabled: boolean;
  volumeSpikeOperator: ComparisonOperator;
  volumeSpikeMultiplier: string;
  volumeSpikePeriod: string;
  supportResistanceEnabled: boolean;
  supportResistanceMode: string;
  supportResistancePeriod: string;
  supportResistanceTolerancePct: string;
  avoidEarningsEnabled: boolean;
  avoidEarningsDaysBefore: string;
  avoidEarningsDaysAfter: string;
}

interface TaRuleErrors {
  rsiThreshold?: string;
  rsiPeriod?: string;
  fastPeriod?: string;
  slowPeriod?: string;
  macdFastPeriod?: string;
  macdSlowPeriod?: string;
  macdSignalPeriod?: string;
  bollingerPeriod?: string;
  bollingerStdDev?: string;
  ivRankThreshold?: string;
  ivRankLookbackDays?: string;
  ivPercentileThreshold?: string;
  ivPercentileLookbackDays?: string;
  volumeSpikeMultiplier?: string;
  volumeSpikePeriod?: string;
  supportResistancePeriod?: string;
  supportResistanceTolerancePct?: string;
  avoidEarningsDaysBefore?: string;
  avoidEarningsDaysAfter?: string;
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

      <div className="rounded-xl border border-border/70 p-4">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <p className="font-medium">MACD rule</p>
            <p className="text-sm text-muted-foreground">
              Trigger entries on MACD line / signal line crossovers.
            </p>
          </div>
          <label className="inline-flex items-center gap-2 text-sm font-medium">
            <input
              checked={values.macdEnabled}
              className="h-4 w-4 rounded border-input"
              type="checkbox"
              onChange={(event) => onChange({ macdEnabled: event.target.checked })}
            />
            Enable MACD rule
          </label>
        </div>

        {values.macdEnabled ? (
          <div className="mt-4 grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <div className="space-y-2">
              <Label htmlFor="macdFastPeriod">Fast period</Label>
              <Input
                id="macdFastPeriod"
                inputMode="numeric"
                value={values.macdFastPeriod}
                aria-invalid={!!errors.macdFastPeriod}
                onChange={(event) => onChange({ macdFastPeriod: event.target.value })}
              />
              {errors.macdFastPeriod ? <p className="text-sm text-destructive">{errors.macdFastPeriod}</p> : null}
            </div>
            <div className="space-y-2">
              <Label htmlFor="macdSlowPeriod">Slow period</Label>
              <Input
                id="macdSlowPeriod"
                inputMode="numeric"
                value={values.macdSlowPeriod}
                aria-invalid={!!errors.macdSlowPeriod}
                onChange={(event) => onChange({ macdSlowPeriod: event.target.value })}
              />
              {errors.macdSlowPeriod ? <p className="text-sm text-destructive">{errors.macdSlowPeriod}</p> : null}
            </div>
            <div className="space-y-2">
              <Label htmlFor="macdSignalPeriod">Signal period</Label>
              <Input
                id="macdSignalPeriod"
                inputMode="numeric"
                value={values.macdSignalPeriod}
                aria-invalid={!!errors.macdSignalPeriod}
                onChange={(event) => onChange({ macdSignalPeriod: event.target.value })}
              />
              {errors.macdSignalPeriod ? <p className="text-sm text-destructive">{errors.macdSignalPeriod}</p> : null}
            </div>
            <div className="space-y-2">
              <Label htmlFor="macdDirection">Direction</Label>
              <Select
                id="macdDirection"
                value={values.macdDirection}
                options={[
                  { value: "bullish", label: "Bullish" },
                  { value: "bearish", label: "Bearish" },
                ]}
                onChange={(event) => onChange({ macdDirection: event.target.value as CrossoverDirection })}
              />
            </div>
          </div>
        ) : null}
      </div>

      <div className="rounded-xl border border-border/70 p-4">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <p className="font-medium">Bollinger Bands rule</p>
            <p className="text-sm text-muted-foreground">
              Trigger entries when price crosses a Bollinger Band.
            </p>
          </div>
          <label className="inline-flex items-center gap-2 text-sm font-medium">
            <input
              checked={values.bollingerEnabled}
              className="h-4 w-4 rounded border-input"
              type="checkbox"
              onChange={(event) => onChange({ bollingerEnabled: event.target.checked })}
            />
            Enable Bollinger rule
          </label>
        </div>

        {values.bollingerEnabled ? (
          <div className="mt-4 grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <div className="space-y-2">
              <Label htmlFor="bollingerPeriod">Period</Label>
              <Input
                id="bollingerPeriod"
                inputMode="numeric"
                value={values.bollingerPeriod}
                aria-invalid={!!errors.bollingerPeriod}
                onChange={(event) => onChange({ bollingerPeriod: event.target.value })}
              />
              {errors.bollingerPeriod ? <p className="text-sm text-destructive">{errors.bollingerPeriod}</p> : null}
            </div>
            <div className="space-y-2">
              <Label htmlFor="bollingerStdDev">Std deviations</Label>
              <Input
                id="bollingerStdDev"
                inputMode="decimal"
                value={values.bollingerStdDev}
                aria-invalid={!!errors.bollingerStdDev}
                onChange={(event) => onChange({ bollingerStdDev: event.target.value })}
              />
              {errors.bollingerStdDev ? <p className="text-sm text-destructive">{errors.bollingerStdDev}</p> : null}
            </div>
            <div className="space-y-2">
              <Label htmlFor="bollingerBand">Band</Label>
              <Select
                id="bollingerBand"
                value={values.bollingerBand}
                options={[
                  { value: "lower", label: "Lower band" },
                  { value: "middle", label: "Middle band" },
                  { value: "upper", label: "Upper band" },
                ]}
                onChange={(event) => onChange({ bollingerBand: event.target.value as BollingerBand })}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="bollingerOperator">Operator</Label>
              <Select
                id="bollingerOperator"
                value={values.bollingerOperator}
                options={[
                  { value: "lt", label: "Less than" },
                  { value: "lte", label: "Less than or equal" },
                  { value: "gt", label: "Greater than" },
                  { value: "gte", label: "Greater than or equal" },
                ]}
                onChange={(event) => onChange({ bollingerOperator: event.target.value as ComparisonOperator })}
              />
            </div>
          </div>
        ) : null}
      </div>

      <div className="rounded-xl border border-border/70 p-4">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <p className="font-medium">IV Rank rule</p>
            <p className="text-sm text-muted-foreground">
              Trigger entries based on implied volatility rank percentile.
            </p>
          </div>
          <label className="inline-flex items-center gap-2 text-sm font-medium">
            <input
              checked={values.ivRankEnabled}
              className="h-4 w-4 rounded border-input"
              type="checkbox"
              onChange={(event) => onChange({ ivRankEnabled: event.target.checked })}
            />
            Enable IV Rank rule
          </label>
        </div>

        {values.ivRankEnabled ? (
          <div className="mt-4 grid gap-4 sm:grid-cols-3">
            <div className="space-y-2">
              <Label htmlFor="ivRankOperator">Operator</Label>
              <Select
                id="ivRankOperator"
                value={values.ivRankOperator}
                options={[
                  { value: "gt", label: "Greater than" },
                  { value: "gte", label: "Greater than or equal" },
                  { value: "lt", label: "Less than" },
                  { value: "lte", label: "Less than or equal" },
                ]}
                onChange={(event) => onChange({ ivRankOperator: event.target.value as ComparisonOperator })}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="ivRankThreshold">Threshold (0–100)</Label>
              <Input
                id="ivRankThreshold"
                inputMode="decimal"
                value={values.ivRankThreshold}
                aria-invalid={!!errors.ivRankThreshold}
                onChange={(event) => onChange({ ivRankThreshold: event.target.value })}
              />
              {errors.ivRankThreshold ? <p className="text-sm text-destructive">{errors.ivRankThreshold}</p> : null}
            </div>
            <div className="space-y-2">
              <Label htmlFor="ivRankLookbackDays">Lookback days</Label>
              <Input
                id="ivRankLookbackDays"
                inputMode="numeric"
                value={values.ivRankLookbackDays}
                aria-invalid={!!errors.ivRankLookbackDays}
                onChange={(event) => onChange({ ivRankLookbackDays: event.target.value })}
              />
              {errors.ivRankLookbackDays ? <p className="text-sm text-destructive">{errors.ivRankLookbackDays}</p> : null}
            </div>
          </div>
        ) : null}
      </div>

      <div className="rounded-xl border border-border/70 p-4">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <p className="font-medium">IV Percentile rule</p>
            <p className="text-sm text-muted-foreground">
              Trigger entries based on implied volatility percentile rank.
            </p>
          </div>
          <label className="inline-flex items-center gap-2 text-sm font-medium">
            <input
              checked={values.ivPercentileEnabled}
              className="h-4 w-4 rounded border-input"
              type="checkbox"
              onChange={(event) => onChange({ ivPercentileEnabled: event.target.checked })}
            />
            Enable IV Percentile rule
          </label>
        </div>

        {values.ivPercentileEnabled ? (
          <div className="mt-4 grid gap-4 sm:grid-cols-3">
            <div className="space-y-2">
              <Label htmlFor="ivPercentileOperator">Operator</Label>
              <Select
                id="ivPercentileOperator"
                value={values.ivPercentileOperator}
                options={[
                  { value: "gt", label: "Greater than" },
                  { value: "gte", label: "Greater than or equal" },
                  { value: "lt", label: "Less than" },
                  { value: "lte", label: "Less than or equal" },
                ]}
                onChange={(event) => onChange({ ivPercentileOperator: event.target.value as ComparisonOperator })}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="ivPercentileThreshold">Threshold (0–100)</Label>
              <Input
                id="ivPercentileThreshold"
                inputMode="decimal"
                value={values.ivPercentileThreshold}
                aria-invalid={!!errors.ivPercentileThreshold}
                onChange={(event) => onChange({ ivPercentileThreshold: event.target.value })}
              />
              {errors.ivPercentileThreshold ? <p className="text-sm text-destructive">{errors.ivPercentileThreshold}</p> : null}
            </div>
            <div className="space-y-2">
              <Label htmlFor="ivPercentileLookbackDays">Lookback days</Label>
              <Input
                id="ivPercentileLookbackDays"
                inputMode="numeric"
                value={values.ivPercentileLookbackDays}
                aria-invalid={!!errors.ivPercentileLookbackDays}
                onChange={(event) => onChange({ ivPercentileLookbackDays: event.target.value })}
              />
              {errors.ivPercentileLookbackDays ? <p className="text-sm text-destructive">{errors.ivPercentileLookbackDays}</p> : null}
            </div>
          </div>
        ) : null}
      </div>

      <div className="rounded-xl border border-border/70 p-4">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <p className="font-medium">Volume spike rule</p>
            <p className="text-sm text-muted-foreground">
              Trigger entries when volume exceeds a multiple of its moving average.
            </p>
          </div>
          <label className="inline-flex items-center gap-2 text-sm font-medium">
            <input
              checked={values.volumeSpikeEnabled}
              className="h-4 w-4 rounded border-input"
              type="checkbox"
              onChange={(event) => onChange({ volumeSpikeEnabled: event.target.checked })}
            />
            Enable volume spike rule
          </label>
        </div>

        {values.volumeSpikeEnabled ? (
          <div className="mt-4 grid gap-4 sm:grid-cols-3">
            <div className="space-y-2">
              <Label htmlFor="volumeSpikeOperator">Operator</Label>
              <Select
                id="volumeSpikeOperator"
                value={values.volumeSpikeOperator}
                options={[
                  { value: "gt", label: "Greater than" },
                  { value: "gte", label: "Greater than or equal" },
                  { value: "lt", label: "Less than" },
                  { value: "lte", label: "Less than or equal" },
                ]}
                onChange={(event) => onChange({ volumeSpikeOperator: event.target.value as ComparisonOperator })}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="volumeSpikeMultiplier">Multiplier</Label>
              <Input
                id="volumeSpikeMultiplier"
                inputMode="decimal"
                value={values.volumeSpikeMultiplier}
                aria-invalid={!!errors.volumeSpikeMultiplier}
                onChange={(event) => onChange({ volumeSpikeMultiplier: event.target.value })}
              />
              <p className="text-xs text-muted-foreground">Volume must exceed average by this factor (e.g. 2 = 2x average).</p>
              {errors.volumeSpikeMultiplier ? <p className="text-sm text-destructive">{errors.volumeSpikeMultiplier}</p> : null}
            </div>
            <div className="space-y-2">
              <Label htmlFor="volumeSpikePeriod">Lookback period</Label>
              <Input
                id="volumeSpikePeriod"
                inputMode="numeric"
                value={values.volumeSpikePeriod}
                aria-invalid={!!errors.volumeSpikePeriod}
                onChange={(event) => onChange({ volumeSpikePeriod: event.target.value })}
              />
              {errors.volumeSpikePeriod ? <p className="text-sm text-destructive">{errors.volumeSpikePeriod}</p> : null}
            </div>
          </div>
        ) : null}
      </div>

      <div className="rounded-xl border border-border/70 p-4">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <p className="font-medium">Support / Resistance rule</p>
            <p className="text-sm text-muted-foreground">
              Trigger entries near rolling support or resistance levels.
            </p>
          </div>
          <label className="inline-flex items-center gap-2 text-sm font-medium">
            <input
              checked={values.supportResistanceEnabled}
              className="h-4 w-4 rounded border-input"
              type="checkbox"
              onChange={(event) => onChange({ supportResistanceEnabled: event.target.checked })}
            />
            Enable support/resistance rule
          </label>
        </div>

        {values.supportResistanceEnabled ? (
          <div className="mt-4 grid gap-4 sm:grid-cols-3">
            <div className="space-y-2">
              <Label htmlFor="supportResistanceMode">Mode</Label>
              <Select
                id="supportResistanceMode"
                value={values.supportResistanceMode}
                options={[
                  { value: "near_support", label: "Near support" },
                  { value: "near_resistance", label: "Near resistance" },
                  { value: "breakout_above_resistance", label: "Breakout above resistance" },
                  { value: "breakdown_below_support", label: "Breakdown below support" },
                ]}
                onChange={(event) => onChange({ supportResistanceMode: event.target.value })}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="supportResistancePeriod">Lookback period</Label>
              <Input
                id="supportResistancePeriod"
                inputMode="numeric"
                value={values.supportResistancePeriod}
                aria-invalid={!!errors.supportResistancePeriod}
                onChange={(event) => onChange({ supportResistancePeriod: event.target.value })}
              />
              {errors.supportResistancePeriod ? <p className="text-sm text-destructive">{errors.supportResistancePeriod}</p> : null}
            </div>
            <div className="space-y-2">
              <Label htmlFor="supportResistanceTolerancePct">Tolerance %</Label>
              <Input
                id="supportResistanceTolerancePct"
                inputMode="decimal"
                value={values.supportResistanceTolerancePct}
                aria-invalid={!!errors.supportResistanceTolerancePct}
                onChange={(event) => onChange({ supportResistanceTolerancePct: event.target.value })}
              />
              {errors.supportResistanceTolerancePct ? <p className="text-sm text-destructive">{errors.supportResistanceTolerancePct}</p> : null}
            </div>
          </div>
        ) : null}
      </div>

      <div className="rounded-xl border border-border/70 p-4">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <p className="font-medium">Avoid earnings rule</p>
            <p className="text-sm text-muted-foreground">
              Skip entries near earnings announcement dates.
            </p>
          </div>
          <label className="inline-flex items-center gap-2 text-sm font-medium">
            <input
              checked={values.avoidEarningsEnabled}
              className="h-4 w-4 rounded border-input"
              type="checkbox"
              onChange={(event) => onChange({ avoidEarningsEnabled: event.target.checked })}
            />
            Enable earnings avoidance
          </label>
        </div>

        {values.avoidEarningsEnabled ? (
          <div className="mt-4 grid gap-4 sm:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="avoidEarningsDaysBefore">Days before earnings</Label>
              <Input
                id="avoidEarningsDaysBefore"
                inputMode="numeric"
                value={values.avoidEarningsDaysBefore}
                aria-invalid={!!errors.avoidEarningsDaysBefore}
                onChange={(event) => onChange({ avoidEarningsDaysBefore: event.target.value })}
              />
              {errors.avoidEarningsDaysBefore ? <p className="text-sm text-destructive">{errors.avoidEarningsDaysBefore}</p> : null}
            </div>
            <div className="space-y-2">
              <Label htmlFor="avoidEarningsDaysAfter">Days after earnings</Label>
              <Input
                id="avoidEarningsDaysAfter"
                inputMode="numeric"
                value={values.avoidEarningsDaysAfter}
                aria-invalid={!!errors.avoidEarningsDaysAfter}
                onChange={(event) => onChange({ avoidEarningsDaysAfter: event.target.value })}
              />
              {errors.avoidEarningsDaysAfter ? <p className="text-sm text-destructive">{errors.avoidEarningsDaysAfter}</p> : null}
            </div>
          </div>
        ) : null}
      </div>
    </div>
  );
}
