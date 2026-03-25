import { MultiStepBacktestForm } from "@/components/backtests/multi-step-backtest-form";

export const dynamic = "force-dynamic";

export default function NewMultiStepBacktestPage() {
  return (
    <div className="space-y-6">
      <div>
        <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Alpha workflows</p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">Create a multi-step backtest</h1>
        <p className="mt-2 max-w-2xl text-muted-foreground">
          Build staged workflows on one symbol with explicit downstream liquidation-on-failure semantics.
        </p>
      </div>
      <MultiStepBacktestForm />
    </div>
  );
}
