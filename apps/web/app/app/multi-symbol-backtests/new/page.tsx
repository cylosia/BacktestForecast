import { MultiSymbolBacktestForm } from "@/components/backtests/multi-symbol-backtest-form";

export const dynamic = "force-dynamic";

export default function NewMultiSymbolBacktestPage() {
  return (
    <div className="space-y-6">
      <div>
        <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Alpha workflows</p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">Create a multi-symbol backtest</h1>
        <p className="mt-2 max-w-2xl text-muted-foreground">
          Build coordinated option workflows across 2-3 symbols with synchronous entry and symbol-scoped risk.
        </p>
      </div>
      <MultiSymbolBacktestForm />
    </div>
  );
}
