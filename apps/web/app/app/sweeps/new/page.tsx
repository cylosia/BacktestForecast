import { SweepForm } from "@/components/sweeps/sweep-form";

export const dynamic = "force-dynamic";

export default function NewSweepPage() {
  return (
    <div className="space-y-6">
      <div>
        <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Sweeps</p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">New parameter sweep</h1>
        <p className="mt-2 text-muted-foreground">
          Configure a grid or genetic sweep to find optimal strategy parameters for a symbol.
        </p>
      </div>
      <SweepForm />
    </div>
  );
}
