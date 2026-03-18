import { getCurrentUser } from "@/lib/api/server";
import { SweepForm } from "@/components/sweeps/sweep-form";
import { UpgradePrompt } from "@/components/billing/upgrade-prompt";

export const dynamic = "force-dynamic";

export default async function NewSweepPage() {
  let user;
  try {
    user = await getCurrentUser();
  } catch {
    return <div className="p-8 text-center text-muted-foreground">Unable to load user data. Please try again.</div>;
  }

  if (user.plan_tier === "free" || !user.features.forecasting_access) {
    return (
      <div className="space-y-6">
        <div>
          <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Sweeps</p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">New parameter sweep</h1>
        </div>
        <UpgradePrompt message="Sweep optimization requires a Pro or Premium plan. Upgrade to run parameter sweeps." />
      </div>
    );
  }

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
