import { getCurrentUser } from "@/lib/api/server";
import { ScannerForm } from "@/components/scanner/scanner-form";
import { UpgradePrompt } from "@/components/billing/upgrade-prompt";

export const dynamic = "force-dynamic";

export default async function NewScanPage() {
  let user;
  try {
    user = await getCurrentUser();
  } catch {
    return <div className="p-8 text-center text-muted-foreground">Unable to load user data. Please try again.</div>;
  }
  const scannerModes = (user.features.scanner_modes ?? []) as ("basic" | "advanced")[];

  if (scannerModes.length === 0) {
    return (
      <div className="space-y-6">
        <div>
          <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Scanner</p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Create a new scan</h1>
        </div>
        <UpgradePrompt message="Scanner access requires a Pro or Premium plan. Upgrade to create scanner jobs." requiredTier="pro" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div>
        <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Scanner</p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">Create a new scan</h1>
        <p className="mt-2 max-w-2xl text-muted-foreground">
          Define your symbols, strategies, and rules. The scanner will evaluate all combinations
          and return ranked recommendations.
        </p>
      </div>

      <ScannerForm scannerModes={scannerModes} planTier={user.plan_tier as "free" | "pro" | "premium"} />
    </div>
  );
}
