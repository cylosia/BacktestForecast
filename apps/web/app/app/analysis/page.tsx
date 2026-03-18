import { getCurrentUser, getAnalysisHistory } from "@/lib/api/server";
import { SymbolAnalysisLauncher } from "@/components/analysis/symbol-analysis-launcher";
import { AnalysisHistory } from "@/components/analysis/analysis-history";
import { UpgradePrompt } from "@/components/billing/upgrade-prompt";

async function AnalysisHistorySection() {
  try {
    const history = await getAnalysisHistory();
    return <AnalysisHistory data={history} />;
  } catch {
    return (
      <div className="rounded-lg border p-4 text-sm text-muted-foreground">
        Unable to load analysis history. Please try again later.
      </div>
    );
  }
}

export const dynamic = "force-dynamic";

export default async function AnalysisPage() {
  let user;
  try {
    user = await getCurrentUser();
  } catch {
    return <div className="p-8 text-center text-muted-foreground">Unable to load user data. Please try again.</div>;
  }
  const hasAccess = user.features.forecasting_access;

  return (
    <div className="space-y-6">
      <div>
        <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Deep analysis</p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">Symbol deep dive</h1>
        <p className="mt-2 max-w-2xl text-muted-foreground">
          Enter a symbol to run an exhaustive analysis across all strategies, parameter
          permutations, and market regimes. The analyzer tests hundreds of configurations
          to find the optimal trade setup.
        </p>
      </div>

      {!hasAccess ? (
        <UpgradePrompt message="Symbol Deep Analysis requires a Pro or Premium plan. Upgrade to analyze any symbol across all strategies and parameter permutations." />
      ) : (
        <>
          <SymbolAnalysisLauncher />
          <AnalysisHistorySection />
        </>
      )}
    </div>
  );
}
