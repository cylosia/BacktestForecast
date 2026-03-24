import { UpgradePrompt } from "@/components/billing/upgrade-prompt";
import { ForecastLookup } from "@/components/forecasts/forecast-lookup";
import { getCurrentUser } from "@/lib/api/server";

export const dynamic = "force-dynamic";

export default async function ForecastsPage() {
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
        <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Forecasts</p>
        <h1 className="mt-2 text-3xl font-semibold tracking-tight">
          Historical analog forecasts
        </h1>
        <p className="mt-2 max-w-2xl text-muted-foreground">
          Look up a probabilistic expected-return range for any supported ticker. The forecast
          is built from historical analog setups that match the current technical conditions
          on daily bars.
        </p>
      </div>

      {!hasAccess ? (
        <UpgradePrompt message="Forecast access requires a Pro or Premium plan. Upgrade to look up expected-return ranges for any ticker." requiredTier="pro" />
      ) : (
        <ForecastLookup />
      )}

      <div className="rounded-xl border border-border/70 bg-muted/30 p-4 text-sm text-muted-foreground space-y-2">
        <p className="font-medium text-foreground">How it works</p>
        <p>
          The engine scans the past year of daily bars for setups with similar RSI, moving-average
          alignment, and volatility characteristics. It then measures the forward return over your
          chosen horizon for each matched analog and produces a low/median/high expected range.
        </p>
        <p>
          The favorable outcome rate shows what percentage of historical analogs favored the
          selected strategy direction over the horizon period. The analog count indicates how
          many matches were found — higher counts mean more statistical confidence.
        </p>
        <p className="text-xs">
          This is research software. Forecasts are probabilistic ranges derived from historical
          patterns, not predictions, certainties, or financial advice.
        </p>
      </div>
    </div>
  );
}
