import { getCurrentUser, getStrategyCatalog, getTemplates } from "@/lib/api/server";
import { buildBacktestQuota } from "@/lib/backtests/quota";
import { BacktestForm } from "@/components/backtests/backtest-form";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default async function NewBacktestPage({
  searchParams,
}: {
  searchParams: Promise<{ template?: string }>;
}) {
  try {
    const [user, templateData, catalog, params] = await Promise.all([
      getCurrentUser(),
      getTemplates(),
      getStrategyCatalog(),
      searchParams,
    ]);
    const quota = buildBacktestQuota(user);

    return (
      <div className="space-y-6">
        <div>
          <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Manual backtest</p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Create a new backtest</h1>
          <p className="mt-2 max-w-2xl text-muted-foreground">
            Choose from {catalog.total_strategies} strategies across single-leg, spreads, multi-leg,
            and income categories. Apply a template to pre-fill the form.
          </p>
        </div>

        <BacktestForm
          quota={quota}
          templates={templateData.items}
          catalogGroups={catalog.groups}
          initialTemplateId={params.template}
        />
      </div>
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "The backtest form could not be prepared.";

    return (
      <div className="space-y-6">
        <div>
          <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Manual backtest</p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Create a new backtest</h1>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Unable to load form prerequisites</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground">{message}</p>
          </CardContent>
        </Card>
      </div>
    );
  }
}
