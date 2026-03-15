import { getCurrentUser, getStrategyCatalog, getTemplates } from "@/lib/api/server";
import { ApiError } from "@/lib/api/shared";
import { buildBacktestQuota } from "@/lib/backtests/quota";
import { BacktestForm } from "@/components/backtests/backtest-form";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default async function NewBacktestPage({
  searchParams,
}: {
  searchParams: Promise<{ template?: string }>;
}) {
  try {
    const [userRes, templateRes, catalogRes, params] = await Promise.allSettled([
      getCurrentUser(),
      getTemplates(),
      getStrategyCatalog(),
      searchParams,
    ]);
    if (userRes.status === "rejected") throw userRes.reason;
    if (templateRes.status === "rejected") throw templateRes.reason;
    const user = userRes.value;
    const templateData = templateRes.value;
    const catalog = catalogRes.status === "fulfilled" ? catalogRes.value : undefined;
    const resolvedParams = params.status === "fulfilled" ? params.value : {};
    const quota = buildBacktestQuota(user);

    return (
      <div className="space-y-6">
        <div>
          <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Manual backtest</p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Create a new backtest</h1>
          <p className="mt-2 max-w-2xl text-muted-foreground">
            {catalog ? `Choose from ${catalog.total_strategies} strategies across single-leg, spreads, multi-leg, and income categories.` : "Choose a strategy and configure your backtest."}
            {" "}Apply a template to pre-fill the form.
          </p>
        </div>

        <BacktestForm
          quota={quota}
          templates={templateData.items}
          catalogGroups={catalog?.groups}
          initialTemplateId={resolvedParams.template}
        />
      </div>
    );
  } catch (error) {
    const message = error instanceof ApiError ? error.message : "This page could not be loaded. Please try again.";

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
