import Link from "next/link";
import { getTemplates } from "@/lib/api/server";
import { formatCurrency, formatNumber, formatDateTime, strategyLabel } from "@/lib/backtests/format";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { TemplateActions } from "@/components/templates/template-actions";

export const dynamic = "force-dynamic";

export default async function TemplatesPage() {
  try {
    const data = await getTemplates();

    return (
      <div className="space-y-6">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Templates</p>
            <h1 className="mt-2 text-3xl font-semibold tracking-tight">Saved templates</h1>
            <p className="mt-2 text-muted-foreground">
              Reusable backtest configurations. Apply a template when creating a new run to pre-fill
              strategy, rules, and risk inputs.
            </p>
          </div>
          <Button asChild>
            <Link href="/app/backtests/new">New backtest</Link>
          </Button>
        </div>

        {data.template_limit != null ? (
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                Template usage
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-3xl font-semibold tracking-tight">
                {data.total} / {data.template_limit}
              </p>
              <p className="mt-1 text-sm text-muted-foreground">
                {data.template_limit - data.total > 0
                  ? `${data.template_limit - data.total} remaining on your plan`
                  : "Template limit reached"}
              </p>
            </CardContent>
          </Card>
        ) : null}

        <Card>
          <CardHeader>
            <CardTitle>Your templates</CardTitle>
            <CardDescription>
              {data.items.length === 0
                ? "No templates saved yet."
                : `${data.items.length} template(s) saved.`}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {data.items.length === 0 ? (
              <div className="rounded-xl border border-dashed p-10 text-center">
                <p className="text-base font-medium">No templates yet</p>
                <p className="mt-2 text-sm text-muted-foreground">
                  Create a backtest and use &quot;Save as template&quot; to save your configuration
                  for reuse.
                </p>
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Name</TableHead>
                    <TableHead>Strategy</TableHead>
                    <TableHead>DTE / Hold</TableHead>
                    <TableHead>Account</TableHead>
                    <TableHead>Updated</TableHead>
                    <TableHead />
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {data.items.map((template) => {
                    const cfg = template.config;
                    return (
                      <TableRow key={template.id}>
                        <TableCell>
                          <div className="space-y-1">
                            <p className="font-medium">{template.name}</p>
                            {template.description ? (
                              <p className="text-xs text-muted-foreground">{template.description}</p>
                            ) : null}
                            {cfg.default_symbol ? (
                              <Badge variant="secondary">{cfg.default_symbol}</Badge>
                            ) : null}
                          </div>
                        </TableCell>
                        <TableCell>{strategyLabel(template.strategy_type)}</TableCell>
                        <TableCell>
                          <div className="space-y-1">
                            <p>{cfg.target_dte} DTE</p>
                            <p className="text-xs text-muted-foreground">
                              Max hold {cfg.max_holding_days}d
                            </p>
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="space-y-1">
                            <p>{formatCurrency(cfg.account_size ?? 0)}</p>
                            <p className="text-xs text-muted-foreground">
                              {formatNumber(cfg.risk_per_trade_pct ?? 0)}% risk
                            </p>
                          </div>
                        </TableCell>
                        <TableCell className="text-muted-foreground">
                          {formatDateTime(template.updated_at)}
                        </TableCell>
                        <TableCell>
                          <TemplateActions templateId={template.id} templateName={template.name} templateDescription={template.description ?? ""} templateUpdatedAt={template.updated_at} />
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>
      </div>
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Templates could not be loaded.";

    return (
      <div className="space-y-6">
        <div>
          <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Templates</p>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight">Saved templates</h1>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Unable to load templates</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground">{message}</p>
          </CardContent>
        </Card>
      </div>
    );
  }
}
