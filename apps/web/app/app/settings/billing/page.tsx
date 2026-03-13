import { Suspense } from "react";
import Link from "next/link";
import { getCurrentUser } from "@/lib/api/server";
import { formatDateTime } from "@/lib/backtests/format";
import { CheckoutSuccessBanner } from "@/components/billing/checkout-success-banner";
import { PortalButton } from "@/components/billing/portal-button";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { planLabel } from "@/lib/plan";

export default async function BillingSettingsPage() {
  let user;
  try {
    user = await getCurrentUser();
  } catch {
    return <div className="p-8 text-center text-muted-foreground">Unable to load user data. Please try again.</div>;
  }

  return (
    <main className="mx-auto max-w-4xl space-y-6 px-6 py-12">
      <div>
        <h1 className="text-3xl font-semibold">Billing settings</h1>
        <p className="mt-3 text-muted-foreground">
          Manage your current subscription tier, billing portal access, and entitlement-backed feature limits.
        </p>
      </div>

      <Suspense>
        <CheckoutSuccessBanner />
      </Suspense>

      <div className="grid gap-6 lg:grid-cols-[1.1fr_0.9fr]">
        <Card>
          <CardHeader>
            <CardTitle>Current plan</CardTitle>
            <CardDescription>Backend-owned billing state synced from Stripe webhooks.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="rounded-xl border border-border/70 p-4">
              <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Plan tier</p>
              <p className="mt-2 text-2xl font-semibold tracking-tight">{planLabel(user.plan_tier)}</p>
            </div>
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="rounded-xl border border-border/70 p-4">
                <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Subscription status</p>
                <p className="mt-2 font-medium">{user.subscription_status ?? "not subscribed"}</p>
              </div>
              <div className="rounded-xl border border-border/70 p-4">
                <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Billing interval</p>
                <p className="mt-2 font-medium">{user.subscription_billing_interval ?? "n/a"}</p>
              </div>
            </div>
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="rounded-xl border border-border/70 p-4">
                <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Current period end</p>
                <p className="mt-2 font-medium">
                  {user.subscription_current_period_end ? formatDateTime(user.subscription_current_period_end) : "n/a"}
                </p>
              </div>
              <div className="rounded-xl border border-border/70 p-4">
                <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Cancel at period end</p>
                <p className="mt-2 font-medium">{user.cancel_at_period_end ? "Yes" : "No"}</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Plan actions</CardTitle>
            <CardDescription>Use Stripe-hosted flows for upgrades, downgrades, and self-service management.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <Button asChild className="w-full">
              <Link href="/pricing">View pricing</Link>
            </Button>
            {user.plan_tier === "free" ? (
              <p className="text-sm text-muted-foreground">Upgrade from the pricing page to unlock scanner, forecast, and export access.</p>
            ) : (
              <PortalButton>Open billing portal</PortalButton>
            )}
            <div className="rounded-xl border border-border/70 p-4 text-sm text-muted-foreground">
              Export access: {(user.features.export_formats ?? []).length > 0 ? (user.features.export_formats ?? []).join(", ") : "none"}
              <br />
              Forecast access: {user.features.forecasting_access ? "enabled" : "locked"}
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Current usage</CardTitle>
          <CardDescription>
            Backend-authoritative usage counts for the current billing period.
          </CardDescription>
        </CardHeader>
        <CardContent className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          <div className="rounded-xl border border-border/70 p-4">
            <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Backtests this month</p>
            <p className="mt-2 text-2xl font-semibold tracking-tight">
              {user.usage.backtests_used_this_month}
              {user.features.monthly_backtest_quota !== null
                ? ` / ${user.features.monthly_backtest_quota}`
                : ""}
            </p>
            <p className="mt-1 text-xs text-muted-foreground">
              {user.usage.backtests_remaining_this_month === null
                ? "No cap on this plan"
                : `${user.usage.backtests_remaining_this_month} remaining`}
            </p>
          </div>
          <div className="rounded-xl border border-border/70 p-4">
            <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">History depth</p>
            <p className="mt-2 text-2xl font-semibold tracking-tight">
              {user.features.history_days === null ? "Full" : `${user.features.history_days}d`}
            </p>
            <p className="mt-1 text-xs text-muted-foreground">
              Up to {user.features.history_item_limit} items
            </p>
          </div>
          <div className="rounded-xl border border-border/70 p-4">
            <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Comparison slots</p>
            <p className="mt-2 text-2xl font-semibold tracking-tight">
              {user.features.side_by_side_comparison_limit}
            </p>
          </div>
          <div className="rounded-xl border border-border/70 p-4">
            <p className="text-xs uppercase tracking-[0.16em] text-muted-foreground">Scanner modes</p>
            <p className="mt-2 text-2xl font-semibold tracking-tight">
              {(user.features.scanner_modes ?? []).length > 0
                ? (user.features.scanner_modes ?? []).join(" + ")
                : "None"}
            </p>
          </div>
        </CardContent>
      </Card>
    </main>
  );
}
