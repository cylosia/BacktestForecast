import Link from "next/link";
import { ArrowRight, Check } from "lucide-react";
import { CheckoutButton } from "@/components/billing/checkout-button";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

// Prices are hard-coded to match Stripe configuration. If Stripe prices
// change, update these values. A future improvement would fetch from a
// /api/pricing endpoint to guarantee parity.
const plans = [
  {
    title: "Free",
    price: "$0",
    subtitle: "Get started with manual research",
    monthly: null,
    yearly: null,
    features: [
      "5 backtests / month",
      "30 days of history",
      "1 side-by-side comparison slot",
      "No scanner, forecast, or export access",
    ],
  },
  {
    title: "Pro",
    price: "$29/mo",
    subtitle: "$290/year",
    monthly: { tier: "pro" as const, billingInterval: "monthly" as const },
    yearly: { tier: "pro" as const, billingInterval: "yearly" as const },
    features: [
      "Unlimited backtests",
      "Basic scanner access",
      "Historical-analog forecasting",
      "CSV exports",
      "365-day history window",
    ],
  },
  {
    title: "Premium",
    price: "$79/mo",
    subtitle: "$790/year",
    monthly: { tier: "premium" as const, billingInterval: "monthly" as const },
    yearly: { tier: "premium" as const, billingInterval: "yearly" as const },
    features: [
      "Advanced scanner access",
      "PDF + CSV exports",
      "Full history depth",
      "Highest comparison allowance",
      "Priority scheduled scan refreshes",
    ],
  },
] as const;

export default function PricingPage() {
  return (
    <main className="mx-auto max-w-6xl px-6 py-16">
      <div className="mx-auto max-w-3xl text-center">
        <p className="text-sm uppercase tracking-[0.2em] text-muted-foreground">Pricing</p>
        <h1 className="mt-3 text-4xl font-semibold tracking-tight sm:text-5xl">
          Tiered access for backtests, scanner workflows, and exports.
        </h1>
        <p className="mt-4 text-lg text-muted-foreground">
          Sign in before starting checkout. Billing is powered by Stripe-hosted Checkout and the customer portal.
        </p>
      </div>

      <div className="mt-12 grid gap-6 lg:grid-cols-3">
        {plans.map((plan) => (
          <Card key={plan.title} className="h-full border-border/80">
            <CardHeader>
              <CardTitle>{plan.title}</CardTitle>
              <CardDescription>{plan.subtitle}</CardDescription>
              <p className="pt-3 text-3xl font-semibold tracking-tight">{plan.price}</p>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="space-y-3">
                {plan.features.map((feature) => (
                  <div className="flex items-start gap-3" key={feature}>
                    <Check className="mt-0.5 h-4 w-4 text-primary" />
                    <p className="text-sm text-muted-foreground">{feature}</p>
                  </div>
                ))}
              </div>

              {plan.monthly ? (
                <div className="space-y-3">
                  <CheckoutButton className="w-full" billingInterval={plan.monthly.billingInterval} tier={plan.monthly.tier}>
                    Start monthly plan
                  </CheckoutButton>
                  {plan.yearly ? (
                    <CheckoutButton className="w-full" billingInterval={plan.yearly.billingInterval} tier={plan.yearly.tier}>
                      Start yearly plan
                    </CheckoutButton>
                  ) : null}
                </div>
              ) : (
                <div className="space-y-3">
                  <Button asChild className="w-full" variant="outline">
                    <Link href="/sign-in">
                      Sign in to get started
                      <ArrowRight className="h-4 w-4" />
                    </Link>
                  </Button>
                  <p className="text-sm text-muted-foreground">Free access is provisioned automatically when you sign in.</p>
                </div>
              )}
            </CardContent>
          </Card>
        ))}
      </div>

      <p className="mt-8 text-center text-xs text-muted-foreground">
        All amounts in USD. Prices shown match current Stripe configuration.
      </p>
    </main>
  );
}
