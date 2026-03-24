import Link from "next/link";
import { ArrowRight, Check } from "lucide-react";
import type { PlanTier } from "@backtestforecast/api-client";
import { validatePricingContractResponse, type PricingContractResponseShape } from "@/lib/api/contracts";
import { CheckoutButton } from "@/components/billing/checkout-button";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

const API_BASE = (process.env.API_INTERNAL_BASE_URL ?? process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000").replace(/\/+$/, "");

type PaidPlanTier = Exclude<PlanTier, "free">;

function isPaidPlanTier(tier: PlanTier): tier is PaidPlanTier {
  return tier === "pro" || tier === "premium";
}

async function getPricingContract(): Promise<PricingContractResponseShape> {
  const response = await fetch(`${API_BASE}/v1/billing/pricing`, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Failed to load pricing contract (${response.status}).`);
  }
  return validatePricingContractResponse((await response.json()) as unknown);
}

export default async function PricingPage() {
  const contract = await getPricingContract();

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
        {contract.plans.map((plan) => (
          <Card key={plan.tier} className="h-full border-border/80">
            <CardHeader>
              <CardTitle>{plan.title}</CardTitle>
              <CardDescription>{plan.description}</CardDescription>
              <p
                className="pt-3 text-3xl font-semibold tracking-tight"
                data-testid={plan.tier === "pro" ? "price-pro" : plan.tier === "premium" ? "price-premium" : undefined}
              >
                {plan.monthly?.display_price ?? plan.headline}
              </p>
              {plan.yearly?.display_price ? (
                <p className="text-sm text-muted-foreground">{plan.yearly.display_price}</p>
              ) : null}
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

              {plan.monthly && isPaidPlanTier(plan.tier) ? (
                <div className="space-y-3">
                  <CheckoutButton className="w-full" billingInterval="monthly" tier={plan.tier}>
                    Start monthly plan
                  </CheckoutButton>
                  {plan.yearly ? (
                    <CheckoutButton className="w-full" billingInterval="yearly" tier={plan.tier}>
                      Start yearly plan
                    </CheckoutButton>
                  ) : null}
                </div>
              ) : (
                <div className="space-y-3">
                  <Button asChild className="w-full" variant="outline">
                    <Link href="/sign-in?redirect_url=%2Fpricing">
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
        All amounts in {contract.currency}. Checkout is authoritative and uses the configured Stripe price IDs for paid plans.
      </p>
    </main>
  );
}
