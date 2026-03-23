from __future__ import annotations

from backtestforecast.config import Settings, get_settings
from backtestforecast.schemas.billing import (
    PricingContractResponse,
    PricingIntervalResponse,
    PricingPlanResponse,
)


def build_pricing_contract(settings: Settings | None = None) -> PricingContractResponse:
    settings = settings or get_settings()

    def interval(tier: str, billing_interval: str, amount_usd: int) -> PricingIntervalResponse:
        price_id = settings.stripe_price_lookup.get((tier, billing_interval))
        suffix = "mo" if billing_interval == "monthly" else "yr"
        return PricingIntervalResponse(
            price_id=price_id,
            unit_amount_usd=amount_usd if price_id else None,
            display_price=f"${amount_usd}/{suffix}",
            available=bool(price_id),
        )

    return PricingContractResponse(
        plans=[
            PricingPlanResponse(
                tier="free",
                title="Free",
                headline="$0",
                description="Get started with manual research",
                features=[
                    "5 backtests / month",
                    "30 days of history",
                    "2 side-by-side comparison slots",
                    "No scanner, forecast, or export access",
                ],
            ),
            PricingPlanResponse(
                tier="pro",
                title="Pro",
                headline="Unlimited backtests and starter automation",
                description="Best for active solo research workflows",
                monthly=interval("pro", "monthly", 29),
                yearly=interval("pro", "yearly", 290),
                features=[
                    "Unlimited backtests",
                    "Basic scanner access",
                    "Historical-analog forecasting",
                    "CSV exports",
                    "365-day history window",
                ],
            ),
            PricingPlanResponse(
                tier="premium",
                title="Premium",
                headline="Advanced automation and full exports",
                description="For heavier scanners, exports, and scheduled workflows",
                monthly=interval("premium", "monthly", 79),
                yearly=interval("premium", "yearly", 790),
                features=[
                    "Advanced scanner access",
                    "PDF + CSV exports",
                    "Full history depth",
                    "Highest comparison allowance",
                    "Priority scheduled scan refreshes",
                ],
            ),
        ]
    )
