from fastapi import APIRouter, Depends, Response

from apps.api.app.dependencies import get_current_user_readonly
from backtestforecast.billing.entitlements import normalize_plan_tier
from backtestforecast.config import get_settings
from backtestforecast.models import User
from backtestforecast.schemas.catalog import (
    StrategyCatalogGroupResponse,
    StrategyCatalogItemResponse,
    StrategyCatalogResponse,
)
from backtestforecast.security import get_rate_limiter
from backtestforecast.strategy_catalog.catalog import (
    CATEGORY_LABELS,
    STRATEGY_CATALOG,
    get_catalog_entries_grouped,
    log_missing_catalog_entries,
)

router = APIRouter(tags=["catalog"])


@router.get("/strategy-catalog", response_model=StrategyCatalogResponse)
def get_strategy_catalog(
    response: Response,
    user: User = Depends(get_current_user_readonly),
) -> StrategyCatalogResponse:
    settings = get_settings()
    get_rate_limiter().check(
        bucket="catalog:read",
        actor_key=str(user.id),
        limit=120,
        window_seconds=settings.rate_limit_window_seconds,
    )
    response.headers["Cache-Control"] = "private, max-age=3600"
    log_missing_catalog_entries()
    grouped = get_catalog_entries_grouped()
    groups = []
    for category, entries in grouped:
        groups.append(
            StrategyCatalogGroupResponse(
                category=category.value,
                category_label=CATEGORY_LABELS[category],
                strategies=[
                    StrategyCatalogItemResponse(
                        strategy_type=e.strategy_type,
                        label=e.label,
                        short_description=e.short_description,
                        category=e.category.value,
                        bias=e.bias.value,
                        leg_count=e.leg_count,
                        min_tier=e.min_tier.value,
                        max_loss_description=e.max_loss_description,
                        notes=e.notes,
                        tags=list(e.tags),
                    )
                    for e in entries
                ],
            )
        )
    effective_tier = normalize_plan_tier(
        user.plan_tier, user.subscription_status, user.subscription_current_period_end,
    )
    return StrategyCatalogResponse(
        groups=groups,
        total_strategies=len(STRATEGY_CATALOG),
        user_tier=effective_tier.value,
    )
