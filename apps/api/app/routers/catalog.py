from fastapi import APIRouter, Depends, Response

from apps.api.app.dependencies import require_authenticated_user
from backtestforecast.schemas.catalog import (
    StrategyCatalogGroupResponse,
    StrategyCatalogItemResponse,
    StrategyCatalogResponse,
)
from backtestforecast.strategy_catalog.catalog import (
    CATEGORY_LABELS,
    STRATEGY_CATALOG,
    get_catalog_entries_grouped,
)

router = APIRouter(tags=["catalog"])


@router.get("/strategy-catalog", response_model=StrategyCatalogResponse)
def get_strategy_catalog(
    response: Response,
    _user_id: str = Depends(require_authenticated_user),
) -> StrategyCatalogResponse:
    response.headers["Cache-Control"] = "public, max-age=3600"
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
    return StrategyCatalogResponse(
        groups=groups,
        total_strategies=len(STRATEGY_CATALOG),
    )
