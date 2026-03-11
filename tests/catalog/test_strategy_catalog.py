from __future__ import annotations

from backtestforecast.schemas.backtests import StrategyType
from backtestforecast.strategy_catalog.catalog import (
    STRATEGY_CATALOG,
    StrategyCategory,
    get_catalog_entries_grouped,
)


def test_catalog_contains_all_strategy_types() -> None:
    """Every value in StrategyType enum has a catalog entry."""
    for strategy_type in StrategyType:
        assert strategy_type.value in STRATEGY_CATALOG, f"Missing catalog entry for {strategy_type.value}"


def test_catalog_has_35_entries() -> None:
    assert len(STRATEGY_CATALOG) == 35


def test_grouped_catalog_preserves_all_entries() -> None:
    grouped = get_catalog_entries_grouped()
    total = sum(len(entries) for _, entries in grouped)
    assert total == 35


def test_grouped_catalog_category_order() -> None:
    grouped = get_catalog_entries_grouped()
    categories = [cat for cat, _ in grouped]
    expected = [
        StrategyCategory.SINGLE_LEG,
        StrategyCategory.INCOME,
        StrategyCategory.VERTICAL_SPREAD,
        StrategyCategory.MULTI_LEG,
        StrategyCategory.SHORT_VOLATILITY,
        StrategyCategory.DIAGONAL,
        StrategyCategory.RATIO,
        StrategyCategory.SYNTHETIC,
        StrategyCategory.CUSTOM,
    ]
    assert categories == expected


def test_every_entry_has_required_fields() -> None:
    for key, entry in STRATEGY_CATALOG.items():
        assert entry.strategy_type == key
        assert entry.label
        assert entry.short_description
        assert entry.category
        assert entry.bias
        assert entry.leg_count >= 1
        assert entry.min_tier
        assert entry.max_loss_description
