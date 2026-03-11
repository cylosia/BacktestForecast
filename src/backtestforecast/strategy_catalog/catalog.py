"""Canonical strategy catalog with metadata for the API and frontend."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from backtestforecast.schemas.backtests import StrategyType


class StrategyCategory(str, Enum):
    SINGLE_LEG = "single_leg"
    VERTICAL_SPREAD = "vertical_spread"
    MULTI_LEG = "multi_leg"
    INCOME = "income"
    SHORT_VOLATILITY = "short_volatility"
    DIAGONAL = "diagonal"
    RATIO = "ratio"
    SYNTHETIC = "synthetic"
    CUSTOM = "custom"


class StrategyBias(str, Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"
    DIRECTIONAL = "directional"


class StrategyTier(str, Enum):
    FREE = "free"
    PRO = "pro"
    PREMIUM = "premium"


@dataclass(frozen=True, slots=True)
class StrategyCatalogEntry:
    strategy_type: str
    label: str
    short_description: str
    category: StrategyCategory
    bias: StrategyBias
    leg_count: int
    min_tier: StrategyTier
    max_loss_description: str
    notes: str = ""
    tags: tuple[str, ...] = field(default_factory=tuple)


STRATEGY_CATALOG: dict[str, StrategyCatalogEntry] = {}


def _register(*entries: StrategyCatalogEntry) -> None:
    for entry in entries:
        STRATEGY_CATALOG[entry.strategy_type] = entry


_register(
    # --- Single-leg ---
    StrategyCatalogEntry(
        strategy_type=StrategyType.LONG_CALL.value,
        label="Long Call",
        short_description="Buy a call option to profit from upward price movement.",
        category=StrategyCategory.SINGLE_LEG,
        bias=StrategyBias.BULLISH,
        leg_count=1,
        min_tier=StrategyTier.FREE,
        max_loss_description="Premium paid",
        tags=("directional", "defined-risk"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.LONG_PUT.value,
        label="Long Put",
        short_description="Buy a put option to profit from downward price movement.",
        category=StrategyCategory.SINGLE_LEG,
        bias=StrategyBias.BEARISH,
        leg_count=1,
        min_tier=StrategyTier.FREE,
        max_loss_description="Premium paid",
        tags=("directional", "defined-risk"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.NAKED_CALL.value,
        label="Naked Call",
        short_description="Sell a call option without owning the underlying. Profit from time decay or decline.",
        category=StrategyCategory.SINGLE_LEG,
        bias=StrategyBias.BEARISH,
        leg_count=1,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Theoretically unlimited",
        notes="Requires margin approval. Unlimited upside risk.",
        tags=("credit", "margin-required", "unlimited-risk"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.NAKED_PUT.value,
        label="Naked Put",
        short_description="Sell a put option without cash collateral. Profit from time decay or rally.",
        category=StrategyCategory.SINGLE_LEG,
        bias=StrategyBias.BULLISH,
        leg_count=1,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Strike price × 100 minus premium received",
        notes="Requires margin approval. Risk to zero on the underlying.",
        tags=("credit", "margin-required"),
    ),
    # --- Income ---
    StrategyCatalogEntry(
        strategy_type=StrategyType.COVERED_CALL.value,
        label="Covered Call",
        short_description="Own 100 shares and sell a call to collect premium.",
        category=StrategyCategory.INCOME,
        bias=StrategyBias.NEUTRAL,
        leg_count=2,
        min_tier=StrategyTier.FREE,
        max_loss_description="Stock decline minus premium received",
        notes="Capital-intensive due to 100-share requirement per contract.",
        tags=("income", "stock-ownership"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.CASH_SECURED_PUT.value,
        label="Cash-Secured Put",
        short_description="Sell a put backed by cash collateral to collect premium.",
        category=StrategyCategory.INCOME,
        bias=StrategyBias.BULLISH,
        leg_count=1,
        min_tier=StrategyTier.FREE,
        max_loss_description="Strike price minus premium received",
        notes="Full strike collateral is reserved.",
        tags=("income", "cash-secured"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.WHEEL.value,
        label="Wheel Strategy",
        short_description="Cycle between selling puts and covered calls on the same underlying.",
        category=StrategyCategory.INCOME,
        bias=StrategyBias.NEUTRAL,
        leg_count=2,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Stock decline minus cumulative premium",
        notes="Multi-cycle strategy; capital-intensive.",
        tags=("income", "multi-cycle"),
    ),
    # --- Vertical spreads ---
    StrategyCatalogEntry(
        strategy_type=StrategyType.BULL_CALL_DEBIT_SPREAD.value,
        label="Bull Call Spread",
        short_description="Buy a lower-strike call, sell a higher-strike call. Debit strategy.",
        category=StrategyCategory.VERTICAL_SPREAD,
        bias=StrategyBias.BULLISH,
        leg_count=2,
        min_tier=StrategyTier.FREE,
        max_loss_description="Net debit paid",
        tags=("defined-risk", "directional"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.BEAR_PUT_DEBIT_SPREAD.value,
        label="Bear Put Spread",
        short_description="Buy a higher-strike put, sell a lower-strike put. Debit strategy.",
        category=StrategyCategory.VERTICAL_SPREAD,
        bias=StrategyBias.BEARISH,
        leg_count=2,
        min_tier=StrategyTier.FREE,
        max_loss_description="Net debit paid",
        tags=("defined-risk", "directional"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.BULL_PUT_CREDIT_SPREAD.value,
        label="Bull Put Credit Spread",
        short_description="Sell a higher-strike put, buy a lower-strike put. Credit strategy.",
        category=StrategyCategory.VERTICAL_SPREAD,
        bias=StrategyBias.BULLISH,
        leg_count=2,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Spread width minus net credit",
        tags=("defined-risk", "credit"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.BEAR_CALL_CREDIT_SPREAD.value,
        label="Bear Call Credit Spread",
        short_description="Sell a lower-strike call, buy a higher-strike call. Credit strategy.",
        category=StrategyCategory.VERTICAL_SPREAD,
        bias=StrategyBias.BEARISH,
        leg_count=2,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Spread width minus net credit",
        tags=("defined-risk", "credit"),
    ),
    # --- Multi-leg ---
    StrategyCatalogEntry(
        strategy_type=StrategyType.IRON_CONDOR.value,
        label="Iron Condor",
        short_description="Sell an OTM put spread and OTM call spread for a net credit.",
        category=StrategyCategory.MULTI_LEG,
        bias=StrategyBias.NEUTRAL,
        leg_count=4,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Widest spread side minus net credit",
        tags=("defined-risk", "credit", "range-bound"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.LONG_STRADDLE.value,
        label="Long Straddle",
        short_description="Buy an ATM call and ATM put at the same strike and expiration.",
        category=StrategyCategory.MULTI_LEG,
        bias=StrategyBias.NEUTRAL,
        leg_count=2,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Total debit paid",
        tags=("volatility", "defined-risk"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.LONG_STRANGLE.value,
        label="Long Strangle",
        short_description="Buy an OTM call and OTM put with the same expiration.",
        category=StrategyCategory.MULTI_LEG,
        bias=StrategyBias.NEUTRAL,
        leg_count=2,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Total debit paid",
        tags=("volatility", "defined-risk"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.CALENDAR_SPREAD.value,
        label="Calendar Spread",
        short_description="Sell a near-term option and buy a longer-term option at the same strike.",
        category=StrategyCategory.MULTI_LEG,
        bias=StrategyBias.NEUTRAL,
        leg_count=2,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Net debit paid",
        tags=("time-decay", "defined-risk"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.BUTTERFLY.value,
        label="Butterfly Spread",
        short_description="Three-strike spread profiting from low volatility around a target price.",
        category=StrategyCategory.MULTI_LEG,
        bias=StrategyBias.NEUTRAL,
        leg_count=3,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Net debit paid",
        tags=("range-bound", "defined-risk"),
    ),
    # --- Short volatility ---
    StrategyCatalogEntry(
        strategy_type=StrategyType.SHORT_STRADDLE.value,
        label="Short Straddle",
        short_description="Sell an ATM call and ATM put at the same strike. Profit from low volatility.",
        category=StrategyCategory.SHORT_VOLATILITY,
        bias=StrategyBias.NEUTRAL,
        leg_count=2,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Theoretically unlimited",
        notes="Requires margin. Unlimited risk on both sides.",
        tags=("credit", "margin-required", "unlimited-risk"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.SHORT_STRANGLE.value,
        label="Short Strangle",
        short_description="Sell an OTM call and OTM put. Wider profit zone than short straddle.",
        category=StrategyCategory.SHORT_VOLATILITY,
        bias=StrategyBias.NEUTRAL,
        leg_count=2,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Theoretically unlimited",
        notes="Requires margin. Unlimited risk beyond strikes.",
        tags=("credit", "margin-required", "unlimited-risk"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.IRON_BUTTERFLY.value,
        label="Iron Butterfly",
        short_description="Sell an ATM straddle and buy OTM wings for protection. Defined-risk credit.",
        category=StrategyCategory.SHORT_VOLATILITY,
        bias=StrategyBias.NEUTRAL,
        leg_count=4,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Wing width minus net credit",
        tags=("defined-risk", "credit", "range-bound"),
    ),
    # --- Diagonal ---
    StrategyCatalogEntry(
        strategy_type=StrategyType.PMCC.value,
        label="Poor Man's Covered Call",
        short_description="Buy a deep ITM long-dated call, sell a short-dated OTM call against it.",
        category=StrategyCategory.DIAGONAL,
        bias=StrategyBias.BULLISH,
        leg_count=2,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Net debit paid",
        notes="Capital-efficient alternative to covered call.",
        tags=("directional", "defined-risk", "time-decay"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.DIAGONAL_SPREAD.value,
        label="Diagonal Spread",
        short_description="Buy a longer-dated call and sell a shorter-dated call at different strikes.",
        category=StrategyCategory.DIAGONAL,
        bias=StrategyBias.BULLISH,
        leg_count=2,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Net debit paid",
        tags=("directional", "time-decay"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.DOUBLE_DIAGONAL.value,
        label="Double Diagonal",
        short_description="Diagonal spread on both the call and put side. Profits from time decay.",
        category=StrategyCategory.DIAGONAL,
        bias=StrategyBias.NEUTRAL,
        leg_count=4,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Net debit paid",
        tags=("time-decay", "neutral"),
    ),
    # --- Ratio ---
    StrategyCatalogEntry(
        strategy_type=StrategyType.RATIO_CALL_BACKSPREAD.value,
        label="Ratio Call Backspread",
        short_description="Sell 1 lower call, buy 2 higher calls. Profits from large upside moves.",
        category=StrategyCategory.RATIO,
        bias=StrategyBias.BULLISH,
        leg_count=3,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Spread width plus net debit at short strike",
        tags=("directional", "volatility"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.RATIO_PUT_BACKSPREAD.value,
        label="Ratio Put Backspread",
        short_description="Sell 1 higher put, buy 2 lower puts. Profits from large downside moves.",
        category=StrategyCategory.RATIO,
        bias=StrategyBias.BEARISH,
        leg_count=3,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Spread width plus net debit at short strike",
        tags=("directional", "volatility"),
    ),
    # --- Income (new) ---
    StrategyCatalogEntry(
        strategy_type=StrategyType.COLLAR.value,
        label="Collar",
        short_description="Own 100 shares, sell OTM call, buy OTM put. Defined range of outcomes.",
        category=StrategyCategory.INCOME,
        bias=StrategyBias.NEUTRAL,
        leg_count=3,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Stock decline to put strike minus net credit/debit",
        notes="Capital-intensive due to 100-share requirement.",
        tags=("stock-ownership", "defined-risk"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.COVERED_STRANGLE.value,
        label="Covered Strangle",
        short_description="Own 100 shares, sell OTM call and OTM put. Enhanced income.",
        category=StrategyCategory.INCOME,
        bias=StrategyBias.NEUTRAL,
        leg_count=3,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Stock decline minus credit; margin on put side",
        notes="Requires margin for the short put.",
        tags=("stock-ownership", "credit", "margin-required"),
    ),
    # --- Synthetic ---
    StrategyCatalogEntry(
        strategy_type=StrategyType.SYNTHETIC_PUT.value,
        label="Synthetic Put",
        short_description="Short 100 shares + buy ATM call. Behaves like a long put.",
        category=StrategyCategory.SYNTHETIC,
        bias=StrategyBias.BEARISH,
        leg_count=2,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Premium paid plus gap between strike and entry",
        notes="Requires margin for short stock.",
        tags=("directional", "margin-required"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.REVERSE_CONVERSION.value,
        label="Reverse Conversion",
        short_description="Short 100 shares + buy ATM call + sell ATM put. Arbitrage-style.",
        category=StrategyCategory.SYNTHETIC,
        bias=StrategyBias.NEUTRAL,
        leg_count=3,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Net option cost plus stock-strike gap",
        notes="Requires margin for short stock.",
        tags=("arbitrage", "margin-required"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.JADE_LIZARD.value,
        label="Jade Lizard",
        short_description="Short OTM put + short OTM call spread. No upside risk if credit exceeds call width.",
        category=StrategyCategory.SYNTHETIC,
        bias=StrategyBias.NEUTRAL,
        leg_count=3,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Downside: put strike minus credit",
        tags=("credit", "neutral"),
    ),
    # --- Custom ---
    StrategyCatalogEntry(
        strategy_type=StrategyType.CUSTOM_2_LEG.value,
        label="Custom 2-Leg",
        short_description="Define any 2-leg option/stock combination with custom strikes and expirations.",
        category=StrategyCategory.CUSTOM,
        bias=StrategyBias.DIRECTIONAL,
        leg_count=2,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Depends on configuration",
        tags=("custom", "advanced"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.CUSTOM_3_LEG.value,
        label="Custom 3-Leg",
        short_description="Define any 3-leg option/stock combination.",
        category=StrategyCategory.CUSTOM,
        bias=StrategyBias.DIRECTIONAL,
        leg_count=3,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Depends on configuration",
        tags=("custom", "advanced"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.CUSTOM_4_LEG.value,
        label="Custom 4-Leg",
        short_description="Define any 4-leg option/stock combination.",
        category=StrategyCategory.CUSTOM,
        bias=StrategyBias.DIRECTIONAL,
        leg_count=4,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Depends on configuration",
        tags=("custom", "advanced"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.CUSTOM_5_LEG.value,
        label="Custom 5-Leg",
        short_description="Define any 5-leg option/stock combination.",
        category=StrategyCategory.CUSTOM,
        bias=StrategyBias.DIRECTIONAL,
        leg_count=5,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Depends on configuration",
        tags=("custom", "advanced"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.CUSTOM_6_LEG.value,
        label="Custom 6-Leg",
        short_description="Define any 6-leg option/stock combination.",
        category=StrategyCategory.CUSTOM,
        bias=StrategyBias.DIRECTIONAL,
        leg_count=6,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Depends on configuration",
        tags=("custom", "advanced"),
    ),
    StrategyCatalogEntry(
        strategy_type=StrategyType.CUSTOM_8_LEG.value,
        label="Custom 8-Leg",
        short_description="Define any 8-leg option/stock combination. Maximum complexity.",
        category=StrategyCategory.CUSTOM,
        bias=StrategyBias.DIRECTIONAL,
        leg_count=8,
        min_tier=StrategyTier.PREMIUM,
        max_loss_description="Depends on configuration",
        tags=("custom", "advanced"),
    ),
)


CATEGORY_ORDER = [
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

CATEGORY_LABELS = {
    StrategyCategory.SINGLE_LEG: "Single-leg",
    StrategyCategory.INCOME: "Income strategies",
    StrategyCategory.VERTICAL_SPREAD: "Vertical spreads",
    StrategyCategory.MULTI_LEG: "Multi-leg",
    StrategyCategory.SHORT_VOLATILITY: "Short volatility",
    StrategyCategory.DIAGONAL: "Diagonal & calendar",
    StrategyCategory.RATIO: "Ratio spreads",
    StrategyCategory.SYNTHETIC: "Synthetic & exotic",
    StrategyCategory.CUSTOM: "Custom strategies",
}


def get_catalog_entries_grouped() -> list[tuple[StrategyCategory, list[StrategyCatalogEntry]]]:
    """Return catalog entries grouped by category in display order."""
    groups: dict[StrategyCategory, list[StrategyCatalogEntry]] = {cat: [] for cat in CATEGORY_ORDER}
    for entry in STRATEGY_CATALOG.values():
        groups[entry.category].append(entry)
    return [(cat, entries) for cat, entries in groups.items() if entries]
