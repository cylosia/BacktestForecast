from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backtestforecast.config import get_settings

SYMBOL_ALLOWED_CHARS = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-")


class StrategyType(str, Enum):
    LONG_CALL = "long_call"
    LONG_PUT = "long_put"
    COVERED_CALL = "covered_call"
    CASH_SECURED_PUT = "cash_secured_put"
    BULL_CALL_DEBIT_SPREAD = "bull_call_debit_spread"
    BEAR_PUT_DEBIT_SPREAD = "bear_put_debit_spread"
    BULL_PUT_CREDIT_SPREAD = "bull_put_credit_spread"
    BEAR_CALL_CREDIT_SPREAD = "bear_call_credit_spread"
    IRON_CONDOR = "iron_condor"
    LONG_STRADDLE = "long_straddle"
    LONG_STRANGLE = "long_strangle"
    CALENDAR_SPREAD = "calendar_spread"
    BUTTERFLY = "butterfly"
    WHEEL = "wheel_strategy"
    # --- New strategies ---
    PMCC = "poor_mans_covered_call"
    RATIO_CALL_BACKSPREAD = "ratio_call_backspread"
    RATIO_PUT_BACKSPREAD = "ratio_put_backspread"
    COLLAR = "collar"
    DIAGONAL_SPREAD = "diagonal_spread"
    DOUBLE_DIAGONAL = "double_diagonal"
    SHORT_STRADDLE = "short_straddle"
    SHORT_STRANGLE = "short_strangle"
    COVERED_STRANGLE = "covered_strangle"
    SYNTHETIC_PUT = "synthetic_put"
    REVERSE_CONVERSION = "reverse_conversion"
    JADE_LIZARD = "jade_lizard"
    IRON_BUTTERFLY = "iron_butterfly"
    CUSTOM_2_LEG = "custom_2_leg"
    CUSTOM_3_LEG = "custom_3_leg"
    CUSTOM_4_LEG = "custom_4_leg"
    CUSTOM_5_LEG = "custom_5_leg"
    CUSTOM_6_LEG = "custom_6_leg"
    CUSTOM_8_LEG = "custom_8_leg"
    NAKED_CALL = "naked_call"
    NAKED_PUT = "naked_put"


class ComparisonOperator(str, Enum):
    LT = "lt"
    LTE = "lte"
    GT = "gt"
    GTE = "gte"


class CrossoverDirection(str, Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"


class BollingerBand(str, Enum):
    LOWER = "lower"
    MIDDLE = "middle"
    UPPER = "upper"


class SupportResistanceMode(str, Enum):
    NEAR_SUPPORT = "near_support"
    NEAR_RESISTANCE = "near_resistance"
    BREAKOUT_ABOVE_RESISTANCE = "breakout_above_resistance"
    BREAKDOWN_BELOW_SUPPORT = "breakdown_below_support"


class RunStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RsiRule(BaseModel):
    type: Literal["rsi"]
    operator: ComparisonOperator
    threshold: Decimal = Field(ge=0, le=100)
    period: int = Field(default=14, ge=2, le=100)


class MovingAverageCrossoverRule(BaseModel):
    type: Literal["sma_crossover", "ema_crossover"]
    fast_period: int = Field(ge=2, le=200)
    slow_period: int = Field(ge=3, le=400)
    direction: CrossoverDirection

    @model_validator(mode="after")
    def validate_period_order(self) -> "MovingAverageCrossoverRule":
        if self.fast_period >= self.slow_period:
            raise ValueError("fast_period must be less than slow_period")
        return self


class MacdRule(BaseModel):
    type: Literal["macd"]
    direction: CrossoverDirection
    fast_period: int = Field(default=12, ge=2, le=100)
    slow_period: int = Field(default=26, ge=3, le=200)
    signal_period: int = Field(default=9, ge=2, le=100)

    @model_validator(mode="after")
    def validate_period_order(self) -> "MacdRule":
        if self.fast_period >= self.slow_period:
            raise ValueError("fast_period must be less than slow_period")
        return self


class BollingerBandsRule(BaseModel):
    type: Literal["bollinger_bands"]
    band: BollingerBand
    operator: ComparisonOperator
    period: int = Field(default=20, ge=5, le=200)
    standard_deviations: Decimal = Field(default=Decimal("2"), gt=0, le=Decimal("5"))


class IvRankRule(BaseModel):
    type: Literal["iv_rank"]
    operator: ComparisonOperator
    threshold: Decimal = Field(ge=0, le=100)
    lookback_days: int = Field(default=252, ge=20, le=756)


class IvPercentileRule(BaseModel):
    type: Literal["iv_percentile"]
    operator: ComparisonOperator
    threshold: Decimal = Field(ge=0, le=100)
    lookback_days: int = Field(default=252, ge=20, le=756)


class VolumeSpikeRule(BaseModel):
    type: Literal["volume_spike"]
    operator: ComparisonOperator = Field(default=ComparisonOperator.GTE)
    multiplier: Decimal = Field(default=Decimal("1.5"), gt=0, le=Decimal("20"))
    lookback_period: int = Field(default=20, ge=2, le=252)


class SupportResistanceRule(BaseModel):
    type: Literal["support_resistance"]
    mode: SupportResistanceMode
    lookback_period: int = Field(default=20, ge=5, le=252)
    tolerance_pct: Decimal = Field(default=Decimal("1.0"), gt=0, le=Decimal("10.0"))


class AvoidEarningsRule(BaseModel):
    type: Literal["avoid_earnings"]
    days_before: int = Field(default=0, ge=0, le=30)
    days_after: int = Field(default=0, ge=0, le=30)


EntryRule = Annotated[
    RsiRule
    | MovingAverageCrossoverRule
    | MacdRule
    | BollingerBandsRule
    | IvRankRule
    | IvPercentileRule
    | VolumeSpikeRule
    | SupportResistanceRule
    | AvoidEarningsRule,
    Field(discriminator="type"),
]


def rule_bias(rule: EntryRule) -> str | None:
    if isinstance(rule, (MovingAverageCrossoverRule, MacdRule)):
        return rule.direction.value
    if isinstance(rule, SupportResistanceRule):
        mapping = {
            SupportResistanceMode.NEAR_SUPPORT: "bullish",
            SupportResistanceMode.BREAKOUT_ABOVE_RESISTANCE: "bullish",
            SupportResistanceMode.NEAR_RESISTANCE: "bearish",
            SupportResistanceMode.BREAKDOWN_BELOW_SUPPORT: "bearish",
        }
        return mapping[rule.mode]
    return None


def rule_direction(rule: EntryRule) -> str | None:
    if isinstance(rule, (MovingAverageCrossoverRule, MacdRule)):
        return rule.direction.value
    if isinstance(rule, SupportResistanceRule):
        return rule.mode.value
    return None


def rule_conflict_key(rule: EntryRule) -> tuple[Any, ...] | None:
    if isinstance(rule, MovingAverageCrossoverRule):
        return (rule.type, rule.fast_period, rule.slow_period)
    if isinstance(rule, MacdRule):
        return (rule.type, rule.fast_period, rule.slow_period, rule.signal_period)
    if isinstance(rule, SupportResistanceRule):
        return (rule.type, rule.lookback_period)
    return None


def validate_entry_rule_collection(entry_rules: list[EntryRule]) -> None:
    directional_biases = {bias for bias in (rule_bias(rule) for rule in entry_rules) if bias is not None}
    if len(directional_biases) > 1:
        raise ValueError(
            "entry_rules contain conflicting directional signals; "
            "use either bullish-only or bearish-only trend rules in a single request"
        )

    directional_rule_keys: dict[tuple[Any, ...], str] = {}
    for rule in entry_rules:
        key = rule_conflict_key(rule)
        if key is None:
            continue
        direction = rule_direction(rule)
        if direction is None:
            continue
        existing = directional_rule_keys.get(key)
        if existing is not None and existing != direction:
            raise ValueError(f"conflicting rules detected for {key[0]}")
        directional_rule_keys[key] = direction


CUSTOM_STRATEGY_TYPES = {
    StrategyType.CUSTOM_2_LEG,
    StrategyType.CUSTOM_3_LEG,
    StrategyType.CUSTOM_4_LEG,
    StrategyType.CUSTOM_5_LEG,
    StrategyType.CUSTOM_6_LEG,
    StrategyType.CUSTOM_8_LEG,
}

CUSTOM_LEG_COUNT = {
    StrategyType.CUSTOM_2_LEG: 2,
    StrategyType.CUSTOM_3_LEG: 3,
    StrategyType.CUSTOM_4_LEG: 4,
    StrategyType.CUSTOM_5_LEG: 5,
    StrategyType.CUSTOM_6_LEG: 6,
    StrategyType.CUSTOM_8_LEG: 8,
}


class CustomLegDefinition(BaseModel):
    """User-defined leg for custom N-leg strategies."""

    asset_type: Literal["option", "stock"] = "option"
    contract_type: Literal["call", "put"] | None = Field(default=None)
    side: Literal["long", "short"]
    strike_offset: int = Field(
        default=0,
        ge=-20,
        le=20,
        description="Strike offset from ATM in listed-strike steps. 0=ATM, +1=one OTM call step, -1=one OTM put step.",
    )
    expiration_offset: int = Field(
        default=0,
        ge=0,
        le=2,
        description="0=primary expiration, 1=next available, 2=second-next.",
    )
    quantity_ratio: int = Field(default=1, ge=1, le=10)

    @model_validator(mode="after")
    def validate_leg(self) -> "CustomLegDefinition":
        if self.asset_type == "option" and self.contract_type is None:
            raise ValueError("contract_type is required for option legs")
        if self.asset_type == "stock" and self.contract_type is not None:
            raise ValueError("contract_type must be null for stock legs")
        return self


# ---------------------------------------------------------------------------
# Strike selection and spread width configuration
# ---------------------------------------------------------------------------


class StrikeSelectionMode(str, Enum):
    """How to choose a strike for a strategy leg."""

    NEAREST_OTM = "nearest_otm"
    """First listed strike OTM (default, current behavior)."""

    PCT_FROM_SPOT = "pct_from_spot"
    """Place at spot × (1 ± value/100). E.g., value=5 → 5% above/below spot."""

    ATM_OFFSET_STEPS = "atm_offset_steps"
    """N listed-strike increments from ATM. E.g., value=2 → two strikes OTM."""

    DELTA_TARGET = "delta_target"
    """Target absolute delta. E.g., value=30 → ~30Δ. Uses BSM approximation (no provider greeks required)."""


class StrikeSelection(BaseModel):
    """Configuration for where a strategy leg's strike is placed relative to the underlying."""

    mode: StrikeSelectionMode = StrikeSelectionMode.NEAREST_OTM
    value: Decimal | None = Field(
        default=None,
        description=(
            "Interpretation depends on mode: pct_from_spot → percentage, "
            "atm_offset_steps → integer steps, delta_target → absolute delta (0-100)."
        ),
    )

    @model_validator(mode="after")
    def validate_selection(self) -> "StrikeSelection":
        if self.mode != StrikeSelectionMode.NEAREST_OTM and self.value is None:
            raise ValueError(f"value is required when mode is {self.mode.value}")
        if self.mode == StrikeSelectionMode.PCT_FROM_SPOT and self.value is not None:
            if self.value < 0 or self.value > 50:
                raise ValueError("pct_from_spot value must be between 0 and 50")
        if self.mode == StrikeSelectionMode.ATM_OFFSET_STEPS and self.value is not None:
            if self.value < 0 or self.value > 20:
                raise ValueError("atm_offset_steps value must be between 0 and 20")
        if self.mode == StrikeSelectionMode.DELTA_TARGET and self.value is not None:
            if self.value < 1 or self.value > 99:
                raise ValueError("delta_target value must be between 1 and 99")
        return self


class SpreadWidthMode(str, Enum):
    """How to determine the width of a spread (distance between short and long strikes)."""

    STRIKE_STEPS = "strike_steps"
    """N listed-strike increments (default: 1)."""

    DOLLAR_WIDTH = "dollar_width"
    """Fixed dollar amount (e.g., 5 → $5 wide)."""

    PCT_WIDTH = "pct_width"
    """Percentage of the underlying (e.g., 3 → 3% wide)."""


class SpreadWidthConfig(BaseModel):
    """Configuration for how wide a spread's wings are."""

    mode: SpreadWidthMode = SpreadWidthMode.STRIKE_STEPS
    value: Decimal = Field(default=Decimal("1"), gt=0)

    @model_validator(mode="after")
    def validate_width(self) -> "SpreadWidthConfig":
        if self.mode == SpreadWidthMode.STRIKE_STEPS:
            if self.value < 1 or self.value > 20:
                raise ValueError("strike_steps width must be between 1 and 20")
        if self.mode == SpreadWidthMode.DOLLAR_WIDTH:
            if self.value < Decimal("0.5") or self.value > Decimal("100"):
                raise ValueError("dollar_width must be between 0.50 and 100")
        if self.mode == SpreadWidthMode.PCT_WIDTH:
            if self.value < Decimal("0.5") or self.value > Decimal("30"):
                raise ValueError("pct_width must be between 0.5 and 30")
        return self


class StrategyOverrides(BaseModel):
    """Optional overrides for how a strategy selects strikes and widths.

    Named fields correspond to standard leg roles. Strategies ignore fields
    that don't apply to them (e.g., a long call ignores ``short_call_strike``).
    """

    short_call_strike: StrikeSelection | None = Field(default=None, description="Override short call placement")
    short_put_strike: StrikeSelection | None = Field(default=None, description="Override short put placement")
    long_call_strike: StrikeSelection | None = Field(
        default=None, description="Override long call placement (for diagonals, PMCC)"
    )
    long_put_strike: StrikeSelection | None = Field(default=None, description="Override long put placement")
    spread_width: SpreadWidthConfig | None = Field(
        default=None, description="Override wing/spread width for strategies with protection legs"
    )


class CreateBacktestRunRequest(BaseModel):
    symbol: str = Field(min_length=1, max_length=16)
    strategy_type: StrategyType
    start_date: date
    end_date: date
    target_dte: int = Field(ge=7, le=365)
    dte_tolerance_days: int = Field(default=5, ge=0, le=60)
    max_holding_days: int = Field(ge=1, le=120)
    account_size: Decimal = Field(gt=0, le=Decimal("100000000"))
    risk_per_trade_pct: Decimal = Field(gt=0, le=100)
    commission_per_contract: Decimal = Field(ge=0, le=Decimal("100"))
    entry_rules: list[EntryRule] = Field(default_factory=list, max_length=8)
    idempotency_key: str | None = Field(default=None, max_length=80)
    custom_legs: list[CustomLegDefinition] | None = Field(default=None, max_length=8)
    strategy_overrides: StrategyOverrides | None = Field(
        default=None, description="Optional overrides for strike placement and spread width"
    )

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized or any(char not in SYMBOL_ALLOWED_CHARS for char in normalized):
            raise ValueError("symbol must contain only letters, digits, '.', or '-'")
        if not normalized[0].isalpha():
            raise ValueError("symbol must start with a letter")
        return normalized

    @model_validator(mode="after")
    def validate_request(self) -> "CreateBacktestRunRequest":
        from datetime import UTC, datetime as _dt
        if self.end_date > _dt.now(UTC).date():
            raise ValueError("end_date cannot be in the future.")
        if self.start_date >= self.end_date:
            raise ValueError("start_date must be earlier than end_date")
        if (self.end_date - self.start_date).days > get_settings().max_backtest_window_days:
            raise ValueError(
                f"backtest window exceeds the configured maximum of {get_settings().max_backtest_window_days} days"
            )
        validate_entry_rule_collection(self.entry_rules)

        if self.strategy_type in CUSTOM_STRATEGY_TYPES:
            expected = CUSTOM_LEG_COUNT[self.strategy_type]
            if not self.custom_legs:
                raise ValueError(f"{self.strategy_type.value} requires exactly {expected} custom_legs definitions")
            if len(self.custom_legs) != expected:
                raise ValueError(
                    f"{self.strategy_type.value} requires exactly {expected} legs, got {len(self.custom_legs)}"
                )
        elif self.custom_legs:
            raise ValueError("custom_legs should only be provided for custom_N_leg strategy types")

        return self


class FeatureAccessResponse(BaseModel):
    plan_tier: str
    monthly_backtest_quota: int | None = None
    history_days: int | None = None
    history_item_limit: int
    side_by_side_comparison_limit: int
    forecasting_access: bool
    export_formats: list[str] = Field(default_factory=list)
    scanner_modes: list[str] = Field(default_factory=list)


class UsageSummaryResponse(BaseModel):
    backtests_used_this_month: int = 0
    backtests_remaining_this_month: int | None = None


class CurrentUserResponse(BaseModel):
    id: UUID
    clerk_user_id: str
    email: str | None
    plan_tier: str
    subscription_status: str | None = None
    subscription_billing_interval: str | None = None
    subscription_current_period_end: datetime | None = None
    cancel_at_period_end: bool = False
    created_at: datetime
    features: FeatureAccessResponse
    usage: UsageSummaryResponse

    model_config = ConfigDict(from_attributes=True)


class BacktestSummaryResponse(BaseModel):
    trade_count: int
    win_rate: Decimal
    total_roi_pct: Decimal
    average_win_amount: Decimal
    average_loss_amount: Decimal
    average_holding_period_days: Decimal
    average_dte_at_open: Decimal
    max_drawdown_pct: Decimal
    total_commissions: Decimal
    total_net_pnl: Decimal
    starting_equity: Decimal
    ending_equity: Decimal


class BacktestTradeResponse(BaseModel):
    id: UUID | None = None
    option_ticker: str
    strategy_type: str
    underlying_symbol: str
    entry_date: date
    exit_date: date
    expiration_date: date
    quantity: int
    dte_at_open: int
    holding_period_days: int
    entry_underlying_close: Decimal
    exit_underlying_close: Decimal
    entry_mid: Decimal
    exit_mid: Decimal
    gross_pnl: Decimal
    net_pnl: Decimal
    total_commissions: Decimal
    entry_reason: str
    exit_reason: str
    detail_json: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(from_attributes=True)


class EquityCurvePointResponse(BaseModel):
    trade_date: date
    equity: Decimal
    cash: Decimal
    position_value: Decimal
    drawdown_pct: Decimal

    model_config = ConfigDict(from_attributes=True)


class BacktestRunHistoryItemResponse(BaseModel):
    id: UUID
    symbol: str
    strategy_type: str
    status: RunStatus
    date_from: date
    date_to: date
    target_dte: int
    max_holding_days: int
    created_at: datetime
    completed_at: datetime | None
    summary: BacktestSummaryResponse


class BacktestRunDetailResponse(BaseModel):
    id: UUID
    symbol: str
    strategy_type: str
    status: RunStatus
    date_from: date
    date_to: date
    target_dte: int
    dte_tolerance_days: int
    max_holding_days: int
    account_size: Decimal
    risk_per_trade_pct: Decimal
    commission_per_contract: Decimal
    engine_version: str
    data_source: str
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None
    warnings: list[dict[str, Any]]
    error_code: str | None = None
    error_message: str | None = None
    summary: BacktestSummaryResponse
    trades: list[BacktestTradeResponse]
    equity_curve: list[EquityCurvePointResponse]


class BacktestRunStatusResponse(BaseModel):
    id: UUID
    status: RunStatus
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error_code: str | None = None
    error_message: str | None = None


class BacktestRunListResponse(BaseModel):
    items: list[BacktestRunHistoryItemResponse]


class CompareBacktestsRequest(BaseModel):
    run_ids: list[UUID] = Field(min_length=2, max_length=10)

    @field_validator("run_ids")
    @classmethod
    def validate_unique_ids(cls, value: list[UUID]) -> list[UUID]:
        if len(set(value)) != len(value):
            raise ValueError("run_ids must contain unique values")
        return value


class CompareBacktestsResponse(BaseModel):
    items: list[BacktestRunDetailResponse]
    comparison_limit: int
