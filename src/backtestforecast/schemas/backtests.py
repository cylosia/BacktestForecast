from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backtestforecast.config import get_settings
from backtestforecast.schemas.common import JobStatus, PlanTier, RunJobStatus, sanitize_error_message

SYMBOL_ALLOWED_CHARS = re.compile(r"^[\^A-Z][A-Z0-9./^-]{0,15}$")


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
    CUSTOM_7_LEG = "custom_7_leg"
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


RunStatus = RunJobStatus


class RsiRule(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["rsi"]
    operator: ComparisonOperator
    threshold: Decimal = Field(ge=0, le=100)
    period: int = Field(default=14, ge=2, le=100)


class MovingAverageCrossoverRule(BaseModel):
    model_config = ConfigDict(extra="forbid")
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
    model_config = ConfigDict(extra="forbid")
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
    model_config = ConfigDict(extra="forbid")
    type: Literal["bollinger_bands"]
    band: BollingerBand
    operator: ComparisonOperator
    period: int = Field(default=20, ge=5, le=200)
    standard_deviations: Decimal = Field(default=Decimal("2"), gt=0, le=Decimal("5"))


class IvRankRule(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["iv_rank"]
    operator: ComparisonOperator
    threshold: Decimal = Field(ge=0, le=100)
    lookback_days: int = Field(default=252, ge=20, le=756)


class IvPercentileRule(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["iv_percentile"]
    operator: ComparisonOperator
    threshold: Decimal = Field(ge=0, le=100)
    lookback_days: int = Field(default=252, ge=20, le=756)


class VolumeSpikeRule(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["volume_spike"]
    operator: ComparisonOperator = Field(default=ComparisonOperator.GTE)
    multiplier: Decimal = Field(default=Decimal("1.5"), gt=0, le=Decimal("20"))
    lookback_period: int = Field(default=20, ge=2, le=252)


class SupportResistanceRule(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["support_resistance"]
    mode: SupportResistanceMode
    lookback_period: int = Field(default=20, ge=5, le=252)
    tolerance_pct: Decimal = Field(default=Decimal("1.0"), gt=0, le=Decimal("10.0"))


class AvoidEarningsRule(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["avoid_earnings"]
    days_before: int = Field(default=0, ge=0, le=30)
    days_after: int = Field(default=0, ge=0, le=30)

    @model_validator(mode="after")
    def validate_non_zero_window(self) -> "AvoidEarningsRule":
        if self.days_before == 0 and self.days_after == 0:
            raise ValueError("At least one of days_before or days_after must be > 0")
        return self


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
    StrategyType.CUSTOM_7_LEG,
    StrategyType.CUSTOM_8_LEG,
}

CUSTOM_LEG_COUNT = {
    StrategyType.CUSTOM_2_LEG: 2,
    StrategyType.CUSTOM_3_LEG: 3,
    StrategyType.CUSTOM_4_LEG: 4,
    StrategyType.CUSTOM_5_LEG: 5,
    StrategyType.CUSTOM_6_LEG: 6,
    StrategyType.CUSTOM_7_LEG: 7,
    StrategyType.CUSTOM_8_LEG: 8,
}


class CustomLegDefinition(BaseModel):
    """User-defined leg for custom N-leg strategies."""
    model_config = ConfigDict(extra="forbid")

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
    quantity_ratio: Decimal = Field(default=Decimal("1.0"), ge=Decimal("0.1"), le=Decimal("10.0"))

    @model_validator(mode="after")
    def validate_leg(self) -> "CustomLegDefinition":
        if self.asset_type == "option" and self.contract_type is None:
            raise ValueError("contract_type is required for option legs")
        if self.asset_type == "stock":
            if self.contract_type is not None:
                raise ValueError("contract_type must be null for stock legs")
            if self.expiration_offset != 0:
                raise ValueError("expiration_offset must be 0 for stock legs")
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
    model_config = ConfigDict(extra="forbid")

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


def validate_spread_width(mode: SpreadWidthMode, value: Decimal) -> None:
    """Shared validation for spread width bounds. Used by both SpreadWidthConfig and WidthGridItem."""
    if mode == SpreadWidthMode.STRIKE_STEPS:
        if value < 1 or value > 20:
            raise ValueError("strike_steps width must be between 1 and 20")
    elif mode == SpreadWidthMode.DOLLAR_WIDTH:
        if value < Decimal("0.5") or value > Decimal("100"):
            raise ValueError("dollar_width must be between 0.50 and 100")
    elif mode == SpreadWidthMode.PCT_WIDTH:
        if value < Decimal("0.5") or value > Decimal("30"):
            raise ValueError("pct_width must be between 0.5 and 30")


class SpreadWidthConfig(BaseModel):
    """Configuration for how wide a spread's wings are."""
    model_config = ConfigDict(extra="forbid")

    mode: SpreadWidthMode = SpreadWidthMode.STRIKE_STEPS
    value: Decimal = Field(default=Decimal("1"), gt=0)

    @model_validator(mode="after")
    def validate_width(self) -> "SpreadWidthConfig":
        validate_spread_width(self.mode, self.value)
        return self


class StrategyOverrides(BaseModel):
    """Optional overrides for how a strategy selects strikes and widths.

    Named fields correspond to standard leg roles. Strategies ignore fields
    that don't apply to them (e.g., a long call ignores ``short_call_strike``).
    """
    model_config = ConfigDict(extra="forbid")

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
    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1, max_length=16, description="Ticker symbol (e.g., AAPL, SPY). Uppercase letters, dots, and slashes allowed.")
    strategy_type: StrategyType
    start_date: date = Field(description="Backtest start date. Maximum window is 1825 days (5 years) from end_date.")
    end_date: date = Field(description="Backtest end date. Must be after start_date.")
    target_dte: int = Field(ge=1, le=365)
    dte_tolerance_days: int = Field(default=5, ge=0, le=60)
    max_holding_days: int = Field(ge=1, le=120, description="Maximum holding period in trading days (weekdays only). 30 trading days ≈ 6 calendar weeks.")
    account_size: Decimal = Field(ge=Decimal("100"), le=Decimal("100000000"))
    risk_per_trade_pct: Decimal = Field(gt=0, le=100)
    commission_per_contract: Decimal = Field(ge=0, le=Decimal("100"))
    entry_rules: list[EntryRule] = Field(min_length=1, max_length=8)
    idempotency_key: str | None = Field(default=None, min_length=4, max_length=80)
    custom_legs: list[CustomLegDefinition] | None = Field(default=None, max_length=8)
    slippage_pct: Decimal = Field(default=Decimal("0"), ge=Decimal("0"), le=Decimal("5"), description="Slippage percentage applied to entry and exit prices.")
    profit_target_pct: Decimal | None = Field(
        default=None, ge=Decimal("1"), le=Decimal("500"),
        description="Close position when unrealized profit reaches this percentage of capital at risk. None disables.",
    )
    stop_loss_pct: Decimal | None = Field(
        default=None, ge=Decimal("1"), le=Decimal("100"),
        description="Close position when unrealized loss reaches this percentage of capital at risk. None disables.",
    )
    strategy_overrides: StrategyOverrides | None = Field(
        default=None, description="Optional overrides for strike placement and spread width"
    )
    risk_free_rate: Decimal | None = Field(
        default=None, ge=Decimal("0"), le=Decimal("0.20"),
        description="Annualized risk-free rate for Sharpe/Sortino calculations. "
                    "If not provided, uses the server default (see /v1/meta). "
                    "Set to 0.0 for ZIRP-era backtests.",
    )
    dividend_yield: Decimal | None = Field(
        default=None, ge=Decimal("0"), le=Decimal("0.15"),
        description="Annualized dividend yield for BSM IV estimation (e.g. 0.03 = 3%). "
                    "Improves IV accuracy for high-yield stocks. Defaults to 0.0.",
    )

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not SYMBOL_ALLOWED_CHARS.match(normalized):
            raise ValueError("symbol must contain only letters, digits, '.', '/', '^', or '-'")
        return normalized

    @model_validator(mode="after")
    def validate_request(self) -> "CreateBacktestRunRequest":
        from backtestforecast.utils.dates import market_date_today
        if self.end_date > market_date_today():
            raise ValueError("end_date cannot be in the future (US Eastern time).")
        if self.start_date >= self.end_date:
            raise ValueError("start_date must be earlier than end_date")
        if self.dte_tolerance_days >= self.target_dte:
            raise ValueError(
                f"dte_tolerance_days ({self.dte_tolerance_days}) must be less than "
                f"target_dte ({self.target_dte}). Reduce tolerance or increase target DTE."
            )
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

        if self.custom_legs:
            long_count = sum(1 for leg in self.custom_legs if leg.side == "long")
            short_count = sum(1 for leg in self.custom_legs if leg.side == "short")
            if long_count == 0 or short_count == 0:
                raise ValueError("custom_legs must contain at least one long and one short leg")

        return self


class FeatureAccessResponse(BaseModel):
    plan_tier: PlanTier
    monthly_backtest_quota: int | None = None
    history_days: int | None = None
    history_item_limit: int
    side_by_side_comparison_limit: int
    forecasting_access: bool
    export_formats: list[str] = Field(default_factory=list)
    scanner_modes: list[str] = Field(default_factory=list)
    cancel_at_period_end: bool = False


class UsageSummaryResponse(BaseModel):
    backtests_used_this_month: int = 0
    backtests_remaining_this_month: int | None = None


class CurrentUserResponse(BaseModel):
    """Response schema for the authenticated user endpoint.

    ``features`` and ``usage`` are computed fields that do not correspond to
    database columns on the User model.  Callers must construct this response
    explicitly (see ``BacktestService.to_current_user_response``) rather than
    passing an ORM User object directly — ``from_attributes`` is enabled only
    for the flat scalar fields inherited from the User row.
    """

    id: UUID
    clerk_user_id: str
    email: str | None
    plan_tier: PlanTier
    subscription_status: str | None = None
    subscription_billing_interval: str | None = Field(
        default=None, pattern=r"^(monthly|yearly)$",
    )
    subscription_current_period_end: datetime | None = None
    cancel_at_period_end: bool = False
    created_at: datetime
    features: FeatureAccessResponse
    usage: UsageSummaryResponse

    model_config = ConfigDict(from_attributes=True)


class BacktestSummaryResponse(BaseModel):
    """Backtest summary metrics.

    DUAL-PURPOSE: This schema is used for BOTH ORM mapping (``from_attributes=True``
    on BacktestRunDetailResponse) AND manual construction from JSON blobs
    (scanner/sweep ``summary_json`` fields). Fields like ``decided_trades``
    that don't exist on the ORM model will silently default to ``None``.
    Do not add validators that assume ORM context — they will break the
    JSON construction path.
    """
    # NOTE: Used both for ORM mapping (BacktestRun detail) and JSON blob
    # parsing (scanner/sweep summary_json). Fields not on the ORM model
    # (e.g. decided_trades) must have defaults so from_attributes works.
    model_config = ConfigDict(from_attributes=True)

    trade_count: int
    decided_trades: int | None = Field(
        default=None,
        description="Trades with non-zero net P&L. Win rate denominator. "
                    "Equals trade_count minus break-even trades.",
    )
    win_rate: Decimal = Decimal("0")
    total_roi_pct: Decimal = Decimal("0")
    average_win_amount: Decimal = Decimal("0")
    average_loss_amount: Decimal = Decimal("0")
    average_holding_period_days: Decimal = Decimal("0")
    average_dte_at_open: Decimal = Decimal("0")
    max_drawdown_pct: Decimal = Decimal("0")
    total_commissions: Decimal
    total_net_pnl: Decimal
    starting_equity: Decimal
    ending_equity: Decimal
    profit_factor: Decimal | None = None
    payoff_ratio: Decimal | None = None
    expectancy: Decimal = Decimal("0")
    sharpe_ratio: Decimal | None = None
    sortino_ratio: Decimal | None = None
    cagr_pct: Decimal | None = None
    calmar_ratio: Decimal | None = None
    max_consecutive_wins: int = 0
    max_consecutive_losses: int = 0
    recovery_factor: Decimal | None = None


class BacktestTradeResponse(BaseModel):
    id: UUID
    option_ticker: str
    strategy_type: str
    underlying_symbol: str
    entry_date: date
    exit_date: date
    expiration_date: date
    quantity: int
    dte_at_open: int
    holding_period_days: int = Field(description="Calendar days between entry and exit dates.")
    holding_period_trading_days: int | None = Field(default=None, description="Trading days (market-open days) held. None for wheel strategy.")
    entry_underlying_close: Decimal
    exit_underlying_close: Decimal
    entry_mid: Decimal = Field(description="Per-unit position value divided by 100 (contract multiplier). NOT the raw option mid-price. To reconstruct cost: value × 100 × quantity.")
    exit_mid: Decimal = Field(description="Per-unit position value at exit divided by 100. Same convention as entry_mid.")
    gross_pnl: Decimal
    net_pnl: Decimal
    total_commissions: Decimal
    entry_reason: str
    exit_reason: str
    detail_json: dict[str, Any] = Field(default_factory=dict, description="Trade leg details. May be large for multi-leg strategies.")

    model_config = ConfigDict(from_attributes=True)


class TradeJsonResponse(BaseModel):
    """Trade response model for JSON-serialized trades (scan/sweep recommendations).

    Unlike ``BacktestTradeResponse``, this model does NOT require ``id`` because
    trades stored as JSON blobs inside recommendation rows have no individual
    database primary key.
    """
    option_ticker: str
    strategy_type: str
    underlying_symbol: str
    entry_date: date
    exit_date: date
    expiration_date: date
    quantity: int
    dte_at_open: int
    holding_period_days: int = Field(description="Calendar days between entry and exit dates.")
    holding_period_trading_days: int | None = Field(default=None, description="Trading days (market-open days) held.")
    entry_underlying_close: Decimal
    exit_underlying_close: Decimal
    entry_mid: Decimal = Field(description="Per-unit position value divided by 100 (contract multiplier). NOT the raw option mid-price.")
    exit_mid: Decimal = Field(description="Per-unit position value at exit divided by 100. Same convention as entry_mid.")
    gross_pnl: Decimal
    net_pnl: Decimal
    total_commissions: Decimal
    entry_reason: str
    exit_reason: str
    detail_json: dict[str, Any] = Field(default_factory=dict, description="Trade leg details. May be large for multi-leg strategies.")

    model_config = ConfigDict(extra="ignore")


class EquityCurvePointResponse(BaseModel):
    trade_date: date
    equity: Decimal
    cash: Decimal
    position_value: Decimal
    drawdown_pct: Decimal

    model_config = ConfigDict(from_attributes=True)


class BacktestRunHistoryItemResponse(BaseModel):
    """History item for backtest runs. The ``summary`` field does not exist on the
    ORM model; this schema requires manual construction in the service layer
    (see BacktestService._to_history_item). Do not use from_attributes."""
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    id: UUID
    symbol: str
    strategy_type: str
    status: RunStatus
    start_date: date = Field(alias="date_from")
    end_date: date = Field(alias="date_to")
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
    start_date: date = Field(alias="date_from")
    end_date: date = Field(alias="date_to")
    target_dte: int
    dte_tolerance_days: int
    max_holding_days: int
    account_size: Decimal
    risk_per_trade_pct: Decimal
    commission_per_contract: Decimal
    engine_version: Literal["options-multileg-v1", "options-multileg-v2"] = "options-multileg-v2"
    data_source: Literal["massive", "manual"] = "massive"
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    warnings: list[dict[str, Any]] = Field(validation_alias="warnings_json", max_length=100)
    error_code: str | None = None
    error_message: str | None = None
    summary: BacktestSummaryResponse
    trades: list[BacktestTradeResponse] = Field(max_length=10000)
    equity_curve: list[EquityCurvePointResponse] = Field(max_length=10000)
    equity_curve_truncated: bool = False
    risk_free_rate: Decimal | None = Field(
        default=None,
        description=(
            "Annualized risk-free rate used for Sharpe and Sortino ratio calculations. "
            "Sourced from the run's persisted column or input snapshot. "
            "Null only when the value could not be determined (very old runs)."
        ),
    )

    model_config = ConfigDict(populate_by_name=True)

    _sanitize = field_validator("error_message", mode="before")(sanitize_error_message)


class BacktestRunStatusResponse(BaseModel):
    id: UUID
    status: RunStatus
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error_code: str | None = None
    error_message: str | None = None

    _sanitize = field_validator("error_message", mode="before")(sanitize_error_message)


class BacktestRunListResponse(BaseModel):
    items: list[BacktestRunHistoryItemResponse]
    total: int = 0
    offset: int = 0
    limit: int = 50
    next_cursor: str | None = Field(
        default=None,
        description="Opaque cursor for keyset pagination. Pass as `cursor` on the next request to fetch the next page.",
    )


class CompareBacktestsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    run_ids: list[UUID] = Field(min_length=2, max_length=8)

    @field_validator("run_ids")
    @classmethod
    def validate_unique_ids(cls, value: list[UUID]) -> list[UUID]:
        if len(set(value)) != len(value):
            raise ValueError("run_ids must contain unique values")
        return value


class CompareBacktestsResponse(BaseModel):
    items: list[BacktestRunDetailResponse]
    comparison_limit: int
    trade_limit_per_run: int = 2_000
    trades_truncated: bool = False
