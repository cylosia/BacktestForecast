from __future__ import annotations

import bisect
import math
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta

import structlog

from backtestforecast.backtests.types import BacktestConfig, OptionDataGateway
from backtestforecast.indicators.calculations import (
    bollinger_bands,
    ema,
    macd,
    rolling_max,
    rolling_min,
    rsi,
    sma,
)
from backtestforecast.market_data.types import DailyBar, OptionContractRecord
from backtestforecast.schemas.backtests import (
    AvoidEarningsRule,
    BollingerBand,
    BollingerBandsRule,
    ComparisonOperator,
    IvPercentileRule,
    IvRankRule,
    MacdRule,
    MovingAverageCrossoverRule,
    RsiRule,
    SupportResistanceMode,
    SupportResistanceRule,
    VolumeSpikeRule,
)

logger = structlog.get_logger(__name__)

# Calendar days for time-to-expiry in BSM pricing (365).
# Trading days (252) are used separately for annualising Sharpe/Sortino.
CALENDAR_DAYS_PER_YEAR = 365.0


@dataclass(slots=True)
class EntryRuleEvaluator:
    config: BacktestConfig
    bars: list[DailyBar]
    earnings_dates: set[date]
    option_gateway: OptionDataGateway
    closes: list[float] = field(init=False)
    volumes: list[float] = field(init=False)
    rsi_cache: dict[int, list[float | None]] = field(default_factory=dict)
    sma_cache: dict[int, list[float | None]] = field(default_factory=dict)
    ema_cache: dict[int, list[float | None]] = field(default_factory=dict)
    macd_cache: dict[tuple[int, int, int], tuple[list[float | None], list[float | None], list[float | None]]] = field(
        default_factory=dict
    )
    bollinger_cache: dict[
        int | tuple[int, float], tuple[list[float | None], list[float | None], list[float | None]]
    ] = field(default_factory=dict)
    rolling_support_cache: dict[int, list[float | None]] = field(default_factory=dict)
    rolling_resistance_cache: dict[int, list[float | None]] = field(default_factory=dict)
    iv_series_cache: list[float | None] | None = None
    _sorted_earnings: list[date] = field(init=False)

    def __post_init__(self) -> None:
        self.closes = [bar.close_price for bar in self.bars]
        self.volumes = [bar.volume for bar in self.bars]
        self._sorted_earnings = sorted(self.earnings_dates)

    def is_entry_allowed(self, index: int) -> bool:
        if index <= 0 and self._has_crossover_rule(self.config.entry_rules):
            return False

        if index < 0:
            return False

        for rule in self.config.entry_rules:
            if isinstance(rule, RsiRule):
                if not self._evaluate_rsi_rule(rule, index):
                    return False
            elif isinstance(rule, MovingAverageCrossoverRule):
                if not self._evaluate_moving_average_rule(rule, index):
                    return False
            elif isinstance(rule, MacdRule):
                if not self._evaluate_macd_rule(rule, index):
                    return False
            elif isinstance(rule, BollingerBandsRule):
                if not self._evaluate_bollinger_rule(rule, index):
                    return False
            elif isinstance(rule, (IvRankRule, IvPercentileRule)):
                if not self._evaluate_iv_rule(rule, index):
                    return False
            elif isinstance(rule, VolumeSpikeRule):
                if not self._evaluate_volume_rule(rule, index):
                    return False
            elif isinstance(rule, SupportResistanceRule):
                if not self._evaluate_support_resistance_rule(rule, index):
                    return False
            elif isinstance(rule, AvoidEarningsRule):
                if not self._evaluate_avoid_earnings_rule(rule, index):
                    return False
            else:
                logger.warning(
                    "unknown_entry_rule_type",
                    rule_type=type(rule).__name__,
                    bar_index=index,
                )
                return False
        return True

    def _evaluate_rsi_rule(self, rule: RsiRule, index: int) -> bool:
        series = self.rsi_cache.setdefault(rule.period, rsi(self.closes, rule.period))
        current_value = series[index]
        if current_value is None:
            return False
        return compare(current_value, float(rule.threshold), rule.operator)

    def _evaluate_moving_average_rule(self, rule: MovingAverageCrossoverRule, index: int) -> bool:
        if rule.type == "sma_crossover":
            fast_series = self.sma_cache.setdefault(rule.fast_period, sma(self.closes, rule.fast_period))
            slow_series = self.sma_cache.setdefault(rule.slow_period, sma(self.closes, rule.slow_period))
        else:
            fast_series = self.ema_cache.setdefault(rule.fast_period, ema(self.closes, rule.fast_period))
            slow_series = self.ema_cache.setdefault(rule.slow_period, ema(self.closes, rule.slow_period))

        previous_fast = fast_series[index - 1]
        previous_slow = slow_series[index - 1]
        current_fast = fast_series[index]
        current_slow = slow_series[index]

        if previous_fast is None or previous_slow is None or current_fast is None or current_slow is None:
            return False

        if rule.direction == "bullish":
            return previous_fast <= previous_slow and current_fast > current_slow
        return previous_fast >= previous_slow and current_fast < current_slow

    def _evaluate_macd_rule(self, rule: MacdRule, index: int) -> bool:
        series = self.macd_cache.setdefault(
            (rule.fast_period, rule.slow_period, rule.signal_period),
            macd(self.closes, rule.fast_period, rule.slow_period, rule.signal_period),
        )
        macd_line, signal_line, _histogram = series
        prev_macd = macd_line[index - 1]
        prev_signal = signal_line[index - 1]
        curr_macd = macd_line[index]
        curr_signal = signal_line[index]
        if prev_macd is None or prev_signal is None or curr_macd is None or curr_signal is None:
            return False
        if rule.direction == "bullish":
            return prev_macd <= prev_signal and curr_macd > curr_signal
        return prev_macd >= prev_signal and curr_macd < curr_signal

    def _evaluate_bollinger_rule(self, rule: BollingerBandsRule, index: int) -> bool:
        cache_key = (rule.period, float(rule.standard_deviations))
        lower, middle, upper = self.bollinger_cache.setdefault(
            cache_key,
            bollinger_bands(self.closes, rule.period, float(rule.standard_deviations)),
        )
        target_series = {
            BollingerBand.LOWER: lower,
            BollingerBand.MIDDLE: middle,
            BollingerBand.UPPER: upper,
        }[rule.band]
        target_value = target_series[index]
        if target_value is None:
            return False
        return compare(self.closes[index], target_value, rule.operator)

    def _evaluate_iv_rule(self, rule: IvRankRule | IvPercentileRule, index: int) -> bool:
        iv_series = self._get_iv_series()
        current_value = iv_series[index]
        if current_value is None:
            return False

        lookback_values = [
            value for value in iv_series[max(0, index - rule.lookback_days + 1) : index + 1] if value is not None
        ]
        if len(lookback_values) < min(20, rule.lookback_days):
            return False

        if isinstance(rule, IvRankRule):
            window_min = min(lookback_values)
            window_max = max(lookback_values)
            if math.isclose(window_min, window_max):
                return False
            else:
                metric = ((current_value - window_min) / (window_max - window_min)) * 100.0
        else:
            below_count = sum(1 for value in lookback_values if value < current_value)
            metric = (below_count / len(lookback_values)) * 100.0

        return compare(metric, float(rule.threshold), rule.operator)

    def _evaluate_volume_rule(self, rule: VolumeSpikeRule, index: int) -> bool:
        if rule.lookback_period < 1 or index < rule.lookback_period:
            return False
        baseline = sum(self.volumes[index - rule.lookback_period : index]) / rule.lookback_period
        if baseline <= 0:
            return False
        ratio = self.volumes[index] / baseline
        return compare(ratio, float(rule.multiplier), rule.operator)

    def _evaluate_support_resistance_rule(self, rule: SupportResistanceRule, index: int) -> bool:
        if index < rule.lookback_period:
            return False
        support_series = self.rolling_support_cache.setdefault(
            rule.lookback_period, rolling_min(self.closes, rule.lookback_period)
        )
        resistance_series = self.rolling_resistance_cache.setdefault(
            rule.lookback_period, rolling_max(self.closes, rule.lookback_period)
        )
        prior_support = support_series[index - 1]
        prior_resistance = resistance_series[index - 1]
        current_close = self.closes[index]
        previous_close = self.closes[index - 1]
        tolerance_ratio = float(rule.tolerance_pct) / 100.0

        if rule.mode == SupportResistanceMode.NEAR_SUPPORT:
            if prior_support is None or prior_support == 0:
                return False
            return abs(current_close - prior_support) / prior_support <= tolerance_ratio
        if rule.mode == SupportResistanceMode.NEAR_RESISTANCE:
            if prior_resistance is None or prior_resistance == 0:
                return False
            return abs(current_close - prior_resistance) / prior_resistance <= tolerance_ratio
        if rule.mode == SupportResistanceMode.BREAKOUT_ABOVE_RESISTANCE:
            if prior_resistance is None:
                return False
            return previous_close <= prior_resistance and current_close > (prior_resistance * (1.0 + tolerance_ratio))
        if prior_support is None:
            return False
        return previous_close >= prior_support and current_close < (prior_support * (1.0 - tolerance_ratio))

    def _evaluate_avoid_earnings_rule(self, rule: AvoidEarningsRule, index: int) -> bool:
        from backtestforecast.utils.dates import trading_to_calendar_days

        bar_date = self.bars[index].trade_date
        cal_after = trading_to_calendar_days(rule.days_after, reference_date=bar_date)
        cal_before = trading_to_calendar_days(rule.days_before, reference_date=bar_date)
        blackout_start = bar_date - timedelta(days=cal_after)
        blackout_end = bar_date + timedelta(days=cal_before)
        lo = bisect.bisect_left(self._sorted_earnings, blackout_start)
        return lo >= len(self._sorted_earnings) or self._sorted_earnings[lo] > blackout_end

    @staticmethod
    def _has_crossover_rule(rules: Sequence) -> bool:
        """Check if any rule requires the previous bar (index-1) for crossover detection."""
        return any(isinstance(rule, (MovingAverageCrossoverRule, MacdRule)) for rule in rules)

    def _get_iv_series(self) -> list[float | None]:
        if self.iv_series_cache is None:
            self.iv_series_cache = build_estimated_iv_series(
                bars=self.bars,
                option_gateway=self.option_gateway,
                target_dte=self.config.target_dte,
                dte_tolerance_days=self.config.dte_tolerance_days,
                risk_free_rate=self.config.risk_free_rate,
                risk_free_rate_resolver=self.config.resolve_risk_free_rate,
                dividend_yield=self.config.dividend_yield,
            )
        return self.iv_series_cache


def compare(left: float, right: float, operator: ComparisonOperator) -> bool:
    if operator == ComparisonOperator.LT:
        return left < right
    if operator == ComparisonOperator.LTE:
        return left <= right
    if operator == ComparisonOperator.GT:
        return left > right
    return left >= right


def build_estimated_iv_series(
    bars: list[DailyBar],
    option_gateway: OptionDataGateway,
    target_dte: int,
    dte_tolerance_days: int,
    risk_free_rate: float = 0.045,
    risk_free_rate_resolver: Callable[[date], float] | None = None,
    dividend_yield: float = 0.0,
    sample_interval: int = 1,
) -> list[float | None]:
    _SENTINEL = object()
    results: list[float | None] = []
    last_index = len(bars) - 1
    iv_cache: dict[date, float | None | object] = {}
    for index, bar in enumerate(bars):
        if index % sample_interval == 0 or index == last_index:
            cached = iv_cache.get(bar.trade_date, _SENTINEL)
            if cached is not _SENTINEL:
                iv_value = cached  # type: ignore[assignment]
            else:
                iv_value = estimate_atm_iv_for_date(
                    trade_date=bar.trade_date,
                    underlying_close=bar.close_price,
                    option_gateway=option_gateway,
                    target_dte=target_dte,
                    dte_tolerance_days=dte_tolerance_days,
                    risk_free_rate=risk_free_rate,
                    risk_free_rate_resolver=risk_free_rate_resolver,
                    dividend_yield=dividend_yield,
                )
                iv_cache[bar.trade_date] = iv_value
            results.append(iv_value)
        else:
            results.append(None)
    return results


def estimate_atm_iv_for_date(
    trade_date,
    underlying_close: float,
    option_gateway: OptionDataGateway,
    target_dte: int,
    dte_tolerance_days: int,
    risk_free_rate: float = 0.045,
    risk_free_rate_resolver: Callable[[date], float] | None = None,
    dividend_yield: float = 0.0,
) -> float | None:
    calls = option_gateway.list_contracts(trade_date, "call", target_dte, dte_tolerance_days)
    puts = option_gateway.list_contracts(trade_date, "put", target_dte, dte_tolerance_days)
    if not calls or not puts:
        return None

    calls_by_exp = _group_by_expiration(calls)
    puts_by_exp = _group_by_expiration(puts)
    common_expirations = sorted(set(calls_by_exp) & set(puts_by_exp))
    if not common_expirations:
        return None

    chosen_expiration = min(
        common_expirations,
        key=lambda expiration: (
            abs((expiration - trade_date).days - target_dte),
            0 if (expiration - trade_date).days >= target_dte else 1,
            (expiration - trade_date).days,
        ),
    )
    call_contracts = calls_by_exp[chosen_expiration]
    put_contracts = puts_by_exp[chosen_expiration]
    common_strikes = sorted(
        {contract.strike_price for contract in call_contracts} & {contract.strike_price for contract in put_contracts}
    )
    if not common_strikes:
        return None
    chosen_strike = min(common_strikes, key=lambda strike: abs(strike - underlying_close))
    call_contract = next((c for c in call_contracts if c.strike_price == chosen_strike), None)
    put_contract = next((c for c in put_contracts if c.strike_price == chosen_strike), None)
    if call_contract is None or put_contract is None:
        return None

    dte = max((chosen_expiration - trade_date).days, 1)
    current_risk_free_rate = risk_free_rate_resolver(trade_date) if risk_free_rate_resolver is not None else risk_free_rate
    estimates: list[float] = []
    for contract in (call_contract, put_contract):
        quote = option_gateway.get_quote(contract.ticker, trade_date)
        if quote is None:
            continue
        option_price = quote.mid_price
        if option_price <= 0:
            continue
        option_type = contract.contract_type
        iv = implied_volatility_from_price(
            option_price=option_price,
            underlying_price=underlying_close,
            strike_price=contract.strike_price,
            time_to_expiry_years=dte / CALENDAR_DAYS_PER_YEAR,
            option_type=option_type,
            risk_free_rate=current_risk_free_rate,
            dividend_yield=dividend_yield,
        )
        if iv is not None:
            estimates.append(iv)

    if not estimates:
        return None
    return sum(estimates) / len(estimates)


def implied_volatility_from_price(
    option_price: float,
    underlying_price: float,
    strike_price: float,
    time_to_expiry_years: float,
    option_type: str,
    risk_free_rate: float = 0.045,
    dividend_yield: float = 0.0,
) -> float | None:
    if option_price <= 0 or underlying_price <= 0 or strike_price <= 0 or time_to_expiry_years <= 0:
        return None

    low = 0.001
    high = 10.0
    _CONVERGENCE_TOL = 1e-4
    for _ in range(60):
        midpoint = (low + high) / 2.0
        theoretical = black_scholes_price(
            option_type=option_type,
            underlying_price=underlying_price,
            strike_price=strike_price,
            time_to_expiry_years=time_to_expiry_years,
            volatility=midpoint,
            risk_free_rate=risk_free_rate,
            dividend_yield=dividend_yield,
        )
        if abs(theoretical - option_price) < _CONVERGENCE_TOL:
            return midpoint
        if theoretical > option_price:
            high = midpoint
        else:
            low = midpoint
    final = (low + high) / 2.0
    residual_threshold = max(_CONVERGENCE_TOL * 100, option_price * 0.01)
    final_theoretical = black_scholes_price(
        option_type=option_type,
        underlying_price=underlying_price,
        strike_price=strike_price,
        time_to_expiry_years=time_to_expiry_years,
        volatility=final,
        risk_free_rate=risk_free_rate,
        dividend_yield=dividend_yield,
    )
    if abs(final_theoretical - option_price) > residual_threshold:
        return None
    return final


def black_scholes_price(
    option_type: str,
    underlying_price: float,
    strike_price: float,
    time_to_expiry_years: float,
    volatility: float,
    risk_free_rate: float = 0.045,
    dividend_yield: float = 0.0,
) -> float:
    if underlying_price <= 0 or strike_price <= 0:
        return 0.0
    if time_to_expiry_years <= 0 or volatility <= 0:
        intrinsic = (
            max(0.0, underlying_price - strike_price)
            if option_type == "call"
            else max(0.0, strike_price - underlying_price)
        )
        return intrinsic

    sigma_sqrt_t = volatility * math.sqrt(time_to_expiry_years)
    d1 = (
        math.log(underlying_price / strike_price)
        + (risk_free_rate - dividend_yield + 0.5 * volatility * volatility) * time_to_expiry_years
    ) / sigma_sqrt_t
    d2 = d1 - sigma_sqrt_t
    discount = math.exp(-risk_free_rate * time_to_expiry_years)
    dividend_discount = math.exp(-dividend_yield * time_to_expiry_years)

    if option_type == "call":
        return (underlying_price * dividend_discount * normal_cdf(d1)) - (strike_price * discount * normal_cdf(d2))
    return (strike_price * discount * normal_cdf(-d2)) - (underlying_price * dividend_discount * normal_cdf(-d1))


def normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _group_by_expiration(contracts: list[OptionContractRecord]) -> dict[date, list[OptionContractRecord]]:
    grouped: dict[date, list[OptionContractRecord]] = defaultdict(list)
    for contract in contracts:
        grouped[contract.expiration_date].append(contract)
    return grouped
