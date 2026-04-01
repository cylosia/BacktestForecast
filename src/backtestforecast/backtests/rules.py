from __future__ import annotations

import bisect
import json
import math
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, TypeVar

import structlog

from backtestforecast.backtests.strategies.common import select_preferred_common_expiration_contracts
from backtestforecast.backtests.types import BacktestConfig, OptionDataGateway
from backtestforecast.indicators.calculations import (
    adx,
    bollinger_bands,
    cci,
    ema,
    macd,
    mfi,
    roc,
    rolling_max,
    rolling_min,
    rsi,
    sma,
    stochastic_oscillator,
    williams_r,
)
from backtestforecast.market_data.types import DailyBar, OptionContractRecord
from backtestforecast.schemas.backtests import (
    AdxSeries,
    AvoidEarningsRule,
    BollingerBand,
    BollingerBandSeries,
    BollingerBandsRule,
    CciSeries,
    CloseSeries,
    ComparisonOperator,
    EmaSeries,
    IndicatorLevelCrossRule,
    IndicatorPersistenceRule,
    IndicatorSeries,
    IndicatorSeriesCrossRule,
    IndicatorThresholdRule,
    IndicatorTrendDirection,
    IndicatorTrendRule,
    IvPercentileRule,
    IvPercentileSeries,
    IvRankRule,
    IvRankSeries,
    MacdHistogramSeries,
    MacdLineSeries,
    MacdRule,
    MacdSignalSeries,
    MfiSeries,
    MovingAverageCrossoverRule,
    RocSeries,
    RsiRule,
    RsiSeriesSpec,
    SmaSeries,
    StochasticDSeries,
    StochasticKSeries,
    SupportResistanceMode,
    SupportResistanceRule,
    VolumeRatioSeries,
    VolumeSpikeRule,
    WilliamsRSeries,
)

logger = structlog.get_logger(__name__)
_CacheValueT = TypeVar("_CacheValueT")

# Calendar days for time-to-expiry in BSM pricing (365).
# Trading days (252) are used separately for annualising Sharpe/Sortino.
CALENDAR_DAYS_PER_YEAR = 365.0


@dataclass(slots=True)
class EntryRuleComputationCache:
    rsi_cache: dict[int, list[float | None]] = field(default_factory=dict)
    sma_cache: dict[int, list[float | None]] = field(default_factory=dict)
    ema_cache: dict[int, list[float | None]] = field(default_factory=dict)
    macd_cache: dict[tuple[int, int, int], tuple[list[float | None], list[float | None], list[float | None]]] = (
        field(default_factory=dict)
    )
    bollinger_cache: dict[
        int | tuple[int, float], tuple[list[float | None], list[float | None], list[float | None]]
    ] = field(default_factory=dict)
    rolling_support_cache: dict[int, list[float | None]] = field(default_factory=dict)
    rolling_resistance_cache: dict[int, list[float | None]] = field(default_factory=dict)
    iv_series_cache: dict[str, list[float | None]] = field(default_factory=dict)
    iv_rank_series_cache: dict[tuple[str, int], list[float | None]] = field(default_factory=dict)
    iv_percentile_series_cache: dict[tuple[str, int], list[float | None]] = field(default_factory=dict)
    volume_ratio_cache: dict[int, list[float | None]] = field(default_factory=dict)
    cci_cache: dict[int, list[float | None]] = field(default_factory=dict)
    roc_cache: dict[int, list[float | None]] = field(default_factory=dict)
    mfi_cache: dict[int, list[float | None]] = field(default_factory=dict)
    stochastic_k_cache: dict[tuple[int, int, int], list[float | None]] = field(default_factory=dict)
    stochastic_d_cache: dict[tuple[int, int, int], list[float | None]] = field(default_factory=dict)
    adx_cache: dict[int, list[float | None]] = field(default_factory=dict)
    williams_r_cache: dict[int, list[float | None]] = field(default_factory=dict)
    generic_series_cache: dict[str, list[float | None]] = field(default_factory=dict)
    entry_allowed_masks: dict[str, list[bool]] = field(default_factory=dict)


@dataclass(slots=True)
class EntryRuleEvaluator:
    config: BacktestConfig
    bars: list[DailyBar]
    earnings_dates: set[date]
    option_gateway: OptionDataGateway
    shared_cache: EntryRuleComputationCache | None = None
    closes: list[float] = field(init=False)
    highs: list[float] = field(init=False)
    lows: list[float] = field(init=False)
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
    iv_series_cache: dict[str, list[float | None]] = field(default_factory=dict)
    iv_rank_series_cache: dict[tuple[str, int], list[float | None]] = field(default_factory=dict)
    iv_percentile_series_cache: dict[tuple[str, int], list[float | None]] = field(default_factory=dict)
    volume_ratio_cache: dict[int, list[float | None]] = field(default_factory=dict)
    cci_cache: dict[int, list[float | None]] = field(default_factory=dict)
    roc_cache: dict[int, list[float | None]] = field(default_factory=dict)
    mfi_cache: dict[int, list[float | None]] = field(default_factory=dict)
    stochastic_k_cache: dict[tuple[int, int, int], list[float | None]] = field(default_factory=dict)
    stochastic_d_cache: dict[tuple[int, int, int], list[float | None]] = field(default_factory=dict)
    adx_cache: dict[int, list[float | None]] = field(default_factory=dict)
    williams_r_cache: dict[int, list[float | None]] = field(default_factory=dict)
    generic_series_cache: dict[str, list[float | None]] = field(default_factory=dict)
    _sorted_earnings: list[date] = field(init=False)
    _entry_allowed_mask: list[bool] | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self.closes = [bar.close_price for bar in self.bars]
        self.highs = [bar.high_price for bar in self.bars]
        self.lows = [bar.low_price for bar in self.bars]
        self.volumes = [bar.volume for bar in self.bars]
        self._sorted_earnings = sorted(self.earnings_dates)
        if self.shared_cache is not None:
            self.rsi_cache = self.shared_cache.rsi_cache
            self.sma_cache = self.shared_cache.sma_cache
            self.ema_cache = self.shared_cache.ema_cache
            self.macd_cache = self.shared_cache.macd_cache
            self.bollinger_cache = self.shared_cache.bollinger_cache
            self.rolling_support_cache = self.shared_cache.rolling_support_cache
            self.rolling_resistance_cache = self.shared_cache.rolling_resistance_cache
            self.iv_series_cache = self.shared_cache.iv_series_cache
            self.iv_rank_series_cache = self.shared_cache.iv_rank_series_cache
            self.iv_percentile_series_cache = self.shared_cache.iv_percentile_series_cache
            self.volume_ratio_cache = self.shared_cache.volume_ratio_cache
            self.cci_cache = self.shared_cache.cci_cache
            self.roc_cache = self.shared_cache.roc_cache
            self.mfi_cache = self.shared_cache.mfi_cache
            self.stochastic_k_cache = self.shared_cache.stochastic_k_cache
            self.stochastic_d_cache = self.shared_cache.stochastic_d_cache
            self.adx_cache = self.shared_cache.adx_cache
            self.williams_r_cache = self.shared_cache.williams_r_cache
            self.generic_series_cache = self.shared_cache.generic_series_cache

    def is_entry_allowed(self, index: int) -> bool:
        if self._entry_allowed_mask is None and self.shared_cache is not None:
            self._entry_allowed_mask = self.shared_cache.entry_allowed_masks.get(self._entry_mask_cache_key())
        if self._entry_allowed_mask is not None:
            if index < 0 or index >= len(self._entry_allowed_mask):
                return False
            return self._entry_allowed_mask[index]

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
            elif isinstance(rule, IndicatorThresholdRule):
                if not self._evaluate_indicator_threshold_rule(rule, index):
                    return False
            elif isinstance(rule, IndicatorTrendRule):
                if not self._evaluate_indicator_trend_rule(rule, index):
                    return False
            elif isinstance(rule, IndicatorLevelCrossRule):
                if not self._evaluate_indicator_level_cross_rule(rule, index):
                    return False
            elif isinstance(rule, IndicatorSeriesCrossRule):
                if not self._evaluate_indicator_series_cross_rule(rule, index):
                    return False
            elif isinstance(rule, IndicatorPersistenceRule):
                if not self._evaluate_indicator_persistence_rule(rule, index):
                    return False
            else:
                logger.warning(
                    "unknown_entry_rule_type",
                    rule_type=type(rule).__name__,
                    bar_index=index,
                )
                return False
        return True

    def build_entry_allowed_mask(self) -> list[bool]:
        cached = self._entry_allowed_mask
        if cached is not None:
            return cached

        cache_key = self._entry_mask_cache_key()
        if self.shared_cache is not None:
            shared_cached = self.shared_cache.entry_allowed_masks.get(cache_key)
            if shared_cached is not None:
                self._entry_allowed_mask = shared_cached
                return shared_cached

        if not self.bars:
            self._entry_allowed_mask = []
            return self._entry_allowed_mask

        combined_mask = [True] * len(self.bars)
        for rule in self.config.entry_rules:
            rule_mask = self._build_rule_mask(rule)
            combined_mask = [
                left and right for left, right in zip(combined_mask, rule_mask, strict=False)
            ]

        self._entry_allowed_mask = combined_mask
        if self.shared_cache is not None:
            self.shared_cache.entry_allowed_masks[cache_key] = combined_mask
        return combined_mask

    def _build_rule_mask(self, rule: object) -> list[bool]:
        if isinstance(rule, RsiRule):
            return self._build_rsi_mask(rule)
        if isinstance(rule, MovingAverageCrossoverRule):
            return self._build_moving_average_mask(rule)
        if isinstance(rule, MacdRule):
            return self._build_macd_mask(rule)
        if isinstance(rule, BollingerBandsRule):
            return self._build_bollinger_mask(rule)
        if isinstance(rule, (IvRankRule, IvPercentileRule)):
            return self._build_iv_mask(rule)
        if isinstance(rule, VolumeSpikeRule):
            return self._build_volume_mask(rule)
        if isinstance(rule, SupportResistanceRule):
            return self._build_support_resistance_mask(rule)
        if isinstance(rule, AvoidEarningsRule):
            return self._build_avoid_earnings_mask(rule)
        if isinstance(rule, IndicatorThresholdRule):
            return self._build_indicator_threshold_mask(rule)
        if isinstance(rule, IndicatorTrendRule):
            return self._build_indicator_trend_mask(rule)
        if isinstance(rule, IndicatorLevelCrossRule):
            return self._build_indicator_level_cross_mask(rule)
        if isinstance(rule, IndicatorSeriesCrossRule):
            return self._build_indicator_series_cross_mask(rule)
        if isinstance(rule, IndicatorPersistenceRule):
            return self._build_indicator_persistence_mask(rule)
        logger.warning("unknown_entry_rule_type", rule_type=type(rule).__name__)
        return [False] * len(self.bars)

    def _build_rsi_mask(self, rule: RsiRule) -> list[bool]:
        return self._mask_from_series_level(
            self._get_cached(self.rsi_cache, rule.period, lambda: rsi(self.closes, rule.period)),
            float(rule.threshold),
            rule.operator,
        )

    def _build_moving_average_mask(self, rule: MovingAverageCrossoverRule) -> list[bool]:
        if rule.type == "sma_crossover":
            fast_series = self._get_cached(self.sma_cache, rule.fast_period, lambda: sma(self.closes, rule.fast_period))
            slow_series = self._get_cached(self.sma_cache, rule.slow_period, lambda: sma(self.closes, rule.slow_period))
        else:
            fast_series = self._get_cached(self.ema_cache, rule.fast_period, lambda: ema(self.closes, rule.fast_period))
            slow_series = self._get_cached(self.ema_cache, rule.slow_period, lambda: ema(self.closes, rule.slow_period))

        mask = [False] * len(self.bars)
        for index in range(1, len(self.bars)):
            previous_fast = fast_series[index - 1]
            previous_slow = slow_series[index - 1]
            current_fast = fast_series[index]
            current_slow = slow_series[index]
            if previous_fast is None or previous_slow is None or current_fast is None or current_slow is None:
                continue
            if rule.direction == "bullish":
                mask[index] = previous_fast <= previous_slow and current_fast > current_slow
            else:
                mask[index] = previous_fast >= previous_slow and current_fast < current_slow
        return mask

    def _build_macd_mask(self, rule: MacdRule) -> list[bool]:
        macd_line, signal_line, _histogram = self._get_cached(
            self.macd_cache,
            (rule.fast_period, rule.slow_period, rule.signal_period),
            lambda: macd(self.closes, rule.fast_period, rule.slow_period, rule.signal_period),
        )
        mask = [False] * len(self.bars)
        for index in range(1, len(self.bars)):
            prev_macd = macd_line[index - 1]
            prev_signal = signal_line[index - 1]
            curr_macd = macd_line[index]
            curr_signal = signal_line[index]
            if prev_macd is None or prev_signal is None or curr_macd is None or curr_signal is None:
                continue
            if rule.direction == "bullish":
                mask[index] = prev_macd <= prev_signal and curr_macd > curr_signal
            else:
                mask[index] = prev_macd >= prev_signal and curr_macd < curr_signal
        return mask

    def _build_bollinger_mask(self, rule: BollingerBandsRule) -> list[bool]:
        cache_key = (rule.period, float(rule.standard_deviations))
        lower, middle, upper = self._get_cached(
            self.bollinger_cache,
            cache_key,
            lambda: bollinger_bands(self.closes, rule.period, float(rule.standard_deviations)),
        )
        target_series = {
            BollingerBand.LOWER: lower,
            BollingerBand.MIDDLE: middle,
            BollingerBand.UPPER: upper,
        }[rule.band]
        return [
            target_value is not None and compare(close, target_value, rule.operator)
            for close, target_value in zip(self.closes, target_series, strict=False)
        ]

    def _build_iv_mask(self, rule: IvRankRule | IvPercentileRule) -> list[bool]:
        iv_cache_key = self._iv_cache_key()
        if isinstance(rule, IvRankRule):
            metric_series = self._get_cached(
                self.iv_rank_series_cache,
                (iv_cache_key, rule.lookback_days),
                lambda: self._build_iv_metric_series(rule.lookback_days, percentile=False),
            )
        else:
            metric_series = self._get_cached(
                self.iv_percentile_series_cache,
                (iv_cache_key, rule.lookback_days),
                lambda: self._build_iv_metric_series(rule.lookback_days, percentile=True),
            )
        return self._mask_from_series_level(metric_series, float(rule.threshold), rule.operator)

    def _build_volume_mask(self, rule: VolumeSpikeRule) -> list[bool]:
        ratio_series = self._get_cached(
            self.volume_ratio_cache,
            rule.lookback_period,
            lambda: self._build_volume_ratio_series(rule.lookback_period),
        )
        return self._mask_from_series_level(ratio_series, float(rule.multiplier), rule.operator)

    def _build_support_resistance_mask(self, rule: SupportResistanceRule) -> list[bool]:
        mask = [False] * len(self.bars)
        support_series = self._get_cached(
            self.rolling_support_cache, rule.lookback_period, lambda: rolling_min(self.closes, rule.lookback_period)
        )
        resistance_series = self._get_cached(
            self.rolling_resistance_cache, rule.lookback_period, lambda: rolling_max(self.closes, rule.lookback_period)
        )
        tolerance_ratio = float(rule.tolerance_pct) / 100.0
        for index in range(rule.lookback_period, len(self.bars)):
            prior_support = support_series[index - 1]
            prior_resistance = resistance_series[index - 1]
            current_close = self.closes[index]
            previous_close = self.closes[index - 1]
            if rule.mode == SupportResistanceMode.NEAR_SUPPORT:
                if prior_support is None or prior_support == 0:
                    continue
                mask[index] = abs(current_close - prior_support) / prior_support <= tolerance_ratio
            elif rule.mode == SupportResistanceMode.NEAR_RESISTANCE:
                if prior_resistance is None or prior_resistance == 0:
                    continue
                mask[index] = abs(current_close - prior_resistance) / prior_resistance <= tolerance_ratio
            elif rule.mode == SupportResistanceMode.BREAKOUT_ABOVE_RESISTANCE:
                if prior_resistance is None:
                    continue
                mask[index] = previous_close <= prior_resistance and current_close > (
                    prior_resistance * (1.0 + tolerance_ratio)
                )
            else:
                if prior_support is None:
                    continue
                mask[index] = previous_close >= prior_support and current_close < (
                    prior_support * (1.0 - tolerance_ratio)
                )
        return mask

    def _build_avoid_earnings_mask(self, rule: AvoidEarningsRule) -> list[bool]:
        return [self._evaluate_avoid_earnings_rule(rule, index) for index in range(len(self.bars))]

    def _build_indicator_threshold_mask(self, rule: IndicatorThresholdRule) -> list[bool]:
        return self._mask_from_series_level(
            self._get_indicator_series(rule.series),
            float(rule.level),
            rule.operator,
        )

    def _build_indicator_trend_mask(self, rule: IndicatorTrendRule) -> list[bool]:
        mask = [False] * len(self.bars)
        start_index = rule.bars - 1
        if start_index < 0:
            return mask
        series = self._get_indicator_series(rule.series)
        for index in range(start_index, len(self.bars)):
            window = series[index - rule.bars + 1 : index + 1]
            if any(value is None for value in window):
                continue
            values = [float(value) for value in window if value is not None]
            if rule.direction == IndicatorTrendDirection.RISING:
                mask[index] = all(left < right for left, right in zip(values, values[1:], strict=False))
            else:
                mask[index] = all(left > right for left, right in zip(values, values[1:], strict=False))
        return mask

    def _build_indicator_level_cross_mask(self, rule: IndicatorLevelCrossRule) -> list[bool]:
        mask = [False] * len(self.bars)
        series = self._get_indicator_series(rule.series)
        level = float(rule.level)
        for index in range(1, len(self.bars)):
            previous = series[index - 1]
            current = series[index]
            if previous is None or current is None:
                continue
            if rule.direction == "crosses_above":
                mask[index] = previous <= level and current > level
            else:
                mask[index] = previous >= level and current < level
        return mask

    def _build_indicator_series_cross_mask(self, rule: IndicatorSeriesCrossRule) -> list[bool]:
        mask = [False] * len(self.bars)
        left_series = self._get_indicator_series(rule.left_series)
        right_series = self._get_indicator_series(rule.right_series)
        for index in range(1, len(self.bars)):
            previous_left = left_series[index - 1]
            current_left = left_series[index]
            previous_right = right_series[index - 1]
            current_right = right_series[index]
            if any(value is None for value in (previous_left, current_left, previous_right, current_right)):
                continue
            if rule.direction == "crosses_above":
                mask[index] = previous_left <= previous_right and current_left > current_right
            else:
                mask[index] = previous_left >= previous_right and current_left < current_right
        return mask

    def _build_indicator_persistence_mask(self, rule: IndicatorPersistenceRule) -> list[bool]:
        mask = [False] * len(self.bars)
        start_index = rule.bars - 1
        if start_index < 0:
            return mask
        series = self._get_indicator_series(rule.series)
        level = float(rule.level)
        for index in range(start_index, len(self.bars)):
            window = series[index - rule.bars + 1 : index + 1]
            if any(value is None for value in window):
                continue
            mask[index] = all(compare(float(value), level, rule.operator) for value in window if value is not None)
        return mask

    @staticmethod
    def _mask_from_series_level(
        series: Sequence[float | None],
        level: float,
        operator: ComparisonOperator,
    ) -> list[bool]:
        return [
            value is not None and compare(float(value), level, operator)
            for value in series
        ]

    @staticmethod
    def _get_cached(
        cache: dict[Any, _CacheValueT],
        key: Any,
        factory: Callable[[], _CacheValueT],
    ) -> _CacheValueT:
        cached = cache.get(key)
        if cached is not None:
            return cached
        value = factory()
        cache[key] = value
        return value

    def _evaluate_rsi_rule(self, rule: RsiRule, index: int) -> bool:
        series = self._get_cached(self.rsi_cache, rule.period, lambda: rsi(self.closes, rule.period))
        current_value = series[index]
        if current_value is None:
            return False
        return compare(current_value, float(rule.threshold), rule.operator)

    def _evaluate_moving_average_rule(self, rule: MovingAverageCrossoverRule, index: int) -> bool:
        if rule.type == "sma_crossover":
            fast_series = self._get_cached(self.sma_cache, rule.fast_period, lambda: sma(self.closes, rule.fast_period))
            slow_series = self._get_cached(self.sma_cache, rule.slow_period, lambda: sma(self.closes, rule.slow_period))
        else:
            fast_series = self._get_cached(self.ema_cache, rule.fast_period, lambda: ema(self.closes, rule.fast_period))
            slow_series = self._get_cached(self.ema_cache, rule.slow_period, lambda: ema(self.closes, rule.slow_period))

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
        series = self._get_cached(
            self.macd_cache,
            (rule.fast_period, rule.slow_period, rule.signal_period),
            lambda: macd(self.closes, rule.fast_period, rule.slow_period, rule.signal_period),
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
        lower, middle, upper = self._get_cached(
            self.bollinger_cache,
            cache_key,
            lambda: bollinger_bands(self.closes, rule.period, float(rule.standard_deviations)),
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
        iv_cache_key = self._iv_cache_key()
        if isinstance(rule, IvRankRule):
            metric_series = self._get_cached(
                self.iv_rank_series_cache,
                (iv_cache_key, rule.lookback_days),
                lambda: self._build_iv_metric_series(rule.lookback_days, percentile=False),
            )
        else:
            metric_series = self._get_cached(
                self.iv_percentile_series_cache,
                (iv_cache_key, rule.lookback_days),
                lambda: self._build_iv_metric_series(rule.lookback_days, percentile=True),
            )
        current_value = metric_series[index]
        if current_value is None:
            return False
        return compare(current_value, float(rule.threshold), rule.operator)

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
        support_series = self._get_cached(
            self.rolling_support_cache, rule.lookback_period, lambda: rolling_min(self.closes, rule.lookback_period)
        )
        resistance_series = self._get_cached(
            self.rolling_resistance_cache, rule.lookback_period, lambda: rolling_max(self.closes, rule.lookback_period)
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

    def _evaluate_indicator_threshold_rule(self, rule: IndicatorThresholdRule, index: int) -> bool:
        series = self._get_indicator_series(rule.series)
        current_value = series[index]
        if current_value is None:
            return False
        return compare(current_value, float(rule.level), rule.operator)

    def _evaluate_indicator_trend_rule(self, rule: IndicatorTrendRule, index: int) -> bool:
        if index < rule.bars - 1:
            return False
        series = self._get_indicator_series(rule.series)
        window = series[index - rule.bars + 1 : index + 1]
        if any(value is None for value in window):
            return False
        values = [float(value) for value in window if value is not None]
        if rule.direction == IndicatorTrendDirection.RISING:
            return all(left < right for left, right in zip(values, values[1:], strict=False))
        return all(left > right for left, right in zip(values, values[1:], strict=False))

    def _evaluate_indicator_level_cross_rule(self, rule: IndicatorLevelCrossRule, index: int) -> bool:
        if index <= 0:
            return False
        series = self._get_indicator_series(rule.series)
        previous = series[index - 1]
        current = series[index]
        if previous is None or current is None:
            return False
        level = float(rule.level)
        if rule.direction == "crosses_above":
            return previous <= level and current > level
        return previous >= level and current < level

    def _evaluate_indicator_series_cross_rule(self, rule: IndicatorSeriesCrossRule, index: int) -> bool:
        if index <= 0:
            return False
        left_series = self._get_indicator_series(rule.left_series)
        right_series = self._get_indicator_series(rule.right_series)
        previous_left = left_series[index - 1]
        current_left = left_series[index]
        previous_right = right_series[index - 1]
        current_right = right_series[index]
        if any(value is None for value in [previous_left, current_left, previous_right, current_right]):
            return False
        if rule.direction == "crosses_above":
            return previous_left <= previous_right and current_left > current_right
        return previous_left >= previous_right and current_left < current_right

    def _evaluate_indicator_persistence_rule(self, rule: IndicatorPersistenceRule, index: int) -> bool:
        if index < rule.bars - 1:
            return False
        series = self._get_indicator_series(rule.series)
        window = series[index - rule.bars + 1 : index + 1]
        if any(value is None for value in window):
            return False
        level = float(rule.level)
        return all(compare(float(value), level, rule.operator) for value in window if value is not None)

    def _build_iv_metric_series(self, lookback_days: int, *, percentile: bool) -> list[float | None]:
        iv_series = self._get_iv_series()
        result: list[float | None] = [None] * len(iv_series)
        minimum_samples = min(20, lookback_days)
        for index, current_value in enumerate(iv_series):
            if current_value is None:
                continue
            lookback_values = [
                value for value in iv_series[max(0, index - lookback_days + 1) : index + 1] if value is not None
            ]
            if len(lookback_values) < minimum_samples:
                continue
            if percentile:
                below_count = sum(1 for value in lookback_values if value < current_value)
                result[index] = (below_count / len(lookback_values)) * 100.0
                continue
            window_min = min(lookback_values)
            window_max = max(lookback_values)
            if math.isclose(window_min, window_max):
                continue
            result[index] = ((current_value - window_min) / (window_max - window_min)) * 100.0
        return result

    def _build_volume_ratio_series(self, lookback_period: int) -> list[float | None]:
        result: list[float | None] = [None] * len(self.volumes)
        for index in range(lookback_period, len(self.volumes)):
            baseline = sum(self.volumes[index - lookback_period : index]) / lookback_period
            if baseline <= 0:
                continue
            result[index] = self.volumes[index] / baseline
        return result

    @staticmethod
    def _series_cache_key(spec: IndicatorSeries) -> str:
        return json.dumps(spec.model_dump(mode="json"), sort_keys=True)

    def _get_indicator_series(self, spec: IndicatorSeries) -> list[float | None]:
        cache_key = self._series_cache_key(spec)
        cached = self.generic_series_cache.get(cache_key)
        if cached is not None:
            return cached
        series = self._build_indicator_series(spec)
        self.generic_series_cache[cache_key] = series
        return series

    def _build_indicator_series(self, spec: IndicatorSeries) -> list[float | None]:
        if isinstance(spec, CloseSeries):
            return [float(value) for value in self.closes]
        if isinstance(spec, RsiSeriesSpec):
            return self._get_cached(self.rsi_cache, spec.period, lambda: rsi(self.closes, spec.period))
        if isinstance(spec, SmaSeries):
            return self._get_cached(self.sma_cache, spec.period, lambda: sma(self.closes, spec.period))
        if isinstance(spec, EmaSeries):
            return self._get_cached(self.ema_cache, spec.period, lambda: ema(self.closes, spec.period))
        if isinstance(spec, (MacdLineSeries, MacdSignalSeries, MacdHistogramSeries)):
            line, signal, histogram = self._get_cached(
                self.macd_cache,
                (spec.fast_period, spec.slow_period, spec.signal_period),
                lambda: macd(self.closes, spec.fast_period, spec.slow_period, spec.signal_period),
            )
            if isinstance(spec, MacdLineSeries):
                return line
            if isinstance(spec, MacdSignalSeries):
                return signal
            return histogram
        if isinstance(spec, BollingerBandSeries):
            lower, middle, upper = self._get_cached(
                self.bollinger_cache,
                (spec.period, float(spec.standard_deviations)),
                lambda: bollinger_bands(self.closes, spec.period, float(spec.standard_deviations)),
            )
            return {
                BollingerBand.LOWER: lower,
                BollingerBand.MIDDLE: middle,
                BollingerBand.UPPER: upper,
            }[spec.band]
        if isinstance(spec, IvRankSeries):
            return self._get_cached(
                self.iv_rank_series_cache,
                (self._iv_cache_key(), spec.lookback_days),
                lambda: self._build_iv_metric_series(spec.lookback_days, percentile=False),
            )
        if isinstance(spec, IvPercentileSeries):
            return self._get_cached(
                self.iv_percentile_series_cache,
                (self._iv_cache_key(), spec.lookback_days),
                lambda: self._build_iv_metric_series(spec.lookback_days, percentile=True),
            )
        if isinstance(spec, VolumeRatioSeries):
            return self._get_cached(
                self.volume_ratio_cache,
                spec.lookback_period,
                lambda: self._build_volume_ratio_series(spec.lookback_period),
            )
        if isinstance(spec, CciSeries):
            return self._get_cached(
                self.cci_cache,
                spec.period,
                lambda: cci(self.highs, self.lows, self.closes, spec.period),
            )
        if isinstance(spec, RocSeries):
            return self._get_cached(self.roc_cache, spec.period, lambda: roc(self.closes, spec.period))
        if isinstance(spec, MfiSeries):
            return self._get_cached(
                self.mfi_cache,
                spec.period,
                lambda: mfi(self.highs, self.lows, self.closes, self.volumes, spec.period),
            )
        if isinstance(spec, (StochasticKSeries, StochasticDSeries)):
            stochastic_key = (spec.k_period, spec.d_period, spec.smooth_k)
            if stochastic_key not in self.stochastic_k_cache or stochastic_key not in self.stochastic_d_cache:
                percent_k, percent_d = stochastic_oscillator(
                    self.highs,
                    self.lows,
                    self.closes,
                    k_period=spec.k_period,
                    d_period=spec.d_period,
                    smooth_k=spec.smooth_k,
                )
                self.stochastic_k_cache[stochastic_key] = percent_k
                self.stochastic_d_cache[stochastic_key] = percent_d
            if isinstance(spec, StochasticKSeries):
                return self.stochastic_k_cache[stochastic_key]
            return self.stochastic_d_cache[stochastic_key]
        if isinstance(spec, AdxSeries):
            return self._get_cached(
                self.adx_cache, spec.period, lambda: adx(self.highs, self.lows, self.closes, spec.period)
            )
        if isinstance(spec, WilliamsRSeries):
            return self._get_cached(
                self.williams_r_cache,
                spec.period,
                lambda: williams_r(self.highs, self.lows, self.closes, spec.period),
            )
        raise TypeError(f"Unsupported indicator series type: {type(spec).__name__}")

    @staticmethod
    def _has_crossover_rule(rules: Sequence) -> bool:
        """Check if any rule requires the previous bar (index-1) for crossover detection."""
        return any(
            isinstance(rule, (MovingAverageCrossoverRule, MacdRule, IndicatorLevelCrossRule, IndicatorSeriesCrossRule))
            for rule in rules
        )

    def _get_iv_series(self) -> list[float | None]:
        cache_key = self._iv_cache_key()
        cached = self.iv_series_cache.get(cache_key)
        if cached is not None:
            return cached
        series = build_estimated_iv_series(
                bars=self.bars,
                option_gateway=self.option_gateway,
                target_dte=self.config.target_dte,
                dte_tolerance_days=self.config.dte_tolerance_days,
                risk_free_rate=self.config.risk_free_rate,
                risk_free_rate_resolver=self.config.resolve_risk_free_rate,
                dividend_yield=self.config.dividend_yield,
        )
        self.iv_series_cache[cache_key] = series
        return series

    def _entry_mask_cache_key(self) -> str:
        payload = {
            "entry_rules": [
                rule.model_dump(mode="json") if hasattr(rule, "model_dump") else repr(rule)
                for rule in self.config.entry_rules
            ],
            "target_dte": self.config.target_dte,
            "dte_tolerance_days": self.config.dte_tolerance_days,
            "risk_free_rate": self.config.risk_free_rate,
            "dividend_yield": self.config.dividend_yield,
        }
        return json.dumps(payload, sort_keys=True, default=str)

    def _iv_cache_key(self) -> str:
        payload = {
            "target_dte": self.config.target_dte,
            "dte_tolerance_days": self.config.dte_tolerance_days,
            "risk_free_rate": self.config.risk_free_rate,
            "dividend_yield": self.config.dividend_yield,
        }
        return json.dumps(payload, sort_keys=True, default=str)


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
    try:
        chosen_expiration, call_contracts, put_contracts = select_preferred_common_expiration_contracts(
            option_gateway,
            entry_date=trade_date,
            target_dte=target_dte,
            dte_tolerance_days=dte_tolerance_days,
        )
    except Exception:
        return None
    if not call_contracts or not put_contracts:
        return None

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
