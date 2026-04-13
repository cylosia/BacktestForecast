from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

import backtestforecast.backtests.strategies.common as strategy_common_module  # noqa: E402
from backtestforecast.backtests.engine import OptionsBacktestEngine  # noqa: E402
from backtestforecast.backtests.types import BacktestConfig  # noqa: E402
from backtestforecast.db.session import create_readonly_session, create_session  # noqa: E402
from backtestforecast.integrations.massive_client import MassiveClient  # noqa: E402
from backtestforecast.market_data.historical_store import HistoricalMarketDataStore  # noqa: E402
from backtestforecast.pipeline.scoring import compute_backtest_score  # noqa: E402
from backtestforecast.schemas.backtests import (  # noqa: E402
    SpreadWidthConfig,
    SpreadWidthMode,
    StrikeSelection,
    StrikeSelectionMode,
    StrategyOverrides,
)
from run_massive_bearish_low_iv_calendar_screen import (  # noqa: E402
    ACCOUNT_SIZE,
    COMMISSION_PER_CONTRACT,
    RISK_PER_TRADE_PCT,
    SLIPPAGE_PCT,
    _build_risk_free_rate_curve,
    _build_symbol_bundle,
    _classify_symbols_for_regime_bucket,
    _latest_completed_massive_bar_date,
    _load_local_option_history_symbols,
    _load_recent_local_histories,
    _suppress_engine_info_logs,
)


@dataclass(frozen=True, slots=True)
class FamilyConfig:
    strategy_type: str
    target_dte: int
    dte_tolerance_days: int
    max_holding_days: int
    param_label: str
    strategy_overrides: StrategyOverrides | None = None


@dataclass(frozen=True, slots=True)
class RegimeSymbolRow:
    symbol: str
    regime_bucket: str
    live_bar_date: date
    live_close_price: float


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Use current Massive regime classification to screen all bearish strategy "
            "families with a coarse parameter set against the historical DB."
        )
    )
    parser.add_argument("--as-of-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--backtest-start", type=date.fromisoformat, default=None)
    parser.add_argument("--backtest-end", type=date.fromisoformat, default=None)
    parser.add_argument("--symbol-limit", type=int, default=None)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "logs" / "analysis",
    )
    return parser.parse_args()


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")


def _strike_selection(mode: StrikeSelectionMode, value: Decimal) -> StrikeSelection:
    return StrikeSelection(mode=mode, value=value)


def _spread_width_strike_steps(value: Decimal) -> SpreadWidthConfig:
    return SpreadWidthConfig(mode=SpreadWidthMode.STRIKE_STEPS, value=value)


ATM = _strike_selection(StrikeSelectionMode.ATM_OFFSET_STEPS, Decimal("0"))
DELTA_30 = _strike_selection(StrikeSelectionMode.DELTA_TARGET, Decimal("30"))
WIDTH_2 = _spread_width_strike_steps(Decimal("2"))


FAMILY_CONFIGS: list[FamilyConfig] = [
    FamilyConfig(
        strategy_type="long_put",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=30,
        param_label="30d_atm_put",
        strategy_overrides=StrategyOverrides(long_put_strike=ATM),
    ),
    FamilyConfig(
        strategy_type="put_calendar_spread",
        target_dte=17,
        dte_tolerance_days=5,
        max_holding_days=17,
        param_label="17d_45d_atm_put_calendar",
        strategy_overrides=StrategyOverrides(
            short_put_strike=ATM,
            calendar_far_leg_target_dte=45,
        ),
    ),
    FamilyConfig(
        strategy_type="bear_put_debit_spread",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=30,
        param_label="30d_delta30_width2",
        strategy_overrides=StrategyOverrides(
            short_put_strike=DELTA_30,
            spread_width=WIDTH_2,
        ),
    ),
    FamilyConfig(
        strategy_type="ratio_put_backspread",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=30,
        param_label="30d_delta30",
        strategy_overrides=StrategyOverrides(short_put_strike=DELTA_30),
    ),
    FamilyConfig(
        strategy_type="synthetic_put",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=30,
        param_label="30d_default",
        strategy_overrides=None,
    ),
    FamilyConfig(
        strategy_type="bear_call_credit_spread",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=30,
        param_label="30d_delta30_width2",
        strategy_overrides=StrategyOverrides(
            short_call_strike=DELTA_30,
            spread_width=WIDTH_2,
        ),
    ),
]


def _serialize_overrides(overrides: StrategyOverrides | None) -> str:
    if overrides is None:
        return ""
    return json.dumps(overrides.model_dump(mode="json", exclude_none=True), sort_keys=True)


def _load_bearish_symbols(*, live_bar_date: date, live_bars: dict[str, Any]) -> tuple[list[RegimeSymbolRow], int, date, date]:
    local_latest_date, local_histories = _load_recent_local_histories(260)
    option_history_latest_date, option_history_symbols = _load_local_option_history_symbols()
    rows: list[RegimeSymbolRow] = []
    skipped_total = 0
    for volatility_regime in ("low_iv", "none"):
        bucket_rows, skipped = _classify_symbols_for_regime_bucket(
            live_bar_date=live_bar_date,
            live_bars=live_bars,
            local_histories=local_histories,
            option_history_symbols=option_history_symbols,
            direction_regime="bearish",
            volatility_regime=volatility_regime,
        )
        skipped_total += len(skipped)
        bucket_label = f"bearish_{volatility_regime}"
        rows.extend(
            RegimeSymbolRow(
                symbol=row.symbol,
                regime_bucket=bucket_label,
                live_bar_date=row.live_bar_date,
                live_close_price=row.close_price,
            )
            for row in bucket_rows
            if row.has_local_option_history
        )
    rows.sort(key=lambda item: (item.regime_bucket, item.symbol))
    return rows, skipped_total, local_latest_date, option_history_latest_date


def _backtest_bearish_families(
    *,
    symbols: list[RegimeSymbolRow],
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    store = HistoricalMarketDataStore(create_session, create_readonly_session)
    risk_free_rate_curve = _build_risk_free_rate_curve(store, start_date, end_date)
    engine = OptionsBacktestEngine()
    total_backtests = len(symbols) * len(FAMILY_CONFIGS)
    completed_backtests = 0
    results: list[dict[str, Any]] = []

    for symbol_index, row in enumerate(symbols, start=1):
        print(f"[backtest] {symbol_index}/{len(symbols)} symbol={row.symbol} bucket={row.regime_bucket}")
        bars, earnings_dates, ex_dividend_dates, option_gateway = _build_symbol_bundle(
            store,
            symbol=row.symbol,
            start_date=start_date,
            end_date=end_date,
        )
        for family in FAMILY_CONFIGS:
            completed_backtests += 1
            result_row = {
                "symbol": row.symbol,
                "regime_bucket": row.regime_bucket,
                "live_bar_date": row.live_bar_date.isoformat(),
                "live_close_price": row.live_close_price,
                "strategy_type": family.strategy_type,
                "target_dte": family.target_dte,
                "dte_tolerance_days": family.dte_tolerance_days,
                "max_holding_days": family.max_holding_days,
                "param_label": family.param_label,
                "strategy_overrides_json": _serialize_overrides(family.strategy_overrides),
                "score": None,
                "trade_count": None,
                "decided_trades": None,
                "total_roi_pct": None,
                "win_rate": None,
                "max_drawdown_pct": None,
                "sharpe_ratio": None,
                "average_holding_period_days": None,
                "warnings": "",
                "error": "",
            }
            config = BacktestConfig(
                symbol=row.symbol,
                strategy_type=family.strategy_type,
                start_date=start_date,
                end_date=end_date,
                target_dte=family.target_dte,
                dte_tolerance_days=family.dte_tolerance_days,
                max_holding_days=family.max_holding_days,
                account_size=ACCOUNT_SIZE,
                risk_per_trade_pct=RISK_PER_TRADE_PCT,
                commission_per_contract=COMMISSION_PER_CONTRACT,
                entry_rules=[],
                risk_free_rate=risk_free_rate_curve.default_rate,
                risk_free_rate_curve=risk_free_rate_curve,
                dividend_yield=0.0,
                slippage_pct=SLIPPAGE_PCT,
                strategy_overrides=family.strategy_overrides,
            )
            try:
                execution = engine.run(
                    config,
                    bars,
                    earnings_dates,
                    option_gateway,
                    ex_dividend_dates=ex_dividend_dates,
                )
                summary = asdict(execution.summary)
                warning_codes = sorted(
                    {
                        str(warning.get("code"))
                        for warning in execution.warnings
                        if isinstance(warning, dict) and warning.get("code")
                    }
                )
                result_row.update(
                    {
                        "score": compute_backtest_score(summary),
                        "trade_count": execution.summary.trade_count,
                        "decided_trades": execution.summary.decided_trades,
                        "total_roi_pct": execution.summary.total_roi_pct,
                        "win_rate": execution.summary.win_rate,
                        "max_drawdown_pct": execution.summary.max_drawdown_pct,
                        "sharpe_ratio": execution.summary.sharpe_ratio,
                        "average_holding_period_days": execution.summary.average_holding_period_days,
                        "warnings": ",".join(warning_codes),
                    }
                )
            except Exception as exc:
                result_row["error"] = str(exc)
            results.append(result_row)
            if completed_backtests % 100 == 0 or completed_backtests == total_backtests:
                print(f"[backtest] completed={completed_backtests}/{total_backtests}")

    results.sort(
        key=lambda item: (
            float("-inf") if item["score"] is None else float(item["score"]),
            float("-inf") if item["total_roi_pct"] is None else float(item["total_roi_pct"]),
        ),
        reverse=True,
    )
    return results


def main() -> int:
    args = _parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    _suppress_engine_info_logs()
    strategy_common_module._logger = type(
        "QuietStrategyLogger",
        (),
        {
            "debug": staticmethod(lambda *args, **kwargs: None),
            "warning": staticmethod(getattr(strategy_common_module._logger, "warning", lambda *args, **kwargs: None)),
            "info": staticmethod(getattr(strategy_common_module._logger, "info", lambda *args, **kwargs: None)),
        },
    )()

    with MassiveClient() as client:
        live_bar_date, live_bars = _latest_completed_massive_bar_date(client, as_of_date=args.as_of_date)

    symbols, skipped_total, local_latest_date, option_history_latest_date = _load_bearish_symbols(
        live_bar_date=live_bar_date,
        live_bars=live_bars,
    )
    if args.symbol_limit is not None:
        symbols = symbols[: args.symbol_limit]

    backtest_end = args.backtest_end or min(local_latest_date, option_history_latest_date)
    backtest_start = args.backtest_start or (backtest_end - timedelta(days=730))

    print(f"[screen] latest completed Massive grouped bar date: {live_bar_date.isoformat()} ({len(live_bars)} symbols)")
    print(f"[screen] local underlying latest date: {local_latest_date.isoformat()}")
    print(f"[screen] local option-history latest date: {option_history_latest_date.isoformat()}")
    print(f"[screen] bearish symbols with local option history: {len(symbols)} (skipped for missing history: {skipped_total})")
    print(f"[screen] backtest window: {backtest_start.isoformat()} -> {backtest_end.isoformat()}")

    symbol_rows = [asdict(row) for row in symbols]
    symbol_csv = args.output_dir / f"massive_bearish_family_symbols_{args.as_of_date:%Y%m%d}.csv"
    _write_csv(
        symbol_csv,
        symbol_rows,
        ["symbol", "regime_bucket", "live_bar_date", "live_close_price"],
    )

    results = _backtest_bearish_families(
        symbols=symbols,
        start_date=backtest_start,
        end_date=backtest_end,
    )
    full_csv = args.output_dir / (
        f"massive_bearish_family_backtests_{backtest_start:%Y%m%d}_{backtest_end:%Y%m%d}_{args.as_of_date:%Y%m%d}.csv"
    )
    _write_csv(
        full_csv,
        results,
        [
            "symbol",
            "regime_bucket",
            "live_bar_date",
            "live_close_price",
            "strategy_type",
            "target_dte",
            "dte_tolerance_days",
            "max_holding_days",
            "param_label",
            "strategy_overrides_json",
            "score",
            "trade_count",
            "decided_trades",
            "total_roi_pct",
            "win_rate",
            "max_drawdown_pct",
            "sharpe_ratio",
            "average_holding_period_days",
            "warnings",
            "error",
        ],
    )

    filtered = [
        row for row in results
        if row["total_roi_pct"] is not None
        and row["trade_count"] is not None
        and row["max_drawdown_pct"] is not None
        and float(row["total_roi_pct"]) > 80.0
        and int(row["trade_count"]) >= 10
        and float(row["max_drawdown_pct"]) <= 15.0
    ]
    filtered_csv = args.output_dir / (
        f"massive_bearish_family_backtests_{backtest_start:%Y%m%d}_{backtest_end:%Y%m%d}_{args.as_of_date:%Y%m%d}_roi80_trades10_dd15.csv"
    )
    _write_csv(
        filtered_csv,
        filtered,
        [
            "symbol",
            "regime_bucket",
            "live_bar_date",
            "live_close_price",
            "strategy_type",
            "target_dte",
            "dte_tolerance_days",
            "max_holding_days",
            "param_label",
            "strategy_overrides_json",
            "score",
            "trade_count",
            "decided_trades",
            "total_roi_pct",
            "win_rate",
            "max_drawdown_pct",
            "sharpe_ratio",
            "average_holding_period_days",
            "warnings",
            "error",
        ],
    )

    strategy_counts: dict[str, int] = {}
    for row in filtered:
        strategy_counts[row["strategy_type"]] = strategy_counts.get(row["strategy_type"], 0) + 1

    summary_json = args.output_dir / (
        f"massive_bearish_family_screen_summary_{args.as_of_date:%Y%m%d}.json"
    )
    _write_json(
        summary_json,
        {
            "as_of_date": args.as_of_date.isoformat(),
            "live_bar_date": live_bar_date.isoformat(),
            "local_underlying_latest_date": local_latest_date.isoformat(),
            "local_option_history_latest_date": option_history_latest_date.isoformat(),
            "backtest_start": backtest_start.isoformat(),
            "backtest_end": backtest_end.isoformat(),
            "family_config_count": len(FAMILY_CONFIGS),
            "matching_symbol_count": len(symbols),
            "skipped_history_count": skipped_total,
            "full_result_count": len(results),
            "filtered_result_count": len(filtered),
            "filtered_counts_by_strategy": strategy_counts,
            "paths": {
                "symbols_csv": str(symbol_csv),
                "full_csv": str(full_csv),
                "filtered_csv": str(filtered_csv),
            },
        },
    )

    print(f"[done] symbols csv: {symbol_csv}")
    print(f"[done] full results csv: {full_csv}")
    print(f"[done] filtered results csv: {filtered_csv}")
    print(f"[done] summary json: {summary_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
