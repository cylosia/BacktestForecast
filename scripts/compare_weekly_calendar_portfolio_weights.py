from __future__ import annotations

import argparse
import csv
import math
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

DEFAULT_SELECTION_CSV = ROOT / "logs" / "weekly_calendar_policy_walk_forward_sweep2_top22_train2y_20251231_q1_2026_selection.csv"
DEFAULT_RESULTS_CSV = ROOT / "logs" / "weekly_calendar_policy_walk_forward_sweep2_top22_train2y_20251231_q1_2026_results.csv"
DEFAULT_LEDGER_CSV = ROOT / "logs" / "weekly_calendar_policy_walk_forward_sweep2_top22_train2y_20251231_q1_2026_trade_ledger.csv"
DEFAULT_OUTPUT_PREFIX = ROOT / "logs" / "weekly_calendar_policy_walk_forward_top22_train2y_20251231_q1_2026_weight_compare"
BASE_SCHEMES = ("equal", "rank_bucket", "median_shrunk", "total_roi_shrunk")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare weighted portfolio variants for a weekly-calendar walk-forward using cached "
            "selection, per-symbol results, and trade ledger outputs."
        )
    )
    parser.add_argument("--selection-csv", type=Path, default=DEFAULT_SELECTION_CSV)
    parser.add_argument("--results-csv", type=Path, default=DEFAULT_RESULTS_CSV)
    parser.add_argument("--ledger-csv", type=Path, default=DEFAULT_LEDGER_CSV)
    parser.add_argument("--output-prefix", type=Path, default=DEFAULT_OUTPUT_PREFIX)
    parser.add_argument(
        "--trade-count-cap",
        type=float,
        default=100.0,
        help="Shrinkage cap used by the median-shrunk and total-roi-shrunk schemes. Defaults to 100.",
    )
    parser.add_argument(
        "--total-roi-shrunk-cap-pcts",
        default="",
        help=(
            "Optional comma-separated max symbol weights, in percent, used to add capped total-roi-shrunk "
            "schemes such as '10,12'."
        ),
    )
    return parser.parse_args()


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _as_float(value: object) -> float:
    if value in (None, ""):
        return 0.0
    return float(value)


def _parse_cap_pcts(raw_value: str) -> list[float]:
    values: list[float] = []
    for chunk in raw_value.split(","):
        item = chunk.strip()
        if not item:
            continue
        values.append(float(item))
    return values


def _normalize_scores(symbols: list[str], raw_scores: dict[str, float]) -> dict[str, float]:
    cleaned: dict[str, float] = {}
    for symbol in symbols:
        score = raw_scores.get(symbol, 0.0)
        cleaned[symbol] = score if math.isfinite(score) and score > 0.0 else 0.0
    total = sum(cleaned.values())
    if total <= 0.0:
        equal_weight = 1.0 / len(symbols) if symbols else 0.0
        return {symbol: equal_weight for symbol in symbols}
    return {symbol: cleaned[symbol] / total for symbol in symbols}


def _cap_normalized_weights(weights: dict[str, float], max_weight: float) -> dict[str, float]:
    if not weights:
        return {}
    if max_weight <= 0.0:
        raise ValueError("max_weight must be positive")
    if max_weight * len(weights) < 1.0 - 1e-12:
        raise ValueError("max_weight is too small for the number of symbols")

    remaining = dict(weights)
    capped: dict[str, float] = {}
    while True:
        over_limit = {symbol for symbol, weight in remaining.items() if weight > max_weight + 1e-12}
        if not over_limit:
            residual = 1.0 - sum(capped.values())
            remaining_total = sum(remaining.values())
            if remaining_total > 0.0:
                scale = residual / remaining_total
                remaining = {symbol: weight * scale for symbol, weight in remaining.items()}
            elif remaining:
                equal_weight = residual / len(remaining)
                remaining = {symbol: equal_weight for symbol in remaining}
            final_weights = {**remaining, **capped}
            total = sum(final_weights.values())
            if total > 0.0:
                final_weights = {symbol: weight / total for symbol, weight in final_weights.items()}
            return final_weights

        for symbol in over_limit:
            capped[symbol] = max_weight
            remaining.pop(symbol, None)

        residual = 1.0 - sum(capped.values())
        if residual < -1e-12:
            raise ValueError("capped weights exceed 100%")
        remaining_total = sum(remaining.values())
        if remaining_total > 0.0:
            remaining = {symbol: weight / remaining_total * residual for symbol, weight in remaining.items()}
        elif remaining:
            equal_weight = residual / len(remaining)
            remaining = {symbol: equal_weight for symbol in remaining}


def _weighted_median(values: list[float], weights: list[float]) -> float:
    pairs = [(value, weight) for value, weight in zip(values, weights) if math.isfinite(value) and weight > 0.0]
    if not pairs:
        return 0.0
    pairs.sort(key=lambda item: item[0])
    total_weight = sum(weight for _, weight in pairs)
    threshold = total_weight / 2.0
    cumulative = 0.0
    for value, weight in pairs:
        cumulative += weight
        if cumulative >= threshold - 1e-12:
            return value
    return pairs[-1][0]


def _rank_bucket_raw_score(rank: int) -> float:
    if rank <= 5:
        return 1.5
    if rank <= 10:
        return 1.25
    if rank <= 16:
        return 1.0
    return 0.75


def _shrink_factor(trade_count: float, trade_count_cap: float) -> float:
    if trade_count_cap <= 0.0:
        return 1.0
    bounded = min(max(trade_count, 0.0), trade_count_cap)
    return math.sqrt(bounded / trade_count_cap)


def _build_scheme_weights(
    selection_rows: list[dict[str, str]],
    trade_count_cap: float,
    total_roi_shrunk_cap_pcts: list[float] | None = None,
) -> dict[str, dict[str, float]]:
    ordered_rows = sorted(selection_rows, key=lambda row: int(row["rank"]))
    symbols = [str(row["symbol"]) for row in ordered_rows]

    equal_weights = {symbol: 1.0 / len(symbols) for symbol in symbols}
    rank_bucket_weights = _normalize_scores(
        symbols,
        {str(row["symbol"]): _rank_bucket_raw_score(int(row["rank"])) for row in ordered_rows},
    )
    median_shrunk_weights = _normalize_scores(
        symbols,
        {
            str(row["symbol"]): max(_as_float(row["training_median_roi_on_margin_pct"]), 0.0)
            * _shrink_factor(_as_float(row["training_trade_count"]), trade_count_cap)
            for row in ordered_rows
        },
    )
    total_roi_shrunk_weights = _normalize_scores(
        symbols,
        {
            str(row["symbol"]): max(_as_float(row["training_total_roi_pct"]), 0.0)
            * _shrink_factor(_as_float(row["training_trade_count"]), trade_count_cap)
            for row in ordered_rows
        },
    )
    weights_by_scheme = {
        "equal": equal_weights,
        "rank_bucket": rank_bucket_weights,
        "median_shrunk": median_shrunk_weights,
        "total_roi_shrunk": total_roi_shrunk_weights,
    }
    for cap_pct in total_roi_shrunk_cap_pcts or []:
        cap_fraction = cap_pct / 100.0
        scheme = f"total_roi_shrunk_cap_{int(cap_pct) if float(cap_pct).is_integer() else str(cap_pct).replace('.', '_')}pct"
        weights_by_scheme[scheme] = _cap_normalized_weights(total_roi_shrunk_weights, cap_fraction)
    return weights_by_scheme


def _summarize_scheme(
    *,
    scheme: str,
    weights: dict[str, float],
    selection_rows: list[dict[str, str]],
    result_rows: list[dict[str, str]],
    ledger_rows: list[dict[str, str]],
) -> tuple[dict[str, object], list[dict[str, object]]]:
    symbol_count = len(weights)
    multipliers = {symbol: weight * symbol_count for symbol, weight in weights.items()}

    total_capital = 0.0
    total_pnl = 0.0
    roi_values: list[float] = []
    roi_weights: list[float] = []
    weekly: dict[str, dict[str, object]] = {}
    for row in ledger_rows:
        symbol = str(row["symbol"])
        multiplier = multipliers.get(symbol, 0.0)
        if multiplier <= 0.0:
            continue
        capital = _as_float(row.get("capital_required"))
        pnl = _as_float(row.get("net_pnl"))
        total_capital += multiplier * capital
        total_pnl += multiplier * pnl
        roi = _as_float(row.get("roi_on_capital_required_pct"))
        roi_values.append(roi)
        roi_weights.append(multiplier)

        week = str(row["entry_date"])
        week_bucket = weekly.setdefault(
            week,
            {
                "trade_count": 0,
                "total_capital_required": 0.0,
                "total_net_pnl": 0.0,
                "roi_values": [],
                "roi_weights": [],
            },
        )
        week_bucket["trade_count"] = int(week_bucket["trade_count"]) + 1
        week_bucket["total_capital_required"] = float(week_bucket["total_capital_required"]) + (multiplier * capital)
        week_bucket["total_net_pnl"] = float(week_bucket["total_net_pnl"]) + (multiplier * pnl)
        week_bucket["roi_values"].append(roi)
        week_bucket["roi_weights"].append(multiplier)

    weighted_avg_trade_roi = (
        sum(value * weight for value, weight in zip(roi_values, roi_weights)) / sum(roi_weights) if roi_weights else 0.0
    )
    weighted_median_trade_roi = _weighted_median(roi_values, roi_weights)

    weekly_rows: list[dict[str, object]] = []
    weekly_medians: list[float] = []
    for week in sorted(weekly):
        bucket = weekly[week]
        week_capital = float(bucket["total_capital_required"])
        week_pnl = float(bucket["total_net_pnl"])
        week_median = _weighted_median(list(bucket["roi_values"]), list(bucket["roi_weights"]))
        weekly_medians.append(week_median)
        weekly_rows.append(
            {
                "scheme": scheme,
                "entry_week": week,
                "trade_count": int(bucket["trade_count"]),
                "total_capital_required": round(week_capital, 4),
                "total_net_pnl": round(week_pnl, 4),
                "roi_on_capital_required_pct": round(week_pnl / week_capital * 100.0, 4) if week_capital > 0.0 else 0.0,
                "median_roi_per_trade_pct": round(week_median, 4),
            }
        )

    rank_lookup = {str(row["symbol"]): int(row["rank"]) for row in selection_rows}
    summary_row = {
        "scheme": scheme,
        "symbol_count": symbol_count,
        "trade_count": len(ledger_rows),
        "total_capital_required": round(total_capital, 4),
        "total_net_pnl": round(total_pnl, 4),
        "roi_on_capital_required_pct": round(total_pnl / total_capital * 100.0, 4) if total_capital > 0.0 else 0.0,
        "average_roi_per_trade_pct": round(weighted_avg_trade_roi, 4),
        "median_roi_per_trade_pct": round(weighted_median_trade_roi, 4),
        "average_weekly_median_roi_per_trade_pct": round(sum(weekly_medians) / len(weekly_medians), 4) if weekly_medians else 0.0,
        "max_symbol_weight_pct": round(max(weights.values()) * 100.0, 4) if weights else 0.0,
        "top3_symbol_weight_pct": round(sum(sorted(weights.values(), reverse=True)[:3]) * 100.0, 4) if weights else 0.0,
        "top_weighted_symbols": ", ".join(
            f"{symbol}:{weights[symbol] * 100.0:.2f}%"
            for symbol in sorted(weights, key=lambda item: (-weights[item], rank_lookup.get(item, 9999)))[:5]
        ),
    }

    symbol_rows: list[dict[str, object]] = []
    for result_row in sorted(result_rows, key=lambda row: int(rank_lookup[str(row["symbol"])])):
        symbol = str(result_row["symbol"])
        weight = weights.get(symbol, 0.0)
        multiplier = multipliers.get(symbol, 0.0)
        capital = _as_float(result_row.get("total_capital_required"))
        pnl = _as_float(result_row.get("total_net_pnl"))
        symbol_rows.append(
            {
                "scheme": scheme,
                "rank": rank_lookup[symbol],
                "symbol": symbol,
                "weight_pct": round(weight * 100.0, 4),
                "multiplier": round(multiplier, 6),
                "training_trade_count": int(float(result_row["training_trade_count"])),
                "training_total_net_pnl": round(_as_float(result_row["training_total_net_pnl"]), 4),
                "training_average_roi_on_margin_pct": round(_as_float(result_row["training_average_roi_on_margin_pct"]), 4),
                "training_median_roi_on_margin_pct": round(_as_float(result_row["training_median_roi_on_margin_pct"]), 4),
                "symbol_trade_count": int(float(result_row["trade_count"])),
                "symbol_total_capital_required": round(capital, 4),
                "symbol_total_net_pnl": round(pnl, 4),
                "symbol_roi_on_capital_required_pct": round(_as_float(result_row["roi_on_capital_required_pct"]), 4),
                "weighted_total_capital_required": round(multiplier * capital, 4),
                "weighted_total_net_pnl": round(multiplier * pnl, 4),
            }
        )
    return summary_row, weekly_rows + symbol_rows


def main() -> int:
    args = _parse_args()
    output_prefix = args.output_prefix
    if not output_prefix.is_absolute():
        output_prefix = ROOT / output_prefix

    selection_rows = _read_csv(args.selection_csv)
    result_rows = _read_csv(args.results_csv)
    ledger_rows = _read_csv(args.ledger_csv)

    cap_pcts = _parse_cap_pcts(args.total_roi_shrunk_cap_pcts)
    weights_by_scheme = _build_scheme_weights(
        selection_rows,
        trade_count_cap=args.trade_count_cap,
        total_roi_shrunk_cap_pcts=cap_pcts,
    )
    scheme_names = list(weights_by_scheme.keys())

    summary_rows: list[dict[str, object]] = []
    weekly_rows: list[dict[str, object]] = []
    symbol_rows: list[dict[str, object]] = []
    rank_lookup = {str(row["symbol"]): int(row["rank"]) for row in selection_rows}
    weight_rows: list[dict[str, object]] = []
    for symbol in sorted(rank_lookup, key=lambda item: rank_lookup[item]):
        row: dict[str, object] = {"rank": rank_lookup[symbol], "symbol": symbol}
        for scheme in scheme_names:
            weight = weights_by_scheme[scheme][symbol]
            row[f"{scheme}_weight_pct"] = round(weight * 100.0, 4)
            row[f"{scheme}_multiplier"] = round(weight * len(rank_lookup), 6)
        weight_rows.append(row)

    result_lookup = {str(row["symbol"]): row for row in result_rows}
    for scheme in scheme_names:
        summary_row, scheme_rows = _summarize_scheme(
            scheme=scheme,
            weights=weights_by_scheme[scheme],
            selection_rows=selection_rows,
            result_rows=[result_lookup[str(row["symbol"])] for row in selection_rows],
            ledger_rows=ledger_rows,
        )
        summary_rows.append(summary_row)
        for row in scheme_rows:
            if "entry_week" in row:
                weekly_rows.append(row)
            else:
                symbol_rows.append(row)

    summary_csv = Path(f"{output_prefix}_summary.csv")
    weights_csv = Path(f"{output_prefix}_weights.csv")
    weekly_csv = Path(f"{output_prefix}_weekly.csv")
    symbol_csv = Path(f"{output_prefix}_symbols.csv")

    _write_csv(summary_csv, summary_rows, list(summary_rows[0].keys()) if summary_rows else ["scheme"])
    _write_csv(weights_csv, weight_rows, list(weight_rows[0].keys()) if weight_rows else ["rank", "symbol"])
    _write_csv(weekly_csv, weekly_rows, list(weekly_rows[0].keys()) if weekly_rows else ["scheme", "entry_week"])
    _write_csv(symbol_csv, symbol_rows, list(symbol_rows[0].keys()) if symbol_rows else ["scheme", "symbol"])

    for row in summary_rows:
        print(
            ",".join(
                [
                    str(row["scheme"]),
                    f"roi={row['roi_on_capital_required_pct']}",
                    f"median_trade_roi={row['median_roi_per_trade_pct']}",
                    f"avg_weekly_median={row['average_weekly_median_roi_per_trade_pct']}",
                    f"top3_weight_pct={row['top3_symbol_weight_pct']}",
                ]
            )
        )
    print(f"summary_csv={summary_csv.relative_to(ROOT).as_posix()}")
    print(f"weights_csv={weights_csv.relative_to(ROOT).as_posix()}")
    print(f"weekly_csv={weekly_csv.relative_to(ROOT).as_posix()}")
    print(f"symbol_csv={symbol_csv.relative_to(ROOT).as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
