from __future__ import annotations

import math


BASE_SCHEMES = ("equal", "rank_bucket", "median_shrunk", "total_roi_shrunk")


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
        if float(cap_pct).is_integer():
            cap_label = str(int(cap_pct))
        else:
            cap_label = str(cap_pct).replace(".", "_")
        scheme = f"total_roi_shrunk_cap_{cap_label}pct"
        weights_by_scheme[scheme] = _cap_normalized_weights(total_roi_shrunk_weights, cap_fraction)
    return weights_by_scheme


def _build_weight_scheme(
    selection_rows: list[dict[str, str]],
    scheme: str,
    trade_count_cap: float,
    max_symbol_weight_pct: float | None = None,
) -> tuple[str, dict[str, float]]:
    weights_by_scheme = _build_scheme_weights(selection_rows, trade_count_cap=trade_count_cap)
    if scheme not in weights_by_scheme:
        raise ValueError(f"Unknown weighting scheme: {scheme}")
    weights = dict(weights_by_scheme[scheme])
    if max_symbol_weight_pct is not None:
        cap_fraction = max_symbol_weight_pct / 100.0
        weights = _cap_normalized_weights(weights, cap_fraction)
        if float(max_symbol_weight_pct).is_integer():
            cap_label = str(int(max_symbol_weight_pct))
        else:
            cap_label = str(max_symbol_weight_pct).replace(".", "_")
        scheme = f"{scheme}_cap_{cap_label}pct"
    return scheme, weights
