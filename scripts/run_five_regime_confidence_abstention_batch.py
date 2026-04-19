from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

import analyze_five_regime_confidence_abstention as abstain  # noqa: E402
import evaluate_five_regime_price_predictions as evaluator  # noqa: E402


def _display_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(ROOT)).replace("\\", "/")
    except ValueError:
        return str(resolved)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the confidence-threshold abstention sweep across a directory of single-symbol "
            "five-regime evaluation results and rank symbols by improvement."
        )
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        required=True,
        help="Directory containing one JSON result per symbol from evaluate_five_regime_price_predictions.",
    )
    parser.add_argument(
        "--confidence-thresholds",
        default=evaluator.DEFAULT_CONFIDENCE_THRESHOLDS,
        help="Comma-separated normalized confidence thresholds to sweep.",
    )
    parser.add_argument(
        "--min-trades-for-best",
        type=int,
        default=30,
        help="Minimum directional predictions required for a threshold to be eligible as best.",
    )
    parser.add_argument(
        "--rank-metric",
        choices=("exact_accuracy", "macro_precision"),
        default="exact_accuracy",
        help="Which improvement metric to rank symbols by. Defaults to exact_accuracy.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=20,
        help="How many top-ranked symbols to print. Defaults to 20.",
    )
    parser.add_argument(
        "--summary-csv",
        type=Path,
        help="Optional CSV output path for the full per-symbol ranking table.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        help="Optional JSON output path for the full ranking payload.",
    )
    return parser.parse_args()


def _best_row_for_metric(
    *,
    rows: list[dict[str, object]],
    min_trades_for_best: int,
    rank_metric: str,
) -> dict[str, object] | None:
    eligible_rows = [row for row in rows if int(row["trade_count"]) >= min_trades_for_best]
    if not eligible_rows:
        return None
    if rank_metric == "macro_precision":
        return max(
            eligible_rows,
            key=lambda row: (
                float(row["macro_precision_pct"]),
                float(row["exact_accuracy_pct"]),
                float(row["trade_count"]),
                -float(row["confidence_threshold"]),
            ),
        )
    return max(
        eligible_rows,
        key=lambda row: (
            float(row["exact_accuracy_pct"]),
            float(row["directional_accuracy_pct"]),
            float(row["trade_count"]),
            -float(row["confidence_threshold"]),
        ),
    )


def _summary_row(
    *,
    result_path: Path,
    symbol: str,
    baseline_row: dict[str, object],
    best_row: dict[str, object] | None,
) -> dict[str, object]:
    row: dict[str, object] = {
        "symbol": symbol,
        "result_json": _display_path(result_path),
        "baseline_threshold": baseline_row["confidence_threshold"],
        "baseline_trade_count": baseline_row["trade_count"],
        "baseline_coverage_pct": baseline_row["coverage_pct"],
        "baseline_exact_accuracy_pct": baseline_row["exact_accuracy_pct"],
        "baseline_directional_accuracy_pct": baseline_row["directional_accuracy_pct"],
        "baseline_macro_precision_pct": baseline_row["macro_precision_pct"],
        "baseline_best_bucket": baseline_row["best_bucket"],
        "baseline_best_bucket_precision_pct": baseline_row["best_bucket_precision_pct"],
        "baseline_best_bucket_count": baseline_row["best_bucket_count"],
    }
    if best_row is None:
        row.update(
            {
                "best_threshold": None,
                "best_trade_count": 0,
                "best_coverage_pct": 0.0,
                "best_exact_accuracy_pct": 0.0,
                "best_directional_accuracy_pct": 0.0,
                "best_macro_precision_pct": 0.0,
                "best_best_bucket": None,
                "best_best_bucket_precision_pct": None,
                "best_best_bucket_count": 0,
                "exact_accuracy_delta_pct": None,
                "directional_accuracy_delta_pct": None,
                "macro_precision_delta_pct": None,
                "coverage_delta_pct": None,
            }
        )
        return row

    row.update(
        {
            "best_threshold": best_row["confidence_threshold"],
            "best_trade_count": best_row["trade_count"],
            "best_coverage_pct": best_row["coverage_pct"],
            "best_exact_accuracy_pct": best_row["exact_accuracy_pct"],
            "best_directional_accuracy_pct": best_row["directional_accuracy_pct"],
            "best_macro_precision_pct": best_row["macro_precision_pct"],
            "best_best_bucket": best_row["best_bucket"],
            "best_best_bucket_precision_pct": best_row["best_bucket_precision_pct"],
            "best_best_bucket_count": best_row["best_bucket_count"],
            "exact_accuracy_delta_pct": round(
                float(best_row["exact_accuracy_pct"]) - float(baseline_row["exact_accuracy_pct"]),
                4,
            ),
            "directional_accuracy_delta_pct": round(
                float(best_row["directional_accuracy_pct"]) - float(baseline_row["directional_accuracy_pct"]),
                4,
            ),
            "macro_precision_delta_pct": round(
                float(best_row["macro_precision_pct"]) - float(baseline_row["macro_precision_pct"]),
                4,
            ),
            "coverage_delta_pct": round(
                float(best_row["coverage_pct"]) - float(baseline_row["coverage_pct"]),
                4,
            ),
        }
    )
    return row


def _write_summary_csv(*, rows: list[dict[str, object]], path: Path) -> None:
    fieldnames = [
        "symbol",
        "result_json",
        "baseline_threshold",
        "baseline_trade_count",
        "baseline_coverage_pct",
        "baseline_exact_accuracy_pct",
        "baseline_directional_accuracy_pct",
        "baseline_macro_precision_pct",
        "baseline_best_bucket",
        "baseline_best_bucket_precision_pct",
        "baseline_best_bucket_count",
        "best_threshold",
        "best_trade_count",
        "best_coverage_pct",
        "best_exact_accuracy_pct",
        "best_directional_accuracy_pct",
        "best_macro_precision_pct",
        "best_best_bucket",
        "best_best_bucket_precision_pct",
        "best_best_bucket_count",
        "exact_accuracy_delta_pct",
        "directional_accuracy_delta_pct",
        "macro_precision_delta_pct",
        "coverage_delta_pct",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})


def main() -> int:
    args = _parse_args()
    result_paths = sorted(args.results_dir.glob("*.json"))
    if not result_paths:
        raise SystemExit(f"No JSON files found in {args.results_dir}")

    confidence_thresholds = evaluator._parse_float_csv(args.confidence_thresholds)
    summary_rows: list[dict[str, object]] = []
    detailed_rows: list[dict[str, object]] = []

    for result_path in result_paths:
        symbol_row, best_result = abstain._load_result_payload(result_path)
        prediction_series = abstain._build_prediction_series(symbol_row=symbol_row, best_result=best_result)
        rows = [
            abstain._directional_subset_metrics(
                predicted_regimes=prediction_series["predicted_regimes"],
                actual_regimes=prediction_series["actual_regimes"],
                confidence_scores=prediction_series["confidence_scores"],
                threshold=threshold,
            )
            for threshold in confidence_thresholds
        ]
        baseline_row = next(row for row in rows if float(row["confidence_threshold"]) == 0.0)
        best_row = _best_row_for_metric(
            rows=rows,
            min_trades_for_best=args.min_trades_for_best,
            rank_metric=args.rank_metric,
        )
        summary_row = _summary_row(
            result_path=result_path,
            symbol=str(prediction_series["symbol"]),
            baseline_row=baseline_row,
            best_row=best_row,
        )
        summary_rows.append(summary_row)
        detailed_rows.append(
            {
                "symbol": prediction_series["symbol"],
                "result_json": _display_path(result_path),
                "rows": rows,
            }
        )

    sort_field = "macro_precision_delta_pct" if args.rank_metric == "macro_precision" else "exact_accuracy_delta_pct"
    summary_rows.sort(
        key=lambda row: (
            float("-inf") if row.get(sort_field) is None else float(row[sort_field]),
            float("-inf") if row.get("best_trade_count") is None else float(row["best_trade_count"]),
            str(row["symbol"]),
        ),
        reverse=True,
    )

    print(
        f"rank_metric={args.rank_metric} min_trades_for_best={args.min_trades_for_best} "
        f"symbol_count={len(summary_rows)}"
    )
    print(
        "rank\tsymbol\tbest_threshold\tdelta_exact_pct\tdelta_macro_precision_pct\t"
        "baseline_exact_pct\tbest_exact_pct\tbaseline_trades\tbest_trades\tcoverage_delta_pct"
    )
    for index, row in enumerate(summary_rows[: args.top_k], start=1):
        best_threshold = "-" if row["best_threshold"] is None else f"{float(row['best_threshold']):.2f}"
        exact_delta = "-" if row["exact_accuracy_delta_pct"] is None else f"{float(row['exact_accuracy_delta_pct']):.4f}"
        macro_delta = (
            "-"
            if row["macro_precision_delta_pct"] is None
            else f"{float(row['macro_precision_delta_pct']):.4f}"
        )
        best_exact = "-" if row["best_exact_accuracy_pct"] is None else f"{float(row['best_exact_accuracy_pct']):.4f}"
        print(
            f"{index}\t{row['symbol']}\t{best_threshold}\t{exact_delta}\t{macro_delta}\t"
            f"{float(row['baseline_exact_accuracy_pct']):.4f}\t{best_exact}\t"
            f"{int(row['baseline_trade_count'])}\t{int(row['best_trade_count'])}\t"
            f"{float(row['coverage_delta_pct']):.4f}"
        )

    if args.summary_csv is not None:
        _write_summary_csv(rows=summary_rows, path=args.summary_csv)
        print(f"Wrote {args.summary_csv}")

    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(
            json.dumps(
                {
                    "rank_metric": args.rank_metric,
                    "min_trades_for_best": args.min_trades_for_best,
                    "confidence_thresholds": list(confidence_thresholds),
                    "summary_rows": summary_rows,
                    "detailed_rows": detailed_rows,
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        print(f"Wrote {args.output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
