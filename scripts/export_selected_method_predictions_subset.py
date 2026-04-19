from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from pathlib import Path

from sqlalchemy.orm import Session, sessionmaker

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.predict_weekly_price_movement as pwm

DEFAULT_SUMMARY_CSV = (
    ROOT
    / "logs"
    / "batch"
    / "weekly_price_movement"
    / "weekly_options_over5_median80_mintrades70_auto_v13_surface_features"
    / "summary.csv"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Export selected-method weekly price-movement predictions for a symbol subset "
            "listed in a CSV."
        )
    )
    parser.add_argument("--input-csv", type=Path, required=True, help="CSV containing at least a symbol column.")
    parser.add_argument("--output-csv", type=Path, required=True, help="CSV path for predictions.")
    parser.add_argument("--as-of-date", type=pwm.date.fromisoformat, required=True, help="Prediction as-of date.")
    parser.add_argument(
        "--start-date",
        type=pwm.date.fromisoformat,
        default=pwm.date.fromisoformat("2024-01-01"),
        help="History start date for feature construction. Defaults to 2024-01-01.",
    )
    parser.add_argument(
        "--summary-csv",
        type=Path,
        default=DEFAULT_SUMMARY_CSV,
        help="Summary CSV containing selected_method per symbol.",
    )
    parser.add_argument(
        "--symbol-column",
        default="symbol",
        help="Column name in --input-csv containing the ticker symbol. Defaults to symbol.",
    )
    parser.add_argument("--start-index", type=int, default=0, help="Zero-based start index into the input rows.")
    parser.add_argument("--end-index", type=int, default=None, help="Optional end index (exclusive).")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL", ""),
        help="SQLAlchemy database URL. Defaults to DATABASE_URL.",
    )
    parser.add_argument(
        "--db-statement-timeout-ms",
        type=int,
        default=120000,
        help="Statement timeout passed to build_engine. Defaults to 120000.",
    )
    return parser


def _load_database_url(explicit_value: str) -> str:
    if explicit_value:
        return explicit_value
    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("DATABASE_URL="):
                return line.split("=", 1)[1].strip()
    raise SystemExit("DATABASE_URL is required. Provide --database-url or configure .env.")


def main() -> int:
    args = build_parser().parse_args()
    database_url = _load_database_url(args.database_url)
    input_rows = list(csv.DictReader(args.input_csv.open(encoding="utf-8")))
    if args.end_index is None:
        selected_rows = input_rows[args.start_index :]
    else:
        selected_rows = input_rows[args.start_index : args.end_index]
    if not selected_rows:
        raise SystemExit("No rows selected from input CSV.")

    summary_by_symbol = {
        row["symbol"].strip().upper(): row
        for row in csv.DictReader(args.summary_csv.open(encoding="utf-8"))
    }
    symbols = [row[args.symbol_column].strip().upper() for row in selected_rows]
    missing = [symbol for symbol in symbols if symbol not in summary_by_symbol]
    if missing:
        raise SystemExit(f"Missing selected_method rows in summary CSV for: {', '.join(missing)}")

    engine = pwm.build_engine(
        database_url=database_url,
        statement_timeout_ms=args.db_statement_timeout_ms,
    )
    results: list[dict[str, object]] = []
    try:
        factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)
        for offset, symbol in enumerate(symbols, start=args.start_index + 1):
            started_at = time.time()
            method_name = summary_by_symbol[symbol]["selected_method"]
            method = pwm._METHOD_NAME_TO_CONFIG[method_name]
            with factory() as session:
                bars = pwm._load_bars(
                    session,
                    symbol=symbol,
                    start_date=args.start_date,
                    end_date=args.as_of_date,
                    warmup_calendar_days=pwm.DEFAULT_WARMUP_CALENDAR_DAYS,
                )
                option_feature_rows = pwm._load_option_feature_rows(
                    session,
                    symbol=symbol,
                    start_date=args.start_date,
                    end_date=args.as_of_date,
                    warmup_calendar_days=pwm.DEFAULT_WARMUP_CALENDAR_DAYS,
                )
                benchmark_bars = (
                    bars
                    if symbol == pwm.DEFAULT_BENCHMARK_SYMBOL
                    else pwm._load_bars(
                        session,
                        symbol=pwm.DEFAULT_BENCHMARK_SYMBOL,
                        start_date=args.start_date,
                        end_date=args.as_of_date,
                        warmup_calendar_days=pwm.DEFAULT_WARMUP_CALENDAR_DAYS,
                    )
                )
                earnings_dates = pwm._load_earnings_dates(
                    session,
                    symbol=symbol,
                    start_date=args.start_date,
                    end_date=args.as_of_date,
                )

            store = pwm.HistoricalMarketDataStore(factory, factory)
            option_gateway = pwm.HistoricalOptionGateway(store, symbol)
            benchmark_context_by_date = pwm._build_benchmark_context_by_date(benchmark_bars)
            front_iv_series = pwm.build_estimated_iv_series(
                bars,
                option_gateway,
                target_dte=pwm.DEFAULT_FRONT_IV_TARGET_DTE,
                dte_tolerance_days=pwm.DEFAULT_FRONT_IV_DTE_TOLERANCE_DAYS,
            )
            back_iv_series = pwm.build_estimated_iv_series(
                bars,
                option_gateway,
                target_dte=pwm.DEFAULT_BACK_IV_TARGET_DTE,
                dte_tolerance_days=pwm.DEFAULT_BACK_IV_DTE_TOLERANCE_DAYS,
            )
            option_context_by_date = pwm._build_option_context_by_date(
                bars,
                option_feature_rows,
                front_iv_series=front_iv_series,
            )
            iv_context_by_date = pwm._build_iv_context_by_date(
                bars,
                front_iv_series=front_iv_series,
                back_iv_series=back_iv_series,
            )
            features = pwm._build_feature_matrix(
                bars,
                benchmark_context_by_date=benchmark_context_by_date,
                earnings_dates=earnings_dates,
                option_context_by_date=option_context_by_date,
                iv_context_by_date=iv_context_by_date,
            )
            candidates = pwm._build_analog_candidates(bars=bars, features=features, horizon_bars=5)
            prediction = pwm._build_latest_prediction(
                bars=bars,
                features=features,
                candidates=candidates,
                horizon_bars=5,
                min_spacing_bars=5,
                min_candidate_count=70,
                method=method,
            )
            result = {
                "symbol": symbol,
                "as_of_date": args.as_of_date.isoformat(),
                "selected_method": method_name,
                "prediction_engine": method.engine,
                "model_name": method.ml_model_name or "",
                "predicted_direction": "" if prediction is None else prediction.get("predicted_direction", ""),
                "predicted_sign": "" if prediction is None else prediction.get("predicted_sign", ""),
                "confidence_pct": "" if prediction is None else prediction.get("confidence_pct", ""),
                "probability_up_pct": "" if prediction is None else prediction.get("probability_up_pct", ""),
                "probability_down_pct": "" if prediction is None else prediction.get("probability_down_pct", ""),
                "train_sample_count": "" if prediction is None else prediction.get("train_sample_count", ""),
                "status": "abstain" if prediction is None else "predicted",
                "elapsed_sec": round(time.time() - started_at, 2),
            }
            results.append(result)
            print(
                f"{offset:02d} {symbol} {method_name} -> {result['status']} "
                f"{result['predicted_direction']} conf={result['confidence_pct']} "
                f"elapsed={result['elapsed_sec']}"
            )
    finally:
        engine.dispose()

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.output_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)
    print(f"WROTE {args.output_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
