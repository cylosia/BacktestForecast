from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from sqlalchemy import delete, select

from backtestforecast.db.base import Base
from backtestforecast.db.session import build_engine, create_session
from backtestforecast.models import (
    AuditEvent,
    BacktestEquityPoint,
    BacktestRun,
    BacktestTrade,
    ExportJob,
    ScannerJob,
    ScannerRecommendation,
    SweepJob,
    SweepResult,
    User,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed deterministic dev data for BacktestForecast.com")
    parser.add_argument("--create-schema", action="store_true", help="Create database tables before seeding.")
    parser.add_argument(
        "--reset-user-data", action="store_true", help="Delete existing data for the target user before seeding."
    )
    parser.add_argument(
        "--yes", "-y", action="store_true", help="Skip confirmation prompts for destructive operations."
    )
    parser.add_argument("--clerk-user-id", default="dev_seed_user", help="Clerk user id to upsert.")
    parser.add_argument("--email", default="dev@backtestforecast.local", help="Email address for the seeded user.")
    parser.add_argument(
        "--plan-tier", default="premium", choices=["free", "pro", "premium"], help="Plan tier snapshot to apply."
    )
    return parser.parse_args()


def ensure_user(session, *, clerk_user_id: str, email: str, plan_tier: str) -> User:
    existing = session.scalar(select(User).where(User.clerk_user_id == clerk_user_id))
    if existing is not None:
        existing.email = email
        existing.plan_tier = plan_tier
        existing.subscription_status = "active" if plan_tier in {"pro", "premium"} else None
        existing.subscription_billing_interval = "monthly" if plan_tier in {"pro", "premium"} else None
        existing.plan_updated_at = datetime.now(UTC)
        session.add(existing)
        session.flush()
        return existing

    user = User(
        clerk_user_id=clerk_user_id,
        email=email,
        plan_tier=plan_tier,
        subscription_status="active" if plan_tier in {"pro", "premium"} else None,
        subscription_billing_interval="monthly" if plan_tier in {"pro", "premium"} else None,
        plan_updated_at=datetime.now(UTC),
    )
    session.add(user)
    session.flush()
    return user


def reset_user_data(session, user: User) -> None:
    session.execute(delete(AuditEvent).where(AuditEvent.user_id == user.id))
    session.execute(delete(ExportJob).where(ExportJob.user_id == user.id))
    session.execute(delete(SweepJob).where(SweepJob.user_id == user.id))
    session.execute(delete(ScannerJob).where(ScannerJob.user_id == user.id))
    session.execute(delete(BacktestRun).where(BacktestRun.user_id == user.id))
    session.flush()


def seed_backtests(session, user: User) -> list[BacktestRun]:
    base_date = date.today() - timedelta(days=120)
    symbols = ["AAPL", "MSFT", "NVDA"]
    strategies = ["long_call", "covered_call", "bull_call_debit_spread"]
    runs: list[BacktestRun] = []

    for index, (symbol, strategy_type) in enumerate(zip(symbols, strategies, strict=True), start=1):
        created_at = datetime.now(UTC) - timedelta(days=30 - (index * 4))
        run = BacktestRun(
            user_id=user.id,
            status="succeeded",
            symbol=symbol,
            strategy_type=strategy_type,
            date_from=base_date + timedelta(days=index * 5),
            date_to=base_date + timedelta(days=60 + (index * 5)),
            target_dte=30,
            dte_tolerance_days=5,
            max_holding_days=10,
            account_size=Decimal("10000"),
            risk_per_trade_pct=Decimal("5"),
            commission_per_contract=Decimal("1"),
            input_snapshot_json={
                "symbol": symbol,
                "strategy_type": strategy_type,
                "entry_rules": [{"type": "rsi", "operator": "lte", "threshold": "40", "period": 14}],
            },
            warnings_json=[],
            engine_version="options-multileg-v2",
            data_source="massive",
            trade_count=2,
            win_rate=Decimal("50"),
            total_roi_pct=Decimal(str(6 + index)),
            average_win_amount=Decimal("275"),
            average_loss_amount=Decimal("-120"),
            average_holding_period_days=Decimal("6"),
            average_dte_at_open=Decimal("30"),
            max_drawdown_pct=Decimal("4.2"),
            total_commissions=Decimal("4"),
            total_net_pnl=Decimal(str(350 + (index * 50))),
            starting_equity=Decimal("10000"),
            ending_equity=Decimal(str(10350 + (index * 50))),
            created_at=created_at,
            completed_at=created_at + timedelta(minutes=2),
        )
        session.add(run)
        session.flush()

        for trade_index in range(2):
            entry_date = run.date_from + timedelta(days=(trade_index * 8) + 7)
            exit_date = entry_date + timedelta(days=6)
            session.add(
                BacktestTrade(
                    run_id=run.id,
                    option_ticker=f"{symbol}2406{trade_index + 1:02d}C00100000",
                    strategy_type=strategy_type,
                    underlying_symbol=symbol,
                    entry_date=entry_date,
                    exit_date=exit_date,
                    expiration_date=exit_date + timedelta(days=21),
                    quantity=1,
                    dte_at_open=30,
                    holding_period_days=6,
                    entry_underlying_close=Decimal("100"),
                    exit_underlying_close=Decimal("104"),
                    entry_mid=Decimal("2.10"),
                    exit_mid=Decimal("3.40"),
                    gross_pnl=Decimal("130"),
                    net_pnl=Decimal("128"),
                    total_commissions=Decimal("2"),
                    entry_reason="seed_entry",
                    exit_reason="seed_exit",
                    detail_json={"seeded": True, "trade_index": trade_index + 1},
                )
            )
        for point_index in range(3):
            trade_date = run.date_from + timedelta(days=(point_index * 6) + 7)
            session.add(
                BacktestEquityPoint(
                    run_id=run.id,
                    trade_date=trade_date,
                    equity=Decimal(str(10000 + (index * 100) + (point_index * 25))),
                    cash=Decimal(str(9800 + (index * 100) + (point_index * 25))),
                    position_value=Decimal("200"),
                    drawdown_pct=Decimal("1.5"),
                )
            )
        runs.append(run)
    session.flush()
    return runs


def seed_scanner(session, user: User, runs: list[BacktestRun]) -> ScannerJob:
    created_at = datetime.now(UTC) - timedelta(days=1)
    job = ScannerJob(
        user_id=user.id,
        name="Seeded premium scan",
        status="succeeded",
        mode="advanced",
        plan_tier_snapshot=user.plan_tier,
        job_kind="manual",
        request_hash="seeded-request-hash",
        refresh_daily=True,
        refresh_priority=80,
        candidate_count=6,
        evaluated_candidate_count=6,
        recommendation_count=2,
        request_snapshot_json={
            "symbols": ["AAPL", "MSFT", "NVDA"],
            "strategy_types": ["long_call", "covered_call"],
            "rule_sets": [{"name": "RSI pullback"}],
        },
        warnings_json=[],
        ranking_version="scanner-ranking-v1",
        engine_version="options-multileg-v2",
        created_at=created_at,
        started_at=created_at + timedelta(minutes=1),
        completed_at=created_at + timedelta(minutes=3),
    )
    session.add(job)
    session.flush()

    for rank, run in enumerate(runs[:2], start=1):
        session.add(
            ScannerRecommendation(
                scanner_job_id=job.id,
                rank=rank,
                score=Decimal(str(90 - (rank * 5))),
                symbol=run.symbol,
                strategy_type=run.strategy_type,
                rule_set_name="RSI pullback",
                rule_set_hash=f"seeded-rule-hash-{rank}",
                request_snapshot_json=run.input_snapshot_json,
                summary_json={
                    "trade_count": run.trade_count,
                    "win_rate": float(run.win_rate),
                    "total_roi_pct": float(run.total_roi_pct),
                    "average_win_amount": float(run.average_win_amount),
                    "average_loss_amount": float(run.average_loss_amount),
                    "average_holding_period_days": float(run.average_holding_period_days),
                    "average_dte_at_open": float(run.average_dte_at_open),
                    "max_drawdown_pct": float(run.max_drawdown_pct),
                    "total_commissions": float(run.total_commissions),
                    "total_net_pnl": float(run.total_net_pnl),
                    "starting_equity": float(run.starting_equity),
                    "ending_equity": float(run.ending_equity),
                },
                warnings_json=[],
                trades_json=[
                    {"option_ticker": trade.option_ticker, "net_pnl": float(trade.net_pnl)} for trade in run.trades
                ],
                equity_curve_json=[
                    {"trade_date": point.trade_date.isoformat(), "equity": float(point.equity)}
                    for point in run.equity_points
                ],
                historical_performance_json={
                    "sample_count": 8,
                    "weighted_win_rate": 58.0,
                    "weighted_total_roi_pct": 7.2,
                    "weighted_total_net_pnl": 420.0,
                    "weighted_max_drawdown_pct": 5.1,
                    "recency_half_life_days": 180,
                },
                forecast_json={
                    "symbol": run.symbol,
                    "strategy_type": run.strategy_type,
                    "as_of_date": run.date_to.isoformat(),
                    "horizon_days": 20,
                    "analog_count": 12,
                    "expected_return_low_pct": -3.0,
                    "expected_return_median_pct": 4.2,
                    "expected_return_high_pct": 9.4,
                    "positive_outcome_rate_pct": 61.0,
                    "summary": "Seeded analog range.",
                    "disclaimer": "Historical analog range only.",
                    "analog_dates": [run.date_from.isoformat()],
                },
                ranking_features_json={
                    "current_performance_score": 0.61,
                    "historical_performance_score": 0.22,
                    "forecast_alignment_score": 0.11,
                    "final_score": 0.94,
                    "reasoning": ["Seeded dev data"],
                },
                created_at=created_at + timedelta(minutes=3),
            )
        )
    session.flush()
    return job


def seed_sweep(session, user: User) -> SweepJob:
    created_at = datetime.now(UTC) - timedelta(days=2)
    job = SweepJob(
        user_id=user.id,
        symbol="AAPL",
        status="succeeded",
        candidate_count=8,
        evaluated_candidate_count=8,
        result_count=2,
        request_snapshot_json={
            "symbol": "AAPL",
            "strategy_types": ["long_call", "covered_call", "bull_call_debit_spread"],
            "parameter_ranges": {"target_dte": [20, 30, 45], "risk_per_trade_pct": [2, 5]},
        },
        warnings_json=[],
        engine_version="options-multileg-v2",
        created_at=created_at,
        started_at=created_at + timedelta(minutes=1),
        completed_at=created_at + timedelta(minutes=5),
    )
    session.add(job)
    session.flush()

    for rank in range(1, 3):
        session.add(
            SweepResult(
                sweep_job_id=job.id,
                rank=rank,
                score=Decimal(str(95 - (rank * 8))),
                strategy_type="long_call" if rank == 1 else "covered_call",
                parameter_snapshot_json={
                    "target_dte": 30,
                    "risk_per_trade_pct": 5,
                    "max_holding_days": 10,
                },
                summary_json={
                    "trade_count": 6,
                    "win_rate": 58.0,
                    "total_roi_pct": 8.3,
                    "max_drawdown_pct": 3.9,
                    "total_net_pnl": 830.0,
                },
                warnings_json=[],
                trades_json=[],
                equity_curve_json=[],
                created_at=created_at + timedelta(minutes=5),
            )
        )
    session.flush()
    return job


def seed_export_job(session, user: User, run: BacktestRun) -> ExportJob:
    export = ExportJob(
        user_id=user.id,
        backtest_run_id=run.id,
        export_format="csv",
        status="succeeded",
        file_name=f"{run.symbol.lower()}-{run.strategy_type}-seed.csv",
        mime_type="text/csv; charset=utf-8",
        size_bytes=128,
        sha256_hex="0" * 64,
        content_bytes=b"section,field,value\nrun,symbol,AAPL\n",
        created_at=datetime.now(UTC) - timedelta(hours=2),
        completed_at=datetime.now(UTC) - timedelta(hours=2) + timedelta(seconds=3),
    )
    session.add(export)
    session.flush()
    return export


def main() -> None:
    import os

    app_env = os.environ.get("APP_ENV", "").lower()
    if app_env in ("production", "prod", "staging"):
        print("ERROR: seed_dev_data.py must not be run against a production or staging database.")
        print(f"       APP_ENV is set to {app_env!r}. Aborting.")
        raise SystemExit(1)
    db_url = os.environ.get("DATABASE_URL", "")
    if db_url and "sslmode=require" in db_url:
        print("ERROR: seed_dev_data.py detected a production-style DATABASE_URL (sslmode=require).")
        print("       Refusing to seed. Aborting.")
        raise SystemExit(1)
    db_url_lower = db_url.lower()
    if any(host in db_url_lower for host in ("rds.amazonaws.com", "cloud.google.com", "azure.com", ".prod.")):
        print("ERROR: DATABASE_URL appears to point to a production database.")
        raise SystemExit(1)

    args = parse_args()
    if args.create_schema:
        Base.metadata.create_all(bind=build_engine())

    with create_session() as session:
        user = ensure_user(
            session,
            clerk_user_id=args.clerk_user_id,
            email=args.email,
            plan_tier=args.plan_tier,
        )
        if args.reset_user_data:
            if not args.yes:
                confirm = input(f"This will DELETE all data for user '{args.clerk_user_id}'. Continue? [y/N] ")
                if confirm.strip().lower() not in ("y", "yes"):
                    print("Aborted.")
                    raise SystemExit(0)
            reset_user_data(session, user)
            session.commit()
            session.refresh(user)

        runs = seed_backtests(session, user)
        scanner_job = seed_scanner(session, user, runs)
        sweep_job = seed_sweep(session, user)
        export_job = seed_export_job(session, user, runs[0])
        session.commit()

        print(
            f"Seeded user {user.clerk_user_id} ({user.plan_tier}) with "
            f"{len(runs)} backtests, 1 scanner job, 1 sweep job, and export job {export_job.id}."
        )
        print(f"Latest scanner job: {scanner_job.id}")
        print(f"Latest sweep job: {sweep_job.id}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Error: {exc}")
        raise SystemExit(1) from exc
