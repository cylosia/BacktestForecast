from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from backtestforecast.db.base import Base
from backtestforecast.db.types import GUID, JSON_VARIANT


class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    clerk_user_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    email: Mapped[str | None] = mapped_column(String(320), nullable=True)
    plan_tier: Mapped[str] = mapped_column(String(16), nullable=False, default="free", server_default="free")
    stripe_customer_id: Mapped[str | None] = mapped_column(String(64), nullable=True, unique=True)
    stripe_subscription_id: Mapped[str | None] = mapped_column(String(64), nullable=True, unique=True)
    stripe_price_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    subscription_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    subscription_billing_interval: Mapped[str | None] = mapped_column(String(16), nullable=True)
    subscription_current_period_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cancel_at_period_end: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    plan_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    backtest_runs: Mapped[list["BacktestRun"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    scanner_jobs: Mapped[list["ScannerJob"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    export_jobs: Mapped[list["ExportJob"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    templates: Mapped[list["BacktestTemplate"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    audit_events: Mapped[list["AuditEvent"]] = relationship(back_populates="user")


class BacktestRun(Base):
    __tablename__ = "backtest_runs"
    __table_args__ = (
        Index("ix_backtest_runs_user_created_at", "user_id", "created_at"),
        Index("ix_backtest_runs_user_status", "user_id", "status"),
        Index("ix_backtest_runs_celery_task_id", "celery_task_id"),
        UniqueConstraint("user_id", "idempotency_key", name="uq_backtest_runs_user_idempotency_key"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued")
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    strategy_type: Mapped[str] = mapped_column(String(32), nullable=False)
    date_from: Mapped[date] = mapped_column(Date, nullable=False)
    date_to: Mapped[date] = mapped_column(Date, nullable=False)
    target_dte: Mapped[int] = mapped_column(Integer, nullable=False)
    dte_tolerance_days: Mapped[int] = mapped_column(Integer, nullable=False)
    max_holding_days: Mapped[int] = mapped_column(Integer, nullable=False)
    account_size: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    risk_per_trade_pct: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    commission_per_contract: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    input_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False)
    warnings_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON_VARIANT, nullable=False, default=list)
    engine_version: Mapped[str] = mapped_column(String(32), nullable=False, default="long-option-v1")
    data_source: Mapped[str] = mapped_column(String(32), nullable=False, default="massive")
    idempotency_key: Mapped[str | None] = mapped_column(String(80), nullable=True)
    celery_task_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    trade_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    win_rate: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"))
    total_roi_pct: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"))
    average_win_amount: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"))
    average_loss_amount: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"))
    average_holding_period_days: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"))
    average_dte_at_open: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"))
    max_drawdown_pct: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"))
    total_commissions: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"))
    total_net_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"))
    starting_equity: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"))
    ending_equity: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    user: Mapped["User"] = relationship(back_populates="backtest_runs")
    trades: Mapped[list["BacktestTrade"]] = relationship(
        back_populates="run", cascade="all, delete-orphan", order_by="BacktestTrade.entry_date"
    )
    equity_points: Mapped[list["BacktestEquityPoint"]] = relationship(
        back_populates="run",
        cascade="all, delete-orphan",
        order_by="BacktestEquityPoint.trade_date",
    )
    exports: Mapped[list["ExportJob"]] = relationship(back_populates="backtest_run")


class BacktestTrade(Base):
    __tablename__ = "backtest_trades"
    __table_args__ = (Index("ix_backtest_trades_run_entry_date", "run_id", "entry_date"),)

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(
        GUID(), ForeignKey("backtest_runs.id", ondelete="CASCADE"), nullable=False
    )
    option_ticker: Mapped[str] = mapped_column(String(64), nullable=False)
    strategy_type: Mapped[str] = mapped_column(String(32), nullable=False)
    underlying_symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    entry_date: Mapped[date] = mapped_column(Date, nullable=False)
    exit_date: Mapped[date] = mapped_column(Date, nullable=False)
    expiration_date: Mapped[date] = mapped_column(Date, nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    dte_at_open: Mapped[int] = mapped_column(Integer, nullable=False)
    holding_period_days: Mapped[int] = mapped_column(Integer, nullable=False)
    entry_underlying_close: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    exit_underlying_close: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    entry_mid: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    exit_mid: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    gross_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    net_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    total_commissions: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    entry_reason: Mapped[str] = mapped_column(String(128), nullable=False)
    exit_reason: Mapped[str] = mapped_column(String(128), nullable=False)
    detail_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict)

    run: Mapped["BacktestRun"] = relationship(back_populates="trades")


class BacktestEquityPoint(Base):
    __tablename__ = "backtest_equity_points"
    __table_args__ = (
        UniqueConstraint("run_id", "trade_date", name="uq_backtest_equity_points_run_date"),
        Index("ix_backtest_equity_points_run_date", "run_id", "trade_date"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(
        GUID(), ForeignKey("backtest_runs.id", ondelete="CASCADE"), nullable=False
    )
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    equity: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    cash: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    position_value: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    drawdown_pct: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)

    run: Mapped["BacktestRun"] = relationship(back_populates="equity_points")


class BacktestTemplate(Base):
    __tablename__ = "backtest_templates"
    __table_args__ = (
        Index("ix_backtest_templates_user_created_at", "user_id", "created_at"),
        Index("ix_backtest_templates_user_strategy", "user_id", "strategy_type"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    strategy_type: Mapped[str] = mapped_column(String(32), nullable=False)
    config_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    user: Mapped["User"] = relationship(back_populates="templates")


class ScannerJob(Base):
    __tablename__ = "scanner_jobs"
    __table_args__ = (
        UniqueConstraint("user_id", "idempotency_key", name="uq_scanner_jobs_user_idempotency_key"),
        UniqueConstraint("refresh_key", name="uq_scanner_jobs_refresh_key"),
        Index("ix_scanner_jobs_user_created_at", "user_id", "created_at"),
        Index("ix_scanner_jobs_user_status", "user_id", "status"),
        Index("ix_scanner_jobs_request_hash", "request_hash"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    parent_job_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID(), ForeignKey("scanner_jobs.id", ondelete="SET NULL"), nullable=True
    )
    name: Mapped[str | None] = mapped_column(String(120), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued")
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    plan_tier_snapshot: Mapped[str] = mapped_column(String(16), nullable=False)
    job_kind: Mapped[str] = mapped_column(String(32), nullable=False, default="manual")
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    idempotency_key: Mapped[str | None] = mapped_column(String(80), nullable=True)
    refresh_key: Mapped[str | None] = mapped_column(String(120), nullable=True)
    refresh_daily: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    refresh_priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    candidate_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    evaluated_candidate_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    recommendation_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    request_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False)
    warnings_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON_VARIANT, nullable=False, default=list)
    ranking_version: Mapped[str] = mapped_column(String(32), nullable=False, default="scanner-ranking-v1")
    engine_version: Mapped[str] = mapped_column(String(32), nullable=False, default="options-multileg-v2")
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    user: Mapped["User"] = relationship(back_populates="scanner_jobs")
    parent_job: Mapped["ScannerJob | None"] = relationship(remote_side=[id], backref="child_jobs")
    recommendations: Mapped[list["ScannerRecommendation"]] = relationship(
        back_populates="job",
        cascade="all, delete-orphan",
        order_by="ScannerRecommendation.rank",
    )


class ScannerRecommendation(Base):
    __tablename__ = "scanner_recommendations"
    __table_args__ = (
        UniqueConstraint("scanner_job_id", "rank", name="uq_scanner_recommendations_job_rank"),
        Index("ix_scanner_recommendations_job_rank", "scanner_job_id", "rank"),
        Index("ix_scanner_recommendations_lookup", "symbol", "strategy_type", "rule_set_hash"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    scanner_job_id: Mapped[uuid.UUID] = mapped_column(
        GUID(), ForeignKey("scanner_jobs.id", ondelete="CASCADE"), nullable=False
    )
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    score: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    strategy_type: Mapped[str] = mapped_column(String(32), nullable=False)
    rule_set_name: Mapped[str] = mapped_column(String(120), nullable=False)
    rule_set_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    request_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False)
    summary_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False)
    warnings_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON_VARIANT, nullable=False, default=list)
    trades_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON_VARIANT, nullable=False, default=list)
    equity_curve_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON_VARIANT, nullable=False, default=list)
    historical_performance_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict)
    forecast_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict)
    ranking_features_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    job: Mapped["ScannerJob"] = relationship(back_populates="recommendations")


class ExportJob(Base):
    __tablename__ = "export_jobs"
    __table_args__ = (
        UniqueConstraint("user_id", "idempotency_key", name="uq_export_jobs_user_idempotency_key"),
        Index("ix_export_jobs_user_created_at", "user_id", "created_at"),
        Index("ix_export_jobs_user_status", "user_id", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    backtest_run_id: Mapped[uuid.UUID] = mapped_column(
        GUID(), ForeignKey("backtest_runs.id", ondelete="CASCADE"), nullable=False
    )
    export_format: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued")
    file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    mime_type: Mapped[str] = mapped_column(String(128), nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    sha256_hex: Mapped[str | None] = mapped_column(String(64), nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(80), nullable=True)
    content_bytes: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    user: Mapped["User"] = relationship(back_populates="export_jobs")
    backtest_run: Mapped["BacktestRun"] = relationship(back_populates="exports")


class AuditEvent(Base):
    __tablename__ = "audit_events"
    __table_args__ = (
        Index("ix_audit_events_user_created_at", "user_id", "created_at"),
        Index("ix_audit_events_event_type_created_at", "event_type", "created_at"),
        UniqueConstraint("event_type", "subject_type", "subject_id", name="uq_audit_events_dedup"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID(), ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    request_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    subject_type: Mapped[str] = mapped_column(String(64), nullable=False)
    subject_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    ip_hash: Mapped[str | None] = mapped_column(String(128), nullable=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    user: Mapped["User"] = relationship(back_populates="audit_events")


class NightlyPipelineRun(Base):
    __tablename__ = "nightly_pipeline_runs"
    __table_args__ = (
        Index("ix_nightly_pipeline_runs_trade_date", "trade_date"),
        Index("ix_nightly_pipeline_runs_status", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="running")
    stage: Mapped[str] = mapped_column(String(32), nullable=False, default="universe_screen")
    symbols_screened: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    symbols_after_screen: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    pairs_generated: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    quick_backtests_run: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    full_backtests_run: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    recommendations_produced: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    duration_seconds: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    stage_details_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    recommendations: Mapped[list["DailyRecommendation"]] = relationship(
        back_populates="pipeline_run", cascade="all, delete-orphan"
    )


class DailyRecommendation(Base):
    __tablename__ = "daily_recommendations"
    __table_args__ = (
        Index("ix_daily_recs_pipeline_rank", "pipeline_run_id", "rank"),
        Index("ix_daily_recs_trade_date", "trade_date"),
        Index("ix_daily_recs_symbol_strategy", "symbol", "strategy_type"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    pipeline_run_id: Mapped[uuid.UUID] = mapped_column(
        GUID(), ForeignKey("nightly_pipeline_runs.id", ondelete="CASCADE"), nullable=False
    )
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    score: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    strategy_type: Mapped[str] = mapped_column(String(64), nullable=False)
    regime_labels: Mapped[str] = mapped_column(String(255), nullable=False)
    close_price: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    target_dte: Mapped[int] = mapped_column(Integer, nullable=False)
    config_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict)
    summary_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict)
    forecast_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    pipeline_run: Mapped["NightlyPipelineRun"] = relationship(back_populates="recommendations")


class SymbolAnalysis(Base):
    __tablename__ = "symbol_analyses"
    __table_args__ = (
        Index("ix_symbol_analyses_user_created", "user_id", "created_at"),
        Index("ix_symbol_analyses_symbol", "symbol"),
        UniqueConstraint("user_id", "idempotency_key", name="uq_symbol_analyses_user_idempotency"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued")
    stage: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    close_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    regime_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict)
    landscape_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON_VARIANT, nullable=False, default=list)
    top_results_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON_VARIANT, nullable=False, default=list)
    forecast_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict)
    strategies_tested: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    configs_tested: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    top_results_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    duration_seconds: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(80), nullable=True)
    celery_task_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    user: Mapped["User"] = relationship()
