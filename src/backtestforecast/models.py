from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import structlog
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
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
    desc,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship, validates
from sqlalchemy.sql import func

# All relationships use lazy="raise" to prevent implicit lazy loading, which
# causes N+1 query performance issues. Access related objects via explicit
# eager loading (selectinload, joinedload) or separate queries. If you see
# a "lazy load operation" error, add the appropriate loading strategy to
# your query rather than changing the relationship to lazy="select".
from backtestforecast.db.base import Base
from backtestforecast.db.types import (
    GUID,
    JSON_DEFAULT_EMPTY_ARRAY,
    JSON_DEFAULT_EMPTY_OBJECT,
    JSON_VARIANT,
)


class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        Index("ix_users_email", "email"),
        CheckConstraint(
            "plan_tier IN ('free', 'pro', 'premium')",
            name="ck_users_valid_plan_tier",
        ),
        CheckConstraint(
            "subscription_status IS NULL OR subscription_status IN ('incomplete', 'incomplete_expired', 'trialing', 'active', 'past_due', 'canceled', 'unpaid', 'paused')",
            name="ck_users_valid_subscription_status",
        ),
        CheckConstraint(
            "subscription_billing_interval IS NULL OR subscription_billing_interval IN ('monthly', 'yearly')",
            name="ck_users_valid_billing_interval",
        ),
        CheckConstraint(
            "email IS NULL OR length(email) > 0",
            name="ck_users_email_not_empty",
        ),
    )

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
    cancel_at_period_end: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    plan_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    # NOTE: onupdate=func.now() is ORM-only and redundant with the DB-level
    # trigger (trg_users_updated_at, migration 20260318_0014). Kept for
    # compatibility with SQLite test sessions where the trigger doesn't exist.
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    backtest_runs: Mapped[list[BacktestRun]] = relationship(back_populates="user", cascade="all, delete-orphan", lazy="raise")
    multi_symbol_runs: Mapped[list[MultiSymbolRun]] = relationship(back_populates="user", cascade="all, delete-orphan", lazy="raise")
    multi_step_runs: Mapped[list[MultiStepRun]] = relationship(back_populates="user", cascade="all, delete-orphan", lazy="raise")
    scanner_jobs: Mapped[list[ScannerJob]] = relationship(back_populates="user", cascade="all, delete-orphan", lazy="raise")
    export_jobs: Mapped[list[ExportJob]] = relationship(back_populates="user", cascade="all, delete-orphan", lazy="raise")
    templates: Mapped[list[BacktestTemplate]] = relationship(back_populates="user", cascade="all, delete-orphan", lazy="raise")
    audit_events: Mapped[list[AuditEvent]] = relationship(back_populates="user", passive_deletes=True, lazy="raise")
    symbol_analyses: Mapped[list[SymbolAnalysis]] = relationship(back_populates="user", cascade="all, delete-orphan", lazy="raise")
    sweep_jobs: Mapped[list[SweepJob]] = relationship(back_populates="user", cascade="all, delete-orphan", lazy="raise")
    stripe_events: Mapped[list[StripeEvent]] = relationship(back_populates="user", foreign_keys="StripeEvent.user_id", passive_deletes=True, lazy="raise")


class BacktestRun(Base):
    __tablename__ = "backtest_runs"
    __table_args__ = (
        Index("ix_backtest_runs_user_id", "user_id"),
        Index("ix_backtest_runs_user_created_at", "user_id", "created_at"),
        Index("ix_backtest_runs_user_status", "user_id", "status"),
        Index("ix_backtest_runs_user_symbol", "user_id", "symbol"),
        Index("ix_backtest_runs_started_at", "started_at"),
        Index("ix_backtest_runs_dispatch_started_at", "dispatch_started_at"),
        Index("ix_backtest_runs_celery_task_id", "celery_task_id"),
        Index("ix_backtest_runs_status_celery_created", "status", "celery_task_id", "created_at"),
        UniqueConstraint("user_id", "idempotency_key", name="uq_backtest_runs_user_idempotency_key"),
        CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
            name="ck_backtest_runs_valid_run_status",
        ),
        CheckConstraint("account_size > 0", name="ck_backtest_runs_account_positive"),
        CheckConstraint("risk_per_trade_pct > 0 AND risk_per_trade_pct <= 100", name="ck_backtest_runs_risk_pct_range"),
        CheckConstraint("commission_per_contract >= 0", name="ck_backtest_runs_commission_nonneg"),
        CheckConstraint("date_from < date_to", name="ck_backtest_runs_date_order"),
        CheckConstraint("max_holding_days >= 1", name="ck_backtest_runs_holding_days_positive"),
        CheckConstraint("target_dte >= 0", name="ck_backtest_runs_target_dte_nonneg"),
        CheckConstraint("dte_tolerance_days >= 0", name="ck_backtest_runs_dte_tolerance_nonneg"),
        CheckConstraint("max_holding_days >= 1 AND max_holding_days <= 120", name="ck_backtest_runs_holding_days_range"),
        CheckConstraint("target_dte >= 1 AND target_dte <= 365", name="ck_backtest_runs_target_dte_range"),
        CheckConstraint("dte_tolerance_days >= 0 AND dte_tolerance_days <= 60", name="ck_backtest_runs_dte_tolerance_range"),
        CheckConstraint("account_size <= 100000000", name="ck_backtest_runs_account_size_max"),
        CheckConstraint("commission_per_contract <= 100", name="ck_backtest_runs_commission_max"),
        Index("ix_backtest_runs_queued", "created_at", postgresql_where=text("status = 'queued'")),
        CheckConstraint(
            "engine_version IN ('options-multileg-v1', 'options-multileg-v2')",
            name="ck_backtest_runs_valid_engine_version",
        ),
        CheckConstraint(
            "data_source IN ('massive', 'manual', 'historical_flatfile')",
            name="ck_backtest_runs_valid_data_source",
        ),
        CheckConstraint("length(symbol) > 0", name="ck_backtest_runs_symbol_not_empty"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued", server_default="queued")
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    strategy_type: Mapped[str] = mapped_column(String(48), nullable=False)
    date_from: Mapped[date] = mapped_column(Date, nullable=False)
    date_to: Mapped[date] = mapped_column(Date, nullable=False)
    target_dte: Mapped[int] = mapped_column(Integer, nullable=False)
    dte_tolerance_days: Mapped[int] = mapped_column(Integer, nullable=False)
    max_holding_days: Mapped[int] = mapped_column(Integer, nullable=False)
    account_size: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    risk_per_trade_pct: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    commission_per_contract: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    input_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict, server_default=JSON_DEFAULT_EMPTY_OBJECT)
    warnings_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON_VARIANT, nullable=False, default=list, server_default=JSON_DEFAULT_EMPTY_ARRAY)
    engine_version: Mapped[str] = mapped_column(String(32), nullable=False, default="options-multileg-v2", server_default="options-multileg-v2")
    data_source: Mapped[str] = mapped_column(String(32), nullable=False, default="massive", server_default="massive")
    idempotency_key: Mapped[str | None] = mapped_column(String(80), nullable=True)
    celery_task_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    trade_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    win_rate: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"), server_default="0")
    total_roi_pct: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"), server_default="0")
    average_win_amount: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    average_loss_amount: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    average_holding_period_days: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"), server_default="0")
    average_dte_at_open: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"), server_default="0")
    max_drawdown_pct: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"), server_default="0")
    total_commissions: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    total_net_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    starting_equity: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    ending_equity: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    profit_factor: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    payoff_ratio: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    expectancy: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    sharpe_ratio: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    sortino_ratio: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    cagr_pct: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    calmar_ratio: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    max_consecutive_wins: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    max_consecutive_losses: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    recovery_factor: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    risk_free_rate: Mapped[Decimal | None] = mapped_column(Numeric(6, 4), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    dispatch_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    user: Mapped[User] = relationship(back_populates="backtest_runs", lazy="raise")
    trades: Mapped[list[BacktestTrade]] = relationship(
        back_populates="run", cascade="all, delete-orphan", order_by="BacktestTrade.entry_date", lazy="raise"
    )
    equity_points: Mapped[list[BacktestEquityPoint]] = relationship(
        back_populates="run",
        cascade="all, delete-orphan",
        order_by="BacktestEquityPoint.trade_date",
        lazy="raise",
    )
    exports: Mapped[list[ExportJob]] = relationship(back_populates="backtest_run", passive_deletes=True, lazy="raise")


# NOTE: strategy_type columns use String(48) without a DB-level CHECK constraint.
# With 30+ strategy types (and growing for custom N-leg), maintaining a CHECK
# constraint would be fragile and migration-heavy.  Validation is enforced at
# the Pydantic schema layer (StrategyType enum) and the service layer instead.


class BacktestTrade(Base):
    __tablename__ = "backtest_trades"
    __table_args__ = (
        Index("ix_backtest_trades_run_entry_date", "run_id", "entry_date"),
        UniqueConstraint("run_id", "entry_date", "option_ticker", name="uq_backtest_trades_dedup"),
        CheckConstraint("quantity > 0", name="ck_backtest_trades_quantity_positive"),
        CheckConstraint("entry_date <= exit_date", name="ck_backtest_trades_date_order"),
        CheckConstraint("dte_at_open >= 0", name="ck_backtest_trades_dte_at_open_nonneg"),
        CheckConstraint("holding_period_days >= 0", name="ck_backtest_trades_holding_period_nonneg"),
        CheckConstraint("holding_period_trading_days IS NULL OR holding_period_trading_days >= 0", name="ck_backtest_trades_holding_trading_days_nonneg"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(
        GUID(), ForeignKey("backtest_runs.id", ondelete="CASCADE"), nullable=False
    )
    option_ticker: Mapped[str] = mapped_column(String(64), nullable=False)
    strategy_type: Mapped[str] = mapped_column(String(48), nullable=False)
    underlying_symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    entry_date: Mapped[date] = mapped_column(Date, nullable=False)
    exit_date: Mapped[date] = mapped_column(Date, nullable=False)
    expiration_date: Mapped[date] = mapped_column(Date, nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    dte_at_open: Mapped[int] = mapped_column(Integer, nullable=False)
    holding_period_days: Mapped[int] = mapped_column(Integer, nullable=False)
    holding_period_trading_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    entry_underlying_close: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    exit_underlying_close: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    entry_mid: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    exit_mid: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    gross_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    net_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    total_commissions: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    entry_reason: Mapped[str] = mapped_column(String(128), nullable=False)
    exit_reason: Mapped[str] = mapped_column(String(128), nullable=False)
    detail_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict, server_default=JSON_DEFAULT_EMPTY_OBJECT, deferred=True)

    run: Mapped[BacktestRun] = relationship(back_populates="trades", lazy="raise")


class BacktestEquityPoint(Base):
    __tablename__ = "backtest_equity_points"
    __table_args__ = (
        Index("ix_backtest_equity_points_trade_date", "trade_date"),
        UniqueConstraint("run_id", "trade_date", name="uq_backtest_equity_points_run_date"),
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

    run: Mapped[BacktestRun] = relationship(back_populates="equity_points", lazy="raise")


class MultiSymbolRun(Base):
    __tablename__ = "multi_symbol_runs"
    __table_args__ = (
        Index("ix_multi_symbol_runs_user_id", "user_id"),
        Index("ix_multi_symbol_runs_user_created_at", "user_id", "created_at"),
        Index("ix_multi_symbol_runs_status", "status"),
        Index("ix_multi_symbol_runs_celery_task_id", "celery_task_id"),
        Index("ix_multi_symbol_runs_status_celery_created", "status", "celery_task_id", "created_at"),
        Index("ix_multi_symbol_runs_dispatch_started_at", "dispatch_started_at"),
        Index("ix_multi_symbol_runs_queued", "created_at", postgresql_where=text("status = 'queued'")),
        UniqueConstraint("user_id", "idempotency_key", name="uq_multi_symbol_runs_user_idempotency_key"),
        CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
            name="ck_multi_symbol_runs_valid_run_status",
        ),
        CheckConstraint("start_date < end_date", name="ck_multi_symbol_runs_date_order"),
        CheckConstraint("account_size > 0", name="ck_multi_symbol_runs_account_positive"),
        CheckConstraint("commission_per_contract >= 0", name="ck_multi_symbol_runs_commission_nonneg"),
        CheckConstraint("slippage_pct >= 0 AND slippage_pct <= 5", name="ck_multi_symbol_runs_slippage_range"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued", server_default="queued")
    name: Mapped[str | None] = mapped_column(String(120), nullable=True)
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[date] = mapped_column(Date, nullable=False)
    account_size: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    capital_allocation_mode: Mapped[str] = mapped_column(String(24), nullable=False, default="equal_weight", server_default="equal_weight")
    commission_per_contract: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    slippage_pct: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"), server_default="0")
    input_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict, server_default=JSON_DEFAULT_EMPTY_OBJECT)
    warnings_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON_VARIANT, nullable=False, default=list, server_default=JSON_DEFAULT_EMPTY_ARRAY)
    idempotency_key: Mapped[str | None] = mapped_column(String(80), nullable=True)
    celery_task_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    trade_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    win_rate: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"), server_default="0")
    total_roi_pct: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"), server_default="0")
    average_win_amount: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    average_loss_amount: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    average_holding_period_days: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"), server_default="0")
    average_dte_at_open: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"), server_default="0")
    max_drawdown_pct: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"), server_default="0")
    total_commissions: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    total_net_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    starting_equity: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    ending_equity: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    profit_factor: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    payoff_ratio: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    expectancy: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    sharpe_ratio: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    sortino_ratio: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    cagr_pct: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    calmar_ratio: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    max_consecutive_wins: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    max_consecutive_losses: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    recovery_factor: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    dispatch_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    user: Mapped[User] = relationship(back_populates="multi_symbol_runs", lazy="raise")
    symbols: Mapped[list[MultiSymbolRunSymbol]] = relationship(back_populates="run", cascade="all, delete-orphan", lazy="raise")
    trade_groups: Mapped[list[MultiSymbolTradeGroup]] = relationship(back_populates="run", cascade="all, delete-orphan", lazy="raise")
    trades: Mapped[list[MultiSymbolTrade]] = relationship(back_populates="run", cascade="all, delete-orphan", lazy="raise")
    equity_points: Mapped[list[MultiSymbolEquityPoint]] = relationship(back_populates="run", cascade="all, delete-orphan", lazy="raise")


class MultiSymbolRunSymbol(Base):
    __tablename__ = "multi_symbol_run_symbols"
    __table_args__ = (
        UniqueConstraint("run_id", "symbol", name="uq_multi_symbol_run_symbols_run_symbol"),
        CheckConstraint("risk_per_trade_pct > 0 AND risk_per_trade_pct <= 100", name="ck_multi_symbol_run_symbols_risk_pct_range"),
        CheckConstraint("max_open_positions >= 1", name="ck_multi_symbol_run_symbols_max_open_positions_positive"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("multi_symbol_runs.id", ondelete="CASCADE"), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    risk_per_trade_pct: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    max_open_positions: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    capital_allocation_pct: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    trade_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    win_rate: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"), server_default="0")
    total_roi_pct: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"), server_default="0")
    max_drawdown_pct: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"), server_default="0")
    total_commissions: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    total_net_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    starting_equity: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    ending_equity: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")

    run: Mapped[MultiSymbolRun] = relationship(back_populates="symbols", lazy="raise")
    equity_points: Mapped[list[MultiSymbolSymbolEquityPoint]] = relationship(back_populates="run_symbol", cascade="all, delete-orphan", lazy="raise")


class MultiSymbolTradeGroup(Base):
    __tablename__ = "multi_symbol_trade_groups"
    __table_args__ = (
        Index("ix_multi_symbol_trade_groups_run_entry_date", "run_id", "entry_date"),
        CheckConstraint("status IN ('open', 'closed', 'cancelled')", name="ck_multi_symbol_trade_groups_status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("multi_symbol_runs.id", ondelete="CASCADE"), nullable=False)
    entry_date: Mapped[date] = mapped_column(Date, nullable=False)
    exit_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="open", server_default="open")
    detail_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict, server_default=JSON_DEFAULT_EMPTY_OBJECT)

    run: Mapped[MultiSymbolRun] = relationship(back_populates="trade_groups", lazy="raise")
    trades: Mapped[list[MultiSymbolTrade]] = relationship(back_populates="trade_group", cascade="all, delete-orphan", lazy="raise")


class MultiSymbolTrade(Base):
    __tablename__ = "multi_symbol_trades"
    __table_args__ = (
        Index("ix_multi_symbol_trades_run_entry_date", "run_id", "entry_date"),
        Index("ix_multi_symbol_trades_trade_group_id", "trade_group_id"),
        CheckConstraint("quantity > 0", name="ck_multi_symbol_trades_quantity_positive"),
        CheckConstraint("entry_date <= exit_date", name="ck_multi_symbol_trades_date_order"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("multi_symbol_runs.id", ondelete="CASCADE"), nullable=False)
    trade_group_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("multi_symbol_trade_groups.id", ondelete="CASCADE"), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    option_ticker: Mapped[str] = mapped_column(String(64), nullable=False)
    strategy_type: Mapped[str] = mapped_column(String(48), nullable=False)
    entry_date: Mapped[date] = mapped_column(Date, nullable=False)
    exit_date: Mapped[date] = mapped_column(Date, nullable=False)
    expiration_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    dte_at_open: Mapped[int | None] = mapped_column(Integer, nullable=True)
    holding_period_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    entry_underlying_close: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    exit_underlying_close: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    entry_mid: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    exit_mid: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    gross_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    net_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    total_commissions: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    entry_reason: Mapped[str] = mapped_column(String(128), nullable=False)
    exit_reason: Mapped[str] = mapped_column(String(128), nullable=False)
    detail_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict, server_default=JSON_DEFAULT_EMPTY_OBJECT)

    run: Mapped[MultiSymbolRun] = relationship(back_populates="trades", lazy="raise")
    trade_group: Mapped[MultiSymbolTradeGroup] = relationship(back_populates="trades", lazy="raise")


class MultiSymbolEquityPoint(Base):
    __tablename__ = "multi_symbol_equity_points"
    __table_args__ = (
        UniqueConstraint("run_id", "trade_date", name="uq_multi_symbol_equity_points_run_date"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("multi_symbol_runs.id", ondelete="CASCADE"), nullable=False)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    equity: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    cash: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    position_value: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    drawdown_pct: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)

    run: Mapped[MultiSymbolRun] = relationship(back_populates="equity_points", lazy="raise")


class MultiSymbolSymbolEquityPoint(Base):
    __tablename__ = "multi_symbol_symbol_equity_points"
    __table_args__ = (
        Index("ix_multi_symbol_symbol_equity_points_run_symbol_id", "run_symbol_id"),
        UniqueConstraint("run_symbol_id", "trade_date", name="uq_multi_symbol_symbol_equity_points_symbol_date"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    run_symbol_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("multi_symbol_run_symbols.id", ondelete="CASCADE"), nullable=False)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    equity: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    cash: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    position_value: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    drawdown_pct: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)

    run_symbol: Mapped[MultiSymbolRunSymbol] = relationship(back_populates="equity_points", lazy="raise")


class MultiStepRun(Base):
    __tablename__ = "multi_step_runs"
    __table_args__ = (
        Index("ix_multi_step_runs_user_id", "user_id"),
        Index("ix_multi_step_runs_user_created_at", "user_id", "created_at"),
        Index("ix_multi_step_runs_status", "status"),
        Index("ix_multi_step_runs_celery_task_id", "celery_task_id"),
        Index("ix_multi_step_runs_status_celery_created", "status", "celery_task_id", "created_at"),
        Index("ix_multi_step_runs_dispatch_started_at", "dispatch_started_at"),
        Index("ix_multi_step_runs_queued", "created_at", postgresql_where=text("status = 'queued'")),
        UniqueConstraint("user_id", "idempotency_key", name="uq_multi_step_runs_user_idempotency_key"),
        CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
            name="ck_multi_step_runs_valid_run_status",
        ),
        CheckConstraint("start_date < end_date", name="ck_multi_step_runs_date_order"),
        CheckConstraint("account_size > 0", name="ck_multi_step_runs_account_positive"),
        CheckConstraint("risk_per_trade_pct > 0 AND risk_per_trade_pct <= 100", name="ck_multi_step_runs_risk_pct_range"),
        CheckConstraint("commission_per_contract >= 0", name="ck_multi_step_runs_commission_nonneg"),
        CheckConstraint("slippage_pct >= 0 AND slippage_pct <= 5", name="ck_multi_step_runs_slippage_range"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued", server_default="queued")
    name: Mapped[str | None] = mapped_column(String(120), nullable=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    workflow_type: Mapped[str] = mapped_column(String(80), nullable=False)
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[date] = mapped_column(Date, nullable=False)
    account_size: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    risk_per_trade_pct: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)
    commission_per_contract: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    slippage_pct: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"), server_default="0")
    input_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict, server_default=JSON_DEFAULT_EMPTY_OBJECT)
    warnings_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON_VARIANT, nullable=False, default=list, server_default=JSON_DEFAULT_EMPTY_ARRAY)
    idempotency_key: Mapped[str | None] = mapped_column(String(80), nullable=True)
    celery_task_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    trade_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    win_rate: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"), server_default="0")
    total_roi_pct: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"), server_default="0")
    average_win_amount: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    average_loss_amount: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    average_holding_period_days: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"), server_default="0")
    average_dte_at_open: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"), server_default="0")
    max_drawdown_pct: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False, default=Decimal("0"), server_default="0")
    total_commissions: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    total_net_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    starting_equity: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    ending_equity: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    profit_factor: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    payoff_ratio: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    expectancy: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    sharpe_ratio: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    sortino_ratio: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    cagr_pct: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    calmar_ratio: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    max_consecutive_wins: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    max_consecutive_losses: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    recovery_factor: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    dispatch_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    user: Mapped[User] = relationship(back_populates="multi_step_runs", lazy="raise")
    steps: Mapped[list[MultiStepRunStep]] = relationship(back_populates="run", cascade="all, delete-orphan", lazy="raise")
    events: Mapped[list[MultiStepStepEvent]] = relationship(back_populates="run", cascade="all, delete-orphan", lazy="raise")
    trades: Mapped[list[MultiStepTrade]] = relationship(back_populates="run", cascade="all, delete-orphan", lazy="raise")
    equity_points: Mapped[list[MultiStepEquityPoint]] = relationship(back_populates="run", cascade="all, delete-orphan", lazy="raise")


class MultiStepRunStep(Base):
    __tablename__ = "multi_step_run_steps"
    __table_args__ = (
        UniqueConstraint("run_id", "step_number", name="uq_multi_step_run_steps_run_step_number"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("multi_step_runs.id", ondelete="CASCADE"), nullable=False)
    step_number: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    action: Mapped[str] = mapped_column(String(32), nullable=False)
    trigger_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict, server_default=JSON_DEFAULT_EMPTY_OBJECT)
    contract_selection_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict, server_default=JSON_DEFAULT_EMPTY_OBJECT)
    failure_policy: Mapped[str] = mapped_column(String(32), nullable=False, default="liquidate", server_default="liquidate")
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="pending", server_default="pending")
    triggered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    executed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    run: Mapped[MultiStepRun] = relationship(back_populates="steps", lazy="raise")


class MultiStepStepEvent(Base):
    __tablename__ = "multi_step_step_events"
    __table_args__ = (
        Index("ix_multi_step_step_events_run_event_at", "run_id", "event_at"),
        Index("ix_multi_step_step_events_step_number", "step_number"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("multi_step_runs.id", ondelete="CASCADE"), nullable=False)
    step_id: Mapped[uuid.UUID | None] = mapped_column(GUID(), ForeignKey("multi_step_run_steps.id", ondelete="SET NULL"), nullable=True)
    step_number: Mapped[int] = mapped_column(Integer, nullable=False)
    event_type: Mapped[str] = mapped_column(String(24), nullable=False)
    event_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict, server_default=JSON_DEFAULT_EMPTY_OBJECT)

    run: Mapped[MultiStepRun] = relationship(back_populates="events", lazy="raise")


class MultiStepTrade(Base):
    __tablename__ = "multi_step_trades"
    __table_args__ = (
        Index("ix_multi_step_trades_run_entry_date", "run_id", "entry_date"),
        CheckConstraint("quantity > 0", name="ck_multi_step_trades_quantity_positive"),
        CheckConstraint("entry_date <= exit_date", name="ck_multi_step_trades_date_order"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("multi_step_runs.id", ondelete="CASCADE"), nullable=False)
    step_number: Mapped[int] = mapped_column(Integer, nullable=False)
    option_ticker: Mapped[str] = mapped_column(String(64), nullable=False)
    strategy_type: Mapped[str] = mapped_column(String(48), nullable=False)
    entry_date: Mapped[date] = mapped_column(Date, nullable=False)
    exit_date: Mapped[date] = mapped_column(Date, nullable=False)
    expiration_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    dte_at_open: Mapped[int | None] = mapped_column(Integer, nullable=True)
    holding_period_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    entry_underlying_close: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    exit_underlying_close: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    entry_mid: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    exit_mid: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    gross_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    net_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    total_commissions: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"), server_default="0")
    entry_reason: Mapped[str] = mapped_column(String(128), nullable=False)
    exit_reason: Mapped[str] = mapped_column(String(128), nullable=False)
    detail_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict, server_default=JSON_DEFAULT_EMPTY_OBJECT)

    run: Mapped[MultiStepRun] = relationship(back_populates="trades", lazy="raise")


class MultiStepEquityPoint(Base):
    __tablename__ = "multi_step_equity_points"
    __table_args__ = (
        UniqueConstraint("run_id", "trade_date", name="uq_multi_step_equity_points_run_date"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("multi_step_runs.id", ondelete="CASCADE"), nullable=False)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    equity: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    cash: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    position_value: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    drawdown_pct: Mapped[Decimal] = mapped_column(Numeric(10, 4), nullable=False)

    run: Mapped[MultiStepRun] = relationship(back_populates="equity_points", lazy="raise")


class BacktestTemplate(Base):
    __tablename__ = "backtest_templates"
    __table_args__ = (
        Index("ix_backtest_templates_user_created_at", "user_id", "created_at"),
        Index("ix_backtest_templates_user_strategy", "user_id", "strategy_type"),
        Index("ix_backtest_templates_user_updated_at", "user_id", "updated_at"),
        UniqueConstraint("user_id", "name", name="uq_backtest_templates_user_name"),
        CheckConstraint("length(name) > 0", name="ck_backtest_templates_name_not_empty"),
        CheckConstraint("description IS NULL OR length(description) <= 2000", name="ck_backtest_templates_desc_length"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    description: Mapped[str | None] = mapped_column(String(2000), nullable=True)
    strategy_type: Mapped[str] = mapped_column(String(48), nullable=False)
    config_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict, server_default=JSON_DEFAULT_EMPTY_OBJECT)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    user: Mapped[User] = relationship(back_populates="templates", lazy="raise")


class ScannerJob(Base):
    __tablename__ = "scanner_jobs"
    __table_args__ = (
        UniqueConstraint("user_id", "idempotency_key", name="uq_scanner_jobs_user_idempotency_key"),
        UniqueConstraint("refresh_key", name="uq_scanner_jobs_refresh_key"),
        Index("ix_scanner_jobs_user_id", "user_id"),
        Index("ix_scanner_jobs_user_created_at", "user_id", "created_at"),
        Index("ix_scanner_jobs_user_status", "user_id", "status"),
        Index("ix_scanner_jobs_request_hash", "request_hash"),
        Index("ix_scanner_jobs_celery_task_id", "celery_task_id"),
        Index("ix_scanner_jobs_status_celery_created", "status", "celery_task_id", "created_at"),
        Index("ix_scanner_jobs_dedup_lookup", "user_id", "request_hash", "mode", "created_at"),
        Index("ix_scanner_jobs_parent_job_id", "parent_job_id"),
        Index("ix_scanner_jobs_pipeline_run_id", "pipeline_run_id"),
        Index("ix_scanner_jobs_refresh_sources", "refresh_daily", "status"),
        Index(
            "ix_scanner_jobs_refresh_sources_lookup",
            "user_id",
            "request_hash",
            "mode",
            desc("completed_at"),
            postgresql_where=text("refresh_daily = true AND status = 'succeeded' AND completed_at IS NOT NULL"),
        ),
        Index(
            "uq_scanner_jobs_active_dedup",
            "user_id", "request_hash", "mode",
            unique=True,
            postgresql_where=text("status IN ('queued', 'running')"),
        ),
        Index("ix_scanner_jobs_queued", "created_at", postgresql_where=text("status = 'queued'")),
        CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
            name="ck_scanner_jobs_valid_job_status",
        ),
        CheckConstraint(
            "plan_tier_snapshot IN ('free', 'pro', 'premium')",
            name="ck_scanner_jobs_valid_plan_tier",
        ),
        CheckConstraint(
            "mode IN ('basic', 'advanced')",
            name="ck_scanner_jobs_valid_mode",
        ),
        CheckConstraint(
            "job_kind IN ('manual', 'refresh', 'nightly')",
            name="ck_scanner_jobs_valid_job_kind",
        ),
        CheckConstraint("refresh_priority >= 0 AND refresh_priority <= 100", name="ck_scanner_jobs_refresh_priority_range"),
        CheckConstraint("candidate_count >= 0", name="ck_scanner_jobs_candidate_count_nonneg"),
        CheckConstraint("evaluated_candidate_count >= 0", name="ck_scanner_jobs_evaluated_count_nonneg"),
        CheckConstraint("recommendation_count >= 0", name="ck_scanner_jobs_recommendation_count_nonneg"),
        CheckConstraint(
            "engine_version IN ('options-multileg-v1', 'options-multileg-v2')",
            name="ck_scanner_jobs_valid_engine_version",
        ),
        CheckConstraint(
            "ranking_version IN ('scanner-ranking-v1', 'scanner-ranking-v2')",
            name="ck_scanner_jobs_valid_ranking_version",
        ),
        CheckConstraint(
            "name IS NULL OR length(name) > 0",
            name="ck_scanner_jobs_name_not_empty",
        ),
        Index("ix_scanner_jobs_dispatch_started_at", "dispatch_started_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    parent_job_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID(), ForeignKey("scanner_jobs.id", ondelete="SET NULL"), nullable=True
    )
    pipeline_run_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID(), ForeignKey("nightly_pipeline_runs.id", ondelete="SET NULL"), nullable=True
    )
    name: Mapped[str | None] = mapped_column(String(120), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued", server_default="queued")
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    plan_tier_snapshot: Mapped[str] = mapped_column(String(16), nullable=False)
    job_kind: Mapped[str] = mapped_column(String(32), nullable=False, default="manual", server_default="manual")
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    idempotency_key: Mapped[str | None] = mapped_column(String(80), nullable=True)
    refresh_key: Mapped[str | None] = mapped_column(String(120), nullable=True)
    refresh_daily: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    refresh_priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    candidate_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    evaluated_candidate_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    recommendation_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    request_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict, server_default=JSON_DEFAULT_EMPTY_OBJECT)
    warnings_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON_VARIANT, nullable=False, default=list, server_default=JSON_DEFAULT_EMPTY_ARRAY)
    ranking_version: Mapped[str] = mapped_column(String(32), nullable=False, default="scanner-ranking-v1", server_default="scanner-ranking-v1")
    engine_version: Mapped[str] = mapped_column(String(32), nullable=False, default="options-multileg-v2", server_default="options-multileg-v2")
    celery_task_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    dispatch_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    user: Mapped[User] = relationship(back_populates="scanner_jobs", lazy="raise")
    parent_job: Mapped[ScannerJob | None] = relationship(remote_side=[id], back_populates=None, lazy="raise")

    @validates('evaluated_candidate_count')
    def _validate_evaluated_count(self, key, value):
        if value is not None and self.candidate_count is not None and self.candidate_count > 0:
            _MAX_EVAL_MULTIPLIER = 5
            if value > self.candidate_count * _MAX_EVAL_MULTIPLIER:
                structlog.get_logger("models").error(
                    "evaluated_candidate_count_vastly_exceeds_candidate_count",
                    evaluated=value,
                    candidate=self.candidate_count,
                    max_multiplier=_MAX_EVAL_MULTIPLIER,
                )
                raise ValueError(
                    f"evaluated_candidate_count ({value}) exceeds candidate_count "
                    f"({self.candidate_count}) by more than {_MAX_EVAL_MULTIPLIER}x"
                )
            if value > self.candidate_count * 2:
                structlog.get_logger("models").warning(
                    "evaluated_candidate_count_exceeds_candidate_count",
                    evaluated=value,
                    candidate=self.candidate_count,
                )
        return value
    recommendations: Mapped[list[ScannerRecommendation]] = relationship(
        back_populates="job",
        cascade="all, delete-orphan",
        order_by="ScannerRecommendation.rank",
        lazy="raise",
    )


class ScannerRecommendation(Base):
    __tablename__ = "scanner_recommendations"
    __table_args__ = (
        UniqueConstraint("scanner_job_id", "rank", name="uq_scanner_recommendations_job_rank"),
        Index("ix_scanner_recommendations_lookup", "symbol", "strategy_type", "rule_set_hash"),
        Index("ix_scanner_recommendations_summary_gin", "summary_json", postgresql_using="gin", postgresql_ops={"summary_json": "jsonb_path_ops"}),
        CheckConstraint("rank >= 1", name="ck_scanner_recommendations_rank_positive"),
        CheckConstraint("length(symbol) > 0", name="ck_scanner_recommendations_symbol_not_empty"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    scanner_job_id: Mapped[uuid.UUID] = mapped_column(
        GUID(), ForeignKey("scanner_jobs.id", ondelete="CASCADE"), nullable=False
    )
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    score: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    strategy_type: Mapped[str] = mapped_column(String(48), nullable=False)
    rule_set_name: Mapped[str] = mapped_column(String(120), nullable=False)
    rule_set_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    # These payload columns must be written explicitly by the scan executor.
    # Hidden JSON defaults make missing writes look like valid empty results.
    request_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False)
    summary_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False)
    warnings_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON_VARIANT, nullable=False)
    trades_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON_VARIANT, nullable=False)
    equity_curve_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON_VARIANT, nullable=False)
    historical_performance_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False)
    forecast_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False)
    ranking_features_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    job: Mapped[ScannerJob] = relationship(back_populates="recommendations", lazy="raise")


class ExportJob(Base):
    __tablename__ = "export_jobs"
    __table_args__ = (
        UniqueConstraint("user_id", "idempotency_key", name="uq_export_jobs_user_idempotency_key"),
        Index("ix_export_jobs_user_id", "user_id"),
        Index("ix_export_jobs_user_created_at", "user_id", "created_at"),
        Index("ix_export_jobs_user_status", "user_id", "status"),
        Index("ix_export_jobs_celery_task_id", "celery_task_id"),
        Index("ix_export_jobs_backtest_run_id", "backtest_run_id"),
        Index("ix_export_jobs_multi_symbol_run_id", "multi_symbol_run_id"),
        Index("ix_export_jobs_multi_step_run_id", "multi_step_run_id"),
        Index("ix_export_jobs_status_celery_created", "status", "celery_task_id", "created_at"),
        Index("ix_export_jobs_status_expires_at", "status", "expires_at"),
        Index("ix_export_jobs_queued", "created_at", postgresql_where=text("status = 'queued'")),
        Index("ix_export_jobs_sha256_hex", "sha256_hex"),
        Index("ix_export_jobs_storage_key", "storage_key"),
        Index("ix_export_jobs_dispatch_started_at", "dispatch_started_at"),
        CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled', 'expired')",
            name="ck_export_jobs_valid_export_status",
        ),
        CheckConstraint(
            "export_target_kind IN ('backtest', 'multi_symbol', 'multi_step')",
            name="ck_export_jobs_valid_target_kind",
        ),
        CheckConstraint(
            "((CASE WHEN backtest_run_id IS NOT NULL THEN 1 ELSE 0 END) + "
            "(CASE WHEN multi_symbol_run_id IS NOT NULL THEN 1 ELSE 0 END) + "
            "(CASE WHEN multi_step_run_id IS NOT NULL THEN 1 ELSE 0 END)) = 1",
            name="ck_export_jobs_exactly_one_target",
        ),
        CheckConstraint(
            "status != 'succeeded' OR content_bytes IS NOT NULL OR storage_key IS NOT NULL",
            name="ck_export_jobs_succeeded_has_storage",
        ),
        CheckConstraint("size_bytes >= 0", name="ck_export_jobs_size_bytes_nonneg"),
        CheckConstraint("export_format IN ('csv', 'pdf')", name="ck_export_jobs_valid_export_format"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    backtest_run_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID(), ForeignKey("backtest_runs.id", ondelete="CASCADE"), nullable=True
    )
    multi_symbol_run_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID(), ForeignKey("multi_symbol_runs.id", ondelete="CASCADE"), nullable=True
    )
    multi_step_run_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID(), ForeignKey("multi_step_runs.id", ondelete="CASCADE"), nullable=True
    )
    export_target_kind: Mapped[str] = mapped_column(String(24), nullable=False, default="backtest", server_default="backtest")
    export_format: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued", server_default="queued")
    file_name: Mapped[str] = mapped_column(String(255), nullable=False)
    mime_type: Mapped[str] = mapped_column(String(128), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default="0")
    sha256_hex: Mapped[str | None] = mapped_column(String(64), nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(80), nullable=True)
    celery_task_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    content_bytes: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True, deferred=True)
    storage_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    dispatch_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Exports with expires_at in the past should be transitioned to 'expired'
    # status by a scheduled maintenance task (e.g. a periodic Celery beat job).
    # The ix_export_jobs_status_expires_at index supports efficient lookup of
    # candidates for this transition.
    user: Mapped[User] = relationship(back_populates="export_jobs", lazy="raise")
    backtest_run: Mapped[BacktestRun | None] = relationship(back_populates="exports", lazy="raise")


class AuditEvent(Base):
    """Audit trail for user actions and system events.

    DEDUP CONSTRAINT: ``uq_audit_events_dedup`` on (event_type, subject_type,
    subject_id) means only ONE event per triple is stored. For repeatable
    events (e.g. ``export.downloaded``), callers MUST use
    ``AuditService.record_always()`` instead of ``record()`` to bypass the
    dedup constraint. Using ``record()`` for repeatable events will silently
    drop all but the first occurrence.

    NULL SUBJECT CONSTRAINT: ``uq_audit_events_dedup_null_subject`` allows
    only ONE event per (event_type, subject_type) where subject_id IS NULL.
    No current caller passes subject_id=None to record(), so this constraint
    is defensive.  If you add system-level events without a subject, use
    ``record_always()`` to avoid hitting this partial unique index.
    """

    __tablename__ = "audit_events"
    __table_args__ = (
        Index("ix_audit_events_user_id", "user_id"),
        Index("ix_audit_events_event_type", "event_type"),
        Index("ix_audit_events_user_created_at", "user_id", "created_at"),
        Index("ix_audit_events_event_type_created_at", "event_type", "created_at"),
        Index("ix_audit_events_created_at", "created_at"),
        UniqueConstraint("event_type", "subject_type", "subject_id", name="uq_audit_events_dedup"),
        Index(
            "uq_audit_events_dedup_null_subject",
            "event_type", "subject_type",
            unique=True,
            postgresql_where=text("subject_id IS NULL"),
        ),
        CheckConstraint(
            "subject_id IS NULL OR length(subject_id) > 0",
            name="ck_audit_events_subject_id_not_empty",
        ),
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
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict, server_default=JSON_DEFAULT_EMPTY_OBJECT)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    user: Mapped[User] = relationship(back_populates="audit_events", lazy="raise")


class NightlyPipelineRun(Base):
    # Scanner jobs created by this pipeline are linked implicitly by trade_date,
    # not by a foreign key.  To find scanner jobs spawned by a given pipeline
    # run, query ScannerJob where job_kind='nightly' and the request_snapshot
    # trade_date matches this run's trade_date.
    __tablename__ = "nightly_pipeline_runs"
    __table_args__ = (
        Index("ix_nightly_pipeline_runs_trade_date", "trade_date"),
        Index("ix_nightly_pipeline_runs_status", "status"),
        Index("ix_nightly_pipeline_runs_date_status", "trade_date", "status"),
        Index("ix_nightly_pipeline_runs_status_created", "status", "created_at"),
        Index("ix_nightly_pipeline_runs_cursor", "created_at", "id"),
        Index("ix_nightly_pipeline_runs_celery_task_id", "celery_task_id"),
        Index("ix_nightly_pipeline_runs_status_celery_created", "status", "celery_task_id", "created_at"),
        Index(
            "ix_nightly_pipeline_runs_queued",
            "created_at",
            postgresql_where=text("status = 'queued'"),
        ),
        Index(
            "uq_pipeline_runs_succeeded_trade_date",
            "trade_date",
            unique=True,
            postgresql_where=text("status = 'succeeded'"),
        ),
        CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
            name="ck_nightly_pipeline_runs_valid_pipeline_status",
        ),
        CheckConstraint(
            "stage IN ('universe_screen', 'strategy_match', 'quick_backtest', 'full_backtest', 'forecast_rank')",
            name="ck_nightly_pipeline_runs_valid_stage",
        ),
        CheckConstraint("symbols_screened >= 0", name="ck_nightly_pipeline_runs_symbols_screened_nonneg"),
        CheckConstraint("symbols_after_screen >= 0", name="ck_nightly_pipeline_runs_symbols_after_nonneg"),
        CheckConstraint("pairs_generated >= 0", name="ck_nightly_pipeline_runs_pairs_nonneg"),
        CheckConstraint("quick_backtests_run >= 0", name="ck_nightly_pipeline_runs_quick_bt_nonneg"),
        CheckConstraint("full_backtests_run >= 0", name="ck_nightly_pipeline_runs_full_bt_nonneg"),
        CheckConstraint("recommendations_produced >= 0", name="ck_nightly_pipeline_runs_recs_nonneg"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    # Default "queued": newly created pipeline runs start in "queued" and
    # transition to "running" when the worker picks them up. The check
    # constraint above enumerates all valid statuses.
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued", server_default="queued")
    stage: Mapped[str] = mapped_column(String(32), nullable=False, default="universe_screen", server_default="universe_screen")
    symbols_screened: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    symbols_after_screen: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    pairs_generated: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    quick_backtests_run: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    full_backtests_run: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    recommendations_produced: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    duration_seconds: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    celery_task_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    stage_details_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict, server_default=JSON_DEFAULT_EMPTY_OBJECT)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    recommendations: Mapped[list[DailyRecommendation]] = relationship(
        back_populates="pipeline_run", cascade="all, delete-orphan", lazy="raise"
    )


class DailyRecommendation(Base):
    __tablename__ = "daily_recommendations"
    __table_args__ = (
        UniqueConstraint("pipeline_run_id", "rank", name="uq_daily_recs_pipeline_rank"),
        Index("ix_daily_recs_trade_date", "trade_date"),
        Index("ix_daily_recs_symbol_strategy", "symbol", "strategy_type"),
        Index("ix_daily_recs_created_at", "created_at"),
        CheckConstraint("rank >= 1", name="ck_daily_recommendations_rank_positive"),
        CheckConstraint("length(symbol) > 0", name="ck_daily_recommendations_symbol_not_empty"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    pipeline_run_id: Mapped[uuid.UUID] = mapped_column(
        GUID(), ForeignKey("nightly_pipeline_runs.id", ondelete="CASCADE"), nullable=False
    )
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    score: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    strategy_type: Mapped[str] = mapped_column(String(48), nullable=False)
    regime_labels: Mapped[list[str]] = mapped_column(JSON_VARIANT, nullable=False, default=list, server_default=JSON_DEFAULT_EMPTY_ARRAY)
    close_price: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    target_dte: Mapped[int] = mapped_column(Integer, nullable=False)
    config_snapshot_json: Mapped[dict[str, Any] | None] = mapped_column(JSON_VARIANT, nullable=True)
    summary_json: Mapped[dict[str, Any] | None] = mapped_column(JSON_VARIANT, nullable=True)
    forecast_json: Mapped[dict[str, Any] | None] = mapped_column(JSON_VARIANT, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    pipeline_run: Mapped[NightlyPipelineRun] = relationship(back_populates="recommendations", lazy="raise")

    @validates('regime_labels')
    def _validate_regime_labels(self, key, value):
        if value is not None and not isinstance(value, list):
            raise ValueError("regime_labels must be a list")
        if value is not None and not all(isinstance(v, str) for v in value):
            raise ValueError("regime_labels must contain only strings")
        return value


class StripeEvent(Base):
    __tablename__ = "stripe_events"
    __table_args__ = (
        UniqueConstraint("stripe_event_id", name="uq_stripe_events_event_id"),
        Index("ix_stripe_events_event_type", "event_type"),
        Index("ix_stripe_events_created_at", "created_at"),
        Index("ix_stripe_events_user_id", "user_id"),
        Index("ix_stripe_events_idempotency_status", "idempotency_status"),
        Index("ix_stripe_events_event_id_status", "stripe_event_id", "idempotency_status"),
        CheckConstraint(
            "idempotency_status IN ('processing', 'processed', 'ignored', 'error')",
            name="ck_stripe_events_valid_status",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    stripe_event_id: Mapped[str] = mapped_column(String(255), nullable=False)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    livemode: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    idempotency_status: Mapped[str] = mapped_column(String(16), nullable=False, default="processing", server_default="processing")
    user_id: Mapped[uuid.UUID | None] = mapped_column(
        GUID(), ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    request_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    ip_hash: Mapped[str | None] = mapped_column(String(128), nullable=True)
    error_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload_summary: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict, server_default=JSON_DEFAULT_EMPTY_OBJECT)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    user: Mapped[User | None] = relationship(back_populates="stripe_events", lazy="raise")


class OptionContractCatalogSnapshot(Base):
    __tablename__ = "option_contract_catalog_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "symbol",
            "as_of_date",
            "contract_type",
            "expiration_date",
            "strike_price_gte",
            "strike_price_lte",
            name="uq_option_contract_catalog_snapshots_query",
        ),
        Index(
            "ix_option_contract_catalog_snapshots_lookup",
            "symbol",
            "as_of_date",
            "contract_type",
            "expiration_date",
        ),
        CheckConstraint("length(symbol) > 0", name="ck_option_contract_catalog_snapshots_symbol_not_empty"),
        CheckConstraint(
            "contract_type IN ('call', 'put')",
            name="ck_option_contract_catalog_snapshots_contract_type",
        ),
        CheckConstraint(
            "strike_price_gte IS NULL OR strike_price_gte >= 0",
            name="ck_option_contract_catalog_snapshots_strike_gte_nonneg",
        ),
        CheckConstraint(
            "strike_price_lte IS NULL OR strike_price_lte >= 0",
            name="ck_option_contract_catalog_snapshots_strike_lte_nonneg",
        ),
        CheckConstraint(
            "strike_price_gte IS NULL OR strike_price_lte IS NULL OR strike_price_gte <= strike_price_lte",
            name="ck_option_contract_catalog_snapshots_strike_bounds",
        ),
        CheckConstraint(
            "contract_count >= 0",
            name="ck_option_contract_catalog_snapshots_contract_count_nonneg",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    as_of_date: Mapped[date] = mapped_column(Date, nullable=False)
    contract_type: Mapped[str] = mapped_column(String(8), nullable=False)
    expiration_date: Mapped[date] = mapped_column(Date, nullable=False)
    strike_price_gte: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    strike_price_lte: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    contracts_json: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON_VARIANT,
        nullable=False,
        default=list,
        server_default=JSON_DEFAULT_EMPTY_ARRAY,
    )
    contract_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class HistoricalOptionContractCatalogSnapshot(Base):
    __tablename__ = "historical_option_contract_catalog_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "symbol",
            "as_of_date",
            "contract_type",
            "expiration_date",
            "strike_price_gte",
            "strike_price_lte",
            name="uq_historical_option_contract_catalog_snapshots_query",
        ),
        Index(
            "ix_historical_option_contract_catalog_snapshots_lookup",
            "symbol",
            "as_of_date",
            "contract_type",
            "expiration_date",
        ),
        CheckConstraint(
            "length(symbol) > 0",
            name="ck_historical_option_contract_catalog_snapshots_symbol_not_empty",
        ),
        CheckConstraint(
            "contract_type IN ('call', 'put')",
            name="ck_historical_option_contract_catalog_snapshots_contract_type",
        ),
        CheckConstraint(
            "strike_price_gte IS NULL OR strike_price_gte >= 0",
            name="ck_historical_option_contract_catalog_snapshots_strike_gte_nonneg",
        ),
        CheckConstraint(
            "strike_price_lte IS NULL OR strike_price_lte >= 0",
            name="ck_historical_option_contract_catalog_snapshots_strike_lte_nonneg",
        ),
        CheckConstraint(
            "strike_price_gte IS NULL OR strike_price_lte IS NULL OR strike_price_gte <= strike_price_lte",
            name="ck_historical_option_contract_catalog_snapshots_strike_bounds",
        ),
        CheckConstraint(
            "contract_count >= 0",
            name="ck_historical_option_contract_catalog_snapshots_contract_count_nonneg",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    as_of_date: Mapped[date] = mapped_column(Date, nullable=False)
    contract_type: Mapped[str] = mapped_column(String(8), nullable=False)
    expiration_date: Mapped[date] = mapped_column(Date, nullable=False)
    strike_price_gte: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    strike_price_lte: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    contracts_json: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON_VARIANT,
        nullable=False,
        default=list,
        server_default=JSON_DEFAULT_EMPTY_ARRAY,
    )
    contract_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class HistoricalUnderlyingDayBar(Base):
    __tablename__ = "historical_underlying_day_bars"
    __table_args__ = (
        UniqueConstraint("symbol", "trade_date", name="uq_historical_underlying_day_bars_symbol_date"),
        Index(
            "ix_historical_underlying_day_bars_covering",
            "symbol",
            "trade_date",
            postgresql_include=["open_price", "high_price", "low_price", "close_price", "volume"],
        ),
        Index("ix_historical_underlying_day_bars_trade_date_desc", text("trade_date DESC")),
        CheckConstraint("length(symbol) > 0", name="ck_historical_underlying_day_bars_symbol_not_empty"),
        CheckConstraint("open_price > 0", name="ck_historical_underlying_day_bars_open_positive"),
        CheckConstraint("high_price > 0", name="ck_historical_underlying_day_bars_high_positive"),
        CheckConstraint("low_price > 0", name="ck_historical_underlying_day_bars_low_positive"),
        CheckConstraint("close_price > 0", name="ck_historical_underlying_day_bars_close_positive"),
        CheckConstraint("volume >= 0", name="ck_historical_underlying_day_bars_volume_nonneg"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    open_price: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    high_price: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    low_price: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    close_price: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    volume: Mapped[Decimal] = mapped_column(Numeric(24, 4), nullable=False)
    source_dataset: Mapped[str] = mapped_column(String(64), nullable=False, default="flatfile_day_aggs", server_default="flatfile_day_aggs")
    source_file_date: Mapped[date] = mapped_column(Date, nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class HistoricalOptionDayBar(Base):
    __tablename__ = "historical_option_day_bars"
    __table_args__ = (
        UniqueConstraint("option_ticker", "trade_date", name="uq_historical_option_day_bars_ticker_date"),
        Index("ix_historical_option_day_bars_underlying_date", "underlying_symbol", "trade_date"),
        Index(
            "ix_historical_option_day_bars_lookup",
            "underlying_symbol",
            "trade_date",
            "contract_type",
            "expiration_date",
            "strike_price",
        ),
        Index(
            "ix_historical_option_day_bars_contract_projection",
            "underlying_symbol",
            "trade_date",
            "contract_type",
            "expiration_date",
            "strike_price",
            postgresql_include=["option_ticker"],
        ),
        Index(
            "ix_historical_option_day_bars_quote_projection",
            "option_ticker",
            "trade_date",
            postgresql_include=["close_price"],
        ),
        Index("ix_historical_option_day_bars_trade_date_desc", text("trade_date DESC")),
        CheckConstraint("length(option_ticker) > 0", name="ck_historical_option_day_bars_ticker_not_empty"),
        CheckConstraint("length(underlying_symbol) > 0", name="ck_historical_option_day_bars_symbol_not_empty"),
        CheckConstraint("contract_type IN ('call', 'put')", name="ck_historical_option_day_bars_contract_type"),
        CheckConstraint("strike_price > 0", name="ck_historical_option_day_bars_strike_positive"),
        CheckConstraint("open_price >= 0", name="ck_historical_option_day_bars_open_nonneg"),
        CheckConstraint("high_price >= 0", name="ck_historical_option_day_bars_high_nonneg"),
        CheckConstraint("low_price >= 0", name="ck_historical_option_day_bars_low_nonneg"),
        CheckConstraint("close_price >= 0", name="ck_historical_option_day_bars_close_nonneg"),
        CheckConstraint("volume >= 0", name="ck_historical_option_day_bars_volume_nonneg"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    option_ticker: Mapped[str] = mapped_column(String(64), nullable=False)
    underlying_symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    expiration_date: Mapped[date] = mapped_column(Date, nullable=False)
    contract_type: Mapped[str] = mapped_column(String(8), nullable=False)
    strike_price: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    open_price: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    high_price: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    low_price: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    close_price: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    volume: Mapped[Decimal] = mapped_column(Numeric(24, 4), nullable=False)
    source_dataset: Mapped[str] = mapped_column(String(64), nullable=False, default="flatfile_day_aggs", server_default="flatfile_day_aggs")
    source_file_date: Mapped[date] = mapped_column(Date, nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class HistoricalExDividendDate(Base):
    __tablename__ = "historical_ex_dividend_dates"
    __table_args__ = (
        UniqueConstraint("provider_dividend_id", name="uq_historical_ex_dividend_dates_provider_dividend_id"),
        Index("ix_historical_ex_dividend_dates_date_desc", text("ex_dividend_date DESC")),
        CheckConstraint("length(symbol) > 0", name="ck_historical_ex_dividend_dates_symbol_not_empty"),
        CheckConstraint("cash_amount IS NULL OR cash_amount >= 0", name="ck_historical_ex_dividend_dates_cash_nonneg"),
        CheckConstraint(
            "split_adjusted_cash_amount IS NULL OR split_adjusted_cash_amount >= 0",
            name="ck_historical_ex_dividend_dates_split_cash_nonneg",
        ),
        CheckConstraint(
            "historical_adjustment_factor IS NULL OR historical_adjustment_factor >= 0",
            name="ck_historical_ex_dividend_dates_adjustment_factor_nonneg",
        ),
        CheckConstraint(
            "frequency IS NULL OR frequency >= 0",
            name="ck_historical_ex_dividend_dates_frequency_nonneg",
        ),
        CheckConstraint(
            "distribution_type IS NULL OR distribution_type IN ('recurring', 'special', 'supplemental', 'irregular', 'unknown')",
            name="ck_historical_ex_dividend_dates_distribution_type",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    ex_dividend_date: Mapped[date] = mapped_column(Date, nullable=False)
    provider_dividend_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    cash_amount: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    currency: Mapped[str | None] = mapped_column(String(8), nullable=True)
    declaration_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    record_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    pay_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    frequency: Mapped[int | None] = mapped_column(Integer, nullable=True)
    distribution_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    historical_adjustment_factor: Mapped[Decimal | None] = mapped_column(Numeric(18, 10), nullable=True)
    split_adjusted_cash_amount: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    source_dataset: Mapped[str] = mapped_column(String(64), nullable=False, default="rest_dividends", server_default="rest_dividends")
    source_file_date: Mapped[date] = mapped_column(Date, nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class HistoricalEarningsEvent(Base):
    __tablename__ = "historical_earnings_events"
    __table_args__ = (
        UniqueConstraint("provider_event_id", name="uq_historical_earnings_events_provider_event_id"),
        UniqueConstraint("symbol", "event_date", "event_type", name="uq_historical_earnings_events_symbol_date_type"),
        Index("ix_historical_earnings_events_date_desc", text("event_date DESC")),
        CheckConstraint("length(symbol) > 0", name="ck_historical_earnings_events_symbol_not_empty"),
        CheckConstraint(
            "event_type IN ('earnings_announcement_date', 'earnings_conference_call')",
            name="ck_historical_earnings_events_event_type",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    event_date: Mapped[date] = mapped_column(Date, nullable=False)
    event_type: Mapped[str] = mapped_column(String(48), nullable=False)
    provider_event_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source_dataset: Mapped[str] = mapped_column(String(64), nullable=False, default="rest_earnings", server_default="rest_earnings")
    source_file_date: Mapped[date] = mapped_column(Date, nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class HistoricalTreasuryYield(Base):
    __tablename__ = "historical_treasury_yields"
    __table_args__ = (
        UniqueConstraint("trade_date", name="uq_historical_treasury_yields_trade_date"),
        Index("ix_historical_treasury_yields_trade_date_desc", text("trade_date DESC")),
        CheckConstraint("yield_3_month >= 0 AND yield_3_month <= 1", name="ck_historical_treasury_yields_3m_range"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    yield_3_month: Mapped[Decimal] = mapped_column(Numeric(10, 6), nullable=False)
    source_dataset: Mapped[str] = mapped_column(String(64), nullable=False, default="rest_treasury", server_default="rest_treasury")
    source_file_date: Mapped[date] = mapped_column(Date, nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class SymbolAnalysis(Base):
    __tablename__ = "symbol_analyses"
    __table_args__ = (
        Index("ix_symbol_analyses_user_id", "user_id"),
        Index("ix_symbol_analyses_user_created", "user_id", "created_at"),
        Index("ix_symbol_analyses_symbol", "symbol"),
        Index("ix_symbol_analyses_status_created", "status", "created_at"),
        Index("ix_symbol_analyses_dispatch_started_at", "dispatch_started_at"),
        Index("ix_symbol_analyses_celery_task_id", "celery_task_id"),
        Index("ix_symbol_analyses_status_celery_created", "status", "celery_task_id", "created_at"),
        UniqueConstraint("user_id", "idempotency_key", name="uq_symbol_analyses_user_idempotency"),
        Index("ix_symbol_analyses_queued", "created_at", postgresql_where=text("status = 'queued'")),
        CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
            name="ck_symbol_analyses_valid_analysis_status",
        ),
        CheckConstraint("strategies_tested >= 0", name="ck_symbol_analyses_strategies_tested_nonneg"),
        CheckConstraint("configs_tested >= 0", name="ck_symbol_analyses_configs_tested_nonneg"),
        CheckConstraint("top_results_count >= 0", name="ck_symbol_analyses_top_results_nonneg"),
        CheckConstraint(
            "stage IN ('pending', 'regime', 'landscape', 'deep_dive', 'forecast')",
            name="ck_symbol_analyses_valid_stage",
        ),
        CheckConstraint("length(symbol) > 0", name="ck_symbol_analyses_symbol_not_empty"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued", server_default="queued")
    stage: Mapped[str] = mapped_column(String(32), nullable=False, default="pending", server_default="pending")
    close_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4), nullable=True)
    # Null means "not produced yet" or "intentionally omitted". Empty JSON means
    # the stage ran and produced an empty result. Do not collapse those states.
    regime_json: Mapped[dict[str, Any] | None] = mapped_column(JSON_VARIANT, nullable=True)
    landscape_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON_VARIANT, nullable=True)
    top_results_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON_VARIANT, nullable=True)
    forecast_json: Mapped[dict[str, Any] | None] = mapped_column(JSON_VARIANT, nullable=True)
    strategies_tested: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    configs_tested: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    top_results_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    duration_seconds: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(80), nullable=True)
    celery_task_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    dispatch_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    user: Mapped[User] = relationship(back_populates="symbol_analyses", lazy="raise")


class SweepJob(Base):
    __tablename__ = "sweep_jobs"
    __table_args__ = (
        Index("ix_sweep_jobs_user_id", "user_id"),
        Index("ix_sweep_jobs_user_created_at", "user_id", "created_at"),
        Index("ix_sweep_jobs_user_status", "user_id", "status"),
        Index("ix_sweep_jobs_user_symbol", "user_id", "symbol"),
        Index("ix_sweep_jobs_celery_task_id", "celery_task_id"),
        Index("ix_sweep_jobs_status_celery_created", "status", "celery_task_id", "created_at"),
        Index("ix_sweep_jobs_user_symbol_created", "user_id", "symbol", "created_at"),
        Index("ix_sweep_jobs_request_hash", "request_hash"),
        Index(
            "ix_sweep_jobs_active_dedup_lookup",
            "user_id",
            "symbol",
            "request_hash",
            "created_at",
            postgresql_where=text("status IN ('queued', 'running') AND request_hash IS NOT NULL"),
        ),
        Index("ix_sweep_jobs_dispatch_started_at", "dispatch_started_at"),
        UniqueConstraint("user_id", "idempotency_key", name="uq_sweep_jobs_user_idempotency_key"),
        Index("ix_sweep_jobs_queued", "created_at", postgresql_where=text("status = 'queued'")),
        CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
            name="ck_sweep_jobs_valid_status",
        ),
        CheckConstraint("candidate_count >= 0", name="ck_sweep_jobs_candidate_count_nonneg"),
        CheckConstraint("evaluated_candidate_count >= 0", name="ck_sweep_jobs_evaluated_count_nonneg"),
        CheckConstraint("result_count >= 0", name="ck_sweep_jobs_result_count_nonneg"),
        CheckConstraint(
            "plan_tier_snapshot IN ('free', 'pro', 'premium')",
            name="ck_sweep_jobs_valid_plan_tier",
        ),
        CheckConstraint(
            "engine_version IN ('options-multileg-v1', 'options-multileg-v2')",
            name="ck_sweep_jobs_valid_engine_version",
        ),
        CheckConstraint(
            "mode IN ('grid', 'genetic')",
            name="ck_sweep_jobs_valid_mode",
        ),
        CheckConstraint("length(symbol) > 0", name="ck_sweep_jobs_symbol_not_empty"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(GUID(), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    mode: Mapped[str] = mapped_column(String(16), nullable=False, default="grid", server_default="grid")
    plan_tier_snapshot: Mapped[str] = mapped_column(String(16), nullable=False, default="free", server_default="free")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued", server_default="queued")
    candidate_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    evaluated_candidate_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    result_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    request_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict, server_default=JSON_DEFAULT_EMPTY_OBJECT)
    request_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    warnings_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON_VARIANT, nullable=False, default=list, server_default=JSON_DEFAULT_EMPTY_ARRAY)
    prefetch_summary_json: Mapped[dict[str, Any] | None] = mapped_column(JSON_VARIANT, nullable=True)
    engine_version: Mapped[str] = mapped_column(String(32), nullable=False, default="options-multileg-v2", server_default="options-multileg-v2")
    celery_task_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(80), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    dispatch_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    user: Mapped[User] = relationship(back_populates="sweep_jobs", lazy="raise")
    results: Mapped[list[SweepResult]] = relationship(
        back_populates="job",
        cascade="all, delete-orphan",
        order_by="SweepResult.rank",
        lazy="raise",
    )



class OutboxMessage(Base):
    """Transactional outbox for reliable Celery task dispatch.

    ``dispatch_celery_task()`` writes an OutboxMessage in the same DB
    transaction as the job record.  After commit, it attempts to send the
    Celery task inline.  On success the message is marked ``"sent"``; on
    failure it stays ``"pending"`` and the ``poll_outbox`` beat task
    (every 30 s) picks it up and retries delivery.

    After ``retry_count`` reaches 10, the message is marked ``"failed"``
    and the correlated job is also failed with ``error_code="outbox_exhausted"``.
    The ``cleanup_outbox`` daily task removes messages older than 7 days.
    """
    __tablename__ = "outbox_messages"
    __table_args__ = (
        Index("ix_outbox_messages_status_created", "status", "created_at"),
        Index("ix_outbox_messages_correlation_id", "correlation_id"),
        CheckConstraint(
            "status IN ('pending', 'sent', 'failed')",
            name="ck_outbox_messages_valid_status",
        ),
        CheckConstraint("retry_count >= 0", name="ck_outbox_messages_retry_count_nonneg"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    task_name: Mapped[str] = mapped_column(String(128), nullable=False)
    task_kwargs_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict, server_default=JSON_DEFAULT_EMPTY_OBJECT)
    queue: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending", server_default="pending")
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    correlation_id: Mapped[uuid.UUID | None] = mapped_column(GUID(), nullable=True)


class SweepResult(Base):
    __tablename__ = "sweep_results"
    __table_args__ = (
        UniqueConstraint("sweep_job_id", "rank", name="uq_sweep_results_job_rank"),
        Index("ix_sweep_results_summary_gin", "summary_json", postgresql_using="gin", postgresql_ops={"summary_json": "jsonb_path_ops"}),
        CheckConstraint("rank >= 1", name="ck_sweep_results_rank_positive"),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    sweep_job_id: Mapped[uuid.UUID] = mapped_column(
        GUID(), ForeignKey("sweep_jobs.id", ondelete="CASCADE"), nullable=False
    )
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    score: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    strategy_type: Mapped[str] = mapped_column(String(48), nullable=False)
    parameter_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False)
    summary_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False)
    warnings_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON_VARIANT, nullable=False)
    trades_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON_VARIANT, nullable=False)
    equity_curve_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON_VARIANT, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    job: Mapped[SweepJob] = relationship(back_populates="results", lazy="raise")


class TaskResult(Base):
    __tablename__ = "task_results"
    __table_args__ = (
        Index("ix_task_results_task_name_created", "task_name", "created_at"),
        Index("ix_task_results_correlation_id", "correlation_id"),
        Index("ix_task_results_status_created", "status", "created_at"),
        Index("ix_task_results_created_at", "created_at"),
        CheckConstraint(
            "status IN ('succeeded', 'failed', 'retried', 'timeout')",
            name="ck_task_results_valid_status",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(GUID(), primary_key=True, default=uuid.uuid4)
    task_name: Mapped[str] = mapped_column(String(128), nullable=False)
    task_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    correlation_id: Mapped[uuid.UUID | None] = mapped_column(GUID(), nullable=True)
    correlation_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    duration_seconds: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    result_summary_json: Mapped[dict[str, Any]] = mapped_column(JSON_VARIANT, nullable=False, default=dict, server_default=JSON_DEFAULT_EMPTY_OBJECT)
    worker_hostname: Mapped[str | None] = mapped_column(String(255), nullable=True)
    retries: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
