from __future__ import annotations

from pathlib import Path

from sqlalchemy import CheckConstraint, UniqueConstraint

from backtestforecast.models import User

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASELINE_MIGRATION = PROJECT_ROOT / "alembic" / "versions" / "20260324_0001_consolidated_baseline.py"


def test_user_model_has_expected_billing_account_columns() -> None:
    columns = User.__table__.columns

    assert "stripe_customer_id" in columns
    assert "stripe_subscription_id" in columns
    assert "stripe_price_id" in columns
    assert "subscription_status" in columns
    assert "subscription_billing_interval" in columns
    assert "subscription_current_period_end" in columns
    assert "cancel_at_period_end" in columns

    assert columns["stripe_customer_id"].nullable is True
    assert columns["stripe_subscription_id"].nullable is True
    assert columns["subscription_status"].nullable is True
    assert columns["cancel_at_period_end"].nullable is False


def test_user_model_keeps_unique_constraints_for_stripe_identifiers() -> None:
    constraints = {c.name: c for c in User.__table__.constraints if isinstance(c, UniqueConstraint)}

    constrained_columns = {
        tuple(column.name for column in constraint.columns)
        for constraint in constraints.values()
    }
    assert ("stripe_customer_id",) in constrained_columns
    assert ("stripe_subscription_id",) in constrained_columns
    assert ("clerk_user_id",) in constrained_columns


def test_user_model_check_constraints_cover_billing_fields() -> None:
    checks = {c.name: str(c.sqltext) for c in User.__table__.constraints if isinstance(c, CheckConstraint)}

    assert "ck_users_valid_plan_tier" in checks
    assert "ck_users_valid_subscription_status" in checks
    assert "ck_users_valid_billing_interval" in checks
    assert "active" in checks["ck_users_valid_subscription_status"]
    assert "canceled" in checks["ck_users_valid_subscription_status"]
    assert "monthly" in checks["ck_users_valid_billing_interval"]
    assert "yearly" in checks["ck_users_valid_billing_interval"]


def test_baseline_migration_is_consolidated_snapshot() -> None:
    text = BASELINE_MIGRATION.read_text(encoding="utf-8")

    assert "Base.metadata.create_all" in text
    assert "backtestforecast.models" in text


def test_consolidated_baseline_is_present() -> None:
    text = BASELINE_MIGRATION.read_text(encoding="utf-8")

    assert "Base.metadata.create_all" in text
    assert "_TRIGGER_TABLES" in text
