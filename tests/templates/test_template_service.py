from __future__ import annotations

import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backtestforecast.db.base import Base
from backtestforecast.errors import NotFoundError, QuotaExceededError
from backtestforecast.models import User
from backtestforecast.schemas.templates import (
    CreateTemplateRequest,
    TemplateConfig,
    UpdateTemplateRequest,
)
from backtestforecast.services.templates import BacktestTemplateService


@pytest.fixture()
def db_session():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)()
    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(engine)
        engine.dispose()


@pytest.fixture()
def free_user(db_session: Session) -> User:
    user = User(clerk_user_id="test_user", email="test@example.com", plan_tier="free")
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


@pytest.fixture()
def pro_user(db_session: Session) -> User:
    user = User(clerk_user_id="test_pro", email="pro@example.com", plan_tier="pro", subscription_status="active")
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


def _config() -> TemplateConfig:
    return TemplateConfig(
        strategy_type="long_call",
        target_dte=30,
        dte_tolerance_days=5,
        max_holding_days=10,
        account_size=10000,
        risk_per_trade_pct=2,
        commission_per_contract=0.65,
        entry_rules=[],
    )


def test_create_and_list(db_session, free_user):
    service = BacktestTemplateService(db_session)
    req = CreateTemplateRequest(name="My template", config=_config())

    created = service.create(free_user, req)
    assert created.name == "My template"
    assert created.strategy_type == "long_call"

    listed = service.list_templates(free_user)
    assert listed.total == 1
    assert listed.items[0].id == created.id


def test_update_template(db_session, free_user):
    service = BacktestTemplateService(db_session)
    created = service.create(free_user, CreateTemplateRequest(name="Original", config=_config()))

    updated = service.update(
        free_user,
        created.id,
        UpdateTemplateRequest(name="Renamed", description="A note"),
    )
    assert updated.name == "Renamed"
    assert updated.description == "A note"


def test_delete_template(db_session, free_user):
    service = BacktestTemplateService(db_session)
    created = service.create(free_user, CreateTemplateRequest(name="To delete", config=_config()))

    service.delete(free_user, created.id)
    assert service.list_templates(free_user).total == 0


def test_get_nonexistent_raises(db_session, free_user):
    service = BacktestTemplateService(db_session)
    with pytest.raises(NotFoundError):
        service.get_template(free_user, uuid.uuid4())


def test_free_tier_limit_is_3(db_session, free_user):
    service = BacktestTemplateService(db_session)
    for i in range(3):
        service.create(free_user, CreateTemplateRequest(name=f"T{i}", config=_config()))

    with pytest.raises(QuotaExceededError):
        service.create(free_user, CreateTemplateRequest(name="T3", config=_config()))


def test_pro_tier_has_higher_limit(db_session, pro_user):
    service = BacktestTemplateService(db_session)
    for i in range(5):
        service.create(pro_user, CreateTemplateRequest(name=f"Pro{i}", config=_config()))
    assert service.list_templates(pro_user).total == 5


def test_canceled_pro_gets_free_limit(db_session):
    user = User(
        clerk_user_id="canceled_pro",
        email="canceled@example.com",
        plan_tier="pro",
        subscription_status="canceled",
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)

    service = BacktestTemplateService(db_session)
    for i in range(3):
        service.create(user, CreateTemplateRequest(name=f"T{i}", config=_config()))
    with pytest.raises(QuotaExceededError):
        service.create(user, CreateTemplateRequest(name="T3", config=_config()))


def test_template_limit_shown_in_list(db_session, free_user):
    service = BacktestTemplateService(db_session)
    result = service.list_templates(free_user)
    assert result.template_limit == 3
