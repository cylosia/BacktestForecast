from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import structlog
from sqlalchemy import select
from sqlalchemy.orm import Session

from backtestforecast.billing.entitlements import PlanTier, normalize_plan_tier
from backtestforecast.errors import ConfigurationError, ConflictError, NotFoundError, QuotaExceededError, ValidationError
from backtestforecast.models import BacktestTemplate, User
from backtestforecast.repositories.templates import BacktestTemplateRepository
from backtestforecast.schemas.templates import (
    UNSET,
    CreateTemplateRequest,
    TemplateListResponse,
    TemplateResponse,
    UpdateTemplateRequest,
)

logger = structlog.get_logger("services.templates")

TEMPLATE_LIMITS: dict[PlanTier, int | None] = {
    PlanTier.FREE: 3,
    PlanTier.PRO: 25,
    PlanTier.PREMIUM: 100,
}


def _resolve_template_limit(
    plan_tier: str | None,
    subscription_status: str | None,
    subscription_current_period_end: datetime | None = None,
) -> int:
    tier = normalize_plan_tier(plan_tier, subscription_status, subscription_current_period_end)
    limit = TEMPLATE_LIMITS.get(tier)
    if limit is None:
        raise ConfigurationError(f"Unknown plan tier for template limit: {tier}")
    return limit


class BacktestTemplateService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.repository = BacktestTemplateRepository(session)

    def create(self, user: User, request: CreateTemplateRequest) -> TemplateResponse:
        from sqlalchemy.exc import IntegrityError

        self._enforce_template_limit(user)
        config_data = request.config.model_dump(mode="json")

        template = BacktestTemplate(
            user_id=user.id,
            name=request.name,
            description=request.description,
            strategy_type=request.config.strategy_type.value,
            config_json=config_data,
        )
        self.repository.add(template)
        try:
            self.session.commit()
        except IntegrityError:
            self.session.rollback()
            raise ValidationError(f"A template named '{request.name}' already exists.")
        self.session.refresh(template)
        return self._to_response(template)

    def list_templates(self, user: User, *, limit: int = 100, offset: int = 0) -> TemplateListResponse:
        templates = self.repository.list_for_user(user.id, limit=limit, offset=offset)
        total = self.repository.count_for_user(user.id)
        template_limit = _resolve_template_limit(user.plan_tier, user.subscription_status, user.subscription_current_period_end)
        items = []
        for t in templates:
            try:
                items.append(self._to_response(t))
            except Exception:
                logger.warning("templates.invalid_template_skipped", template_id=str(t.id))
        return TemplateListResponse(
            items=items,
            total=total,
            template_limit=template_limit,
        )

    def get_template(self, user: User, template_id: UUID) -> TemplateResponse:
        template = self.repository.get_for_user(template_id, user.id)
        if template is None:
            raise NotFoundError("Template not found.")
        return self._to_response(template)

    def update(self, user: User, template_id: UUID, request: UpdateTemplateRequest) -> TemplateResponse:
        template = self.session.scalar(
            select(BacktestTemplate).where(
                BacktestTemplate.id == template_id,
                BacktestTemplate.user_id == user.id,
            ).with_for_update()
        )
        if template is None:
            raise NotFoundError("Template not found.")

        if request.expected_updated_at is not None:
            if abs((template.updated_at - request.expected_updated_at).total_seconds()) > 0.002:
                raise ConflictError(
                    "Template was modified by another request. Please refresh and try again."
                )

        changed = False
        if request.name is not None:
            template.name = request.name
            changed = True
        if request.description is not UNSET:
            template.description = request.description
            changed = True
        if request.config is not None:
            template.strategy_type = request.config.strategy_type.value
            template.config_json = request.config.model_dump(mode="json")
            changed = True
        if changed:
            template.updated_at = datetime.now(UTC)  # type: ignore[assignment]

        from sqlalchemy.exc import IntegrityError
        try:
            self.session.commit()
        except IntegrityError as exc:
            self.session.rollback()
            exc_str = str(exc.orig).lower() if exc.orig else ""
            if "unique" in exc_str or "duplicate" in exc_str or "uq_" in exc_str:
                raise ValidationError(f"A template named '{request.name or template.name}' already exists.")
            raise
        self.session.refresh(template)
        return self._to_response(template)

    def delete(self, user: User, template_id: UUID) -> None:
        template = self.repository.get_for_user(template_id, user.id, for_update=True)
        if template is None:
            raise NotFoundError("Template not found.")
        self.repository.delete(template)
        self.session.commit()

    def _enforce_template_limit(self, user: User) -> None:
        locked_user = self.session.execute(
            select(User).where(User.id == user.id).with_for_update()
        ).scalar_one_or_none()
        if locked_user is None:
            raise NotFoundError("User not found.")

        limit = _resolve_template_limit(
            locked_user.plan_tier, locked_user.subscription_status, locked_user.subscription_current_period_end,
        )
        count = self.repository.count_for_user(user.id)
        if count >= limit:
            tier = normalize_plan_tier(
                locked_user.plan_tier, locked_user.subscription_status, locked_user.subscription_current_period_end,
            )
            raise QuotaExceededError(
                f"Template limit reached. Your {tier.value} plan allows up to {limit} templates.",
                current_tier=tier.value,
            )

    @staticmethod
    def _to_response(template: BacktestTemplate) -> TemplateResponse:
        return TemplateResponse(
            id=template.id,
            name=template.name,
            description=template.description,
            strategy_type=template.strategy_type,
            config=template.config_json,
            created_at=template.created_at,
            updated_at=template.updated_at,
        )
