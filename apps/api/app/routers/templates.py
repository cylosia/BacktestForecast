from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from uuid import UUID

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user, get_current_user_readonly
from backtestforecast.config import Settings, get_settings
from backtestforecast.db.session import get_db
from backtestforecast.models import User
from backtestforecast.schemas.templates import (
    CreateTemplateRequest,
    TemplateListResponse,
    TemplateResponse,
    UpdateTemplateRequest,
)
from backtestforecast.security import get_rate_limiter
from backtestforecast.services.templates import BacktestTemplateService

# Templates are always available regardless of feature flags - they store
# user configuration presets and don't consume compute or data resources.
router = APIRouter(prefix="/templates", tags=["templates"])


@contextmanager
def _template_service(db: Session) -> Generator[BacktestTemplateService, None, None]:
    svc = BacktestTemplateService(db)
    try:
        yield svc
    finally:
        if hasattr(svc, "close"):
            svc.close()


@router.get("", response_model=TemplateListResponse)
def list_templates(
    user: User = Depends(get_current_user_readonly),
    db: Session = Depends(get_db),
    limit: int = Query(default=100, ge=1, le=200),
    offset: int = Query(default=0, ge=0, le=10000),
    settings: Settings = Depends(get_settings),
) -> TemplateListResponse:
    # Derived limit: reads are cheaper than mutations so we allow 5x the mutate limit.
    get_rate_limiter().check(
        bucket="templates:read",
        actor_key=str(user.id),
        limit=settings.template_mutate_rate_limit * 5,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with _template_service(db) as service:
        return service.list_templates(user, limit=limit, offset=offset)


@router.post("", response_model=TemplateResponse, status_code=status.HTTP_201_CREATED)
def create_template(
    payload: CreateTemplateRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> TemplateResponse:
    get_rate_limiter().check(
        bucket="templates:mutate",
        actor_key=str(user.id),
        limit=settings.template_mutate_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with _template_service(db) as service:
        return service.create(user, payload)


@router.get("/{template_id}", response_model=TemplateResponse)
def get_template(
    template_id: UUID,
    user: User = Depends(get_current_user_readonly),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> TemplateResponse:
    # Derived limit: reads are cheaper than mutations so we allow 5x the mutate limit.
    get_rate_limiter().check(
        bucket="templates:read",
        actor_key=str(user.id),
        limit=settings.template_mutate_rate_limit * 5,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with _template_service(db) as service:
        return service.get_template(user, template_id)


@router.patch("/{template_id}", response_model=TemplateResponse)
def update_template(
    template_id: UUID,
    payload: UpdateTemplateRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> TemplateResponse:
    get_rate_limiter().check(
        bucket="templates:mutate",
        actor_key=str(user.id),
        limit=settings.template_mutate_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with _template_service(db) as service:
        return service.update(user, template_id, payload)


@router.delete("/{template_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_template(
    template_id: UUID,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> None:
    get_rate_limiter().check(
        bucket="templates:mutate",
        actor_key=str(user.id),
        limit=settings.template_mutate_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    with _template_service(db) as service:
        service.delete(user, template_id)
