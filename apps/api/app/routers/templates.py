from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user
from backtestforecast.config import Settings, get_settings
from backtestforecast.db.session import get_db
from backtestforecast.models import User
from backtestforecast.schemas.templates import (
    CreateTemplateRequest,
    TemplateListResponse,
    TemplateResponse,
    UpdateTemplateRequest,
)
from backtestforecast.security import rate_limiter
from backtestforecast.services.templates import BacktestTemplateService

router = APIRouter(prefix="/templates", tags=["templates"])


@router.get("", response_model=TemplateListResponse)
def list_templates(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> TemplateListResponse:
    service = BacktestTemplateService(db)
    return service.list_templates(user)


@router.post("", response_model=TemplateResponse, status_code=status.HTTP_201_CREATED)
def create_template(
    payload: CreateTemplateRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> TemplateResponse:
    rate_limiter.check(
        bucket="templates:mutate",
        actor_key=str(user.id),
        limit=settings.template_mutate_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    service = BacktestTemplateService(db)
    return service.create(user, payload)


@router.get("/{template_id}", response_model=TemplateResponse)
def get_template(
    template_id: UUID,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> TemplateResponse:
    service = BacktestTemplateService(db)
    return service.get_template(user, template_id)


@router.patch("/{template_id}", response_model=TemplateResponse)
def update_template(
    template_id: UUID,
    payload: UpdateTemplateRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> TemplateResponse:
    rate_limiter.check(
        bucket="templates:mutate",
        actor_key=str(user.id),
        limit=settings.template_mutate_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    service = BacktestTemplateService(db)
    return service.update(user, template_id, payload)


@router.delete("/{template_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_template(
    template_id: UUID,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    settings: Settings = Depends(get_settings),
) -> None:
    rate_limiter.check(
        bucket="templates:mutate",
        actor_key=str(user.id),
        limit=settings.template_mutate_rate_limit,
        window_seconds=settings.rate_limit_window_seconds,
    )
    service = BacktestTemplateService(db)
    service.delete(user, template_id)
