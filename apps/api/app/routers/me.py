from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from apps.api.app.dependencies import get_current_user
from backtestforecast.db.session import get_db
from backtestforecast.models import User
from backtestforecast.schemas.backtests import CurrentUserResponse
from backtestforecast.services.backtests import BacktestService

router = APIRouter(tags=["me"])


@router.get("/me", response_model=CurrentUserResponse)
def get_me(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> CurrentUserResponse:
    return BacktestService(db).to_current_user_response(user)
