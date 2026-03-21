from typing import Any

import jwt.exceptions
import structlog
from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from sqlalchemy.exc import DatabaseError
from sqlalchemy.orm import Session
from starlette.exceptions import HTTPException as StarletteHTTPException

from apps.api.app.dependencies import _extract_client_ip, get_token_verifier
from backtestforecast.config import get_settings
from backtestforecast.db.session import get_readonly_db
from backtestforecast.errors import AuthenticationError
from backtestforecast.models import User
from backtestforecast.repositories.users import UserRepository
from backtestforecast.security import get_rate_limiter
from backtestforecast.version import get_public_version


class FeatureFlagsResponse(BaseModel):
    backtests: bool = True
    scanner: bool = True
    exports: bool = True
    forecasts: bool = True
    analysis: bool = True
    daily_picks: bool = True
    billing: bool = True
    sweeps: bool = True


class MetaResponse(BaseModel):
    service: str
    version: str
    billing_enabled: bool | None = None
    features: FeatureFlagsResponse | None = None
    environment: str | None = None

router = APIRouter(tags=["meta"])
logger = structlog.get_logger("api.meta")


def _try_authenticate(request: Request, db: Session) -> User | None:
    """Verify JWT and look up the user without creating a new record.

    Unlike ``get_current_user``, this avoids ``get_or_create`` so that
    presenting a valid JWT on this unauthenticated endpoint does not
    produce user-record side-effects.  If the user hasn't been created
    yet (e.g. first API visit), we simply return None and omit the
    authenticated metadata from the response.
    """
    try:
        token: str | None = None
        authorization = request.headers.get("authorization")
        if authorization:
            scheme, _, candidate = authorization.partition(" ")
            if scheme.lower() == "bearer" and candidate:
                token = candidate
        if not token:
            token = request.cookies.get("__session")
        if not token or len(token) > 4096:
            return None
        principal = get_token_verifier().verify_bearer_token(token)
        repo = UserRepository(db)
        return repo.get_by_clerk_user_id(principal.clerk_user_id)
    except (jwt.exceptions.PyJWTError, ValueError, KeyError, AttributeError, StarletteHTTPException, AuthenticationError):
        return None
    except (DatabaseError, ConnectionError, OSError):
        raise
    except Exception:
        logger.warning("meta.auth_unexpected_error", exc_info=True)
        return None


@router.get("/meta", response_model=MetaResponse)
def get_meta(request: Request, db: Session = Depends(get_readonly_db)) -> dict[str, Any]:
    settings = get_settings()
    client_ip = _extract_client_ip(request)
    get_rate_limiter().check(
        bucket="meta:read",
        actor_key=client_ip or "unknown",
        limit=120,
        window_seconds=settings.rate_limit_window_seconds,
    )
    result: dict[str, Any] = {
        "service": "backtestforecast-api",
        "version": get_public_version(),
    }
    try:
        user = _try_authenticate(request, db)
    except (DatabaseError, ConnectionError, OSError):
        logger.warning("meta.auth_degraded_db_unavailable", exc_info=True)
        user = None
    if user is not None:
        result["billing_enabled"] = settings.stripe_billing_enabled
        result["features"] = {
            "backtests": settings.feature_backtests_enabled,
            "scanner": settings.feature_scanner_enabled,
            "exports": settings.feature_exports_enabled,
            "forecasts": settings.feature_forecasts_enabled,
            "analysis": settings.feature_analysis_enabled,
            "daily_picks": settings.feature_daily_picks_enabled,
            "billing": settings.feature_billing_enabled,
            "sweeps": settings.feature_sweeps_enabled,
        }
    if settings.app_env not in ("production", "staging"):
        result["environment"] = settings.app_env
    return result
