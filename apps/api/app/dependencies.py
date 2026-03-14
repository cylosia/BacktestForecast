from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from typing import Annotated

import structlog
from fastapi import Depends, Header, Request
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backtestforecast.auth.verification import ClerkTokenVerifier
from backtestforecast.config import get_settings
from backtestforecast.db.session import get_db
from backtestforecast.errors import AuthenticationError
from backtestforecast.models import User
from backtestforecast.repositories.users import UserRepository

token_verifier = ClerkTokenVerifier()

_trusted_networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] | None = None


def _get_trusted_networks() -> list[ipaddress.IPv4Network | ipaddress.IPv6Network]:
    global _trusted_networks
    if _trusted_networks is None:
        raw = get_settings().trusted_proxy_cidrs
        entries = [cidr.strip() for cidr in raw.split(",") if cidr.strip()]
        _trusted_networks = [ipaddress.ip_network(cidr, strict=False) for cidr in entries]
    return _trusted_networks


def reset_trusted_networks() -> None:
    """Clear cached networks so they are re-read from settings on next call."""
    global _trusted_networks
    _trusted_networks = None


def _is_trusted_proxy(host: str | None) -> bool:
    if not host:
        return False
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return False
    return any(addr in network for network in _get_trusted_networks())


@dataclass(slots=True)
class RequestMetadata:
    request_id: str | None
    ip_address: str | None


def _validate_ip(value: str) -> str | None:
    """Return the IP string if valid, otherwise None."""
    try:
        ipaddress.ip_address(value)
        return value
    except ValueError:
        return None


def _extract_client_ip(request: Request) -> str | None:
    direct_host = request.client.host if request.client is not None else None
    if _is_trusted_proxy(direct_host):
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            first = forwarded_for.split(",", maxsplit=1)[0].strip()
            if first:
                validated = _validate_ip(first)
                if validated:
                    return validated
        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            stripped = real_ip.strip()
            if stripped:
                validated = _validate_ip(stripped)
                if validated:
                    return validated
    return direct_host


def get_request_metadata(request: Request) -> RequestMetadata:
    request_id = getattr(request.state, "request_id", None)
    return RequestMetadata(request_id=request_id, ip_address=_extract_client_ip(request))


def get_current_user(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
    db: Session = Depends(get_db),
) -> User:
    """Authenticate via Bearer token (primary) or ``__session`` cookie (fallback).

    Cookie auth exists for Clerk's server-side rendering flow, where the JWT
    is placed in the ``__session`` cookie rather than an Authorization header.
    State-changing methods require ``X-Requested-With`` to mitigate CSRF when
    using cookie auth.  This is intentional and should not be removed without
    replacing the SSR auth flow.
    """
    token: str | None = None
    if authorization:
        scheme, _, candidate = authorization.partition(" ")
        if scheme.lower() != "bearer" or not candidate:
            raise AuthenticationError("Bearer token is required.")
        token = candidate
    else:
        # The __session cookie is set by Clerk with SameSite=Lax by default.
        # If Clerk configuration changes, verify SameSite is at least Lax
        # to prevent cross-site cookie attachment in sub-requests.
        token = request.cookies.get("__session")
        if token and request.method in {"POST", "PUT", "PATCH", "DELETE"}:
            xrw = request.headers.get("x-requested-with")
            if not xrw or xrw.strip().lower() != "xmlhttprequest":
                raise AuthenticationError(
                    "Cookie-based authentication requires the X-Requested-With: XMLHttpRequest header for state-changing requests."
                )
        if token:
            origin = request.headers.get("origin")
            if origin:
                allowed_origins = [o.strip() for o in get_settings().web_cors_origins_raw.split(",") if o.strip()]
                if origin not in allowed_origins:
                    raise AuthenticationError(
                        "Cookie-based request origin not in allowed list."
                    )

    if not token:
        raise AuthenticationError()

    # NOTE: verify_bearer_token may perform an HTTP fetch to the Clerk JWKS
    # endpoint via PyJWKClient. That client does not expose a request-level
    # timeout knob; its default urllib timeout applies.  If latency becomes a
    # concern, configure PyJWKClient with a custom ``urllib.request.Request``
    # or switch to an httpx-based JWKS fetcher with explicit timeouts.
    principal = token_verifier.verify_bearer_token(token)
    repository = UserRepository(db)
    user = repository.get_or_create(principal.clerk_user_id, principal.email)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        user = repository.get_by_clerk_user_id(principal.clerk_user_id)
        if user is None:
            raise
    db.refresh(user)
    structlog.contextvars.bind_contextvars(user_id=str(user.id), clerk_user_id=user.clerk_user_id)
    return user


def require_authenticated_user(user: User = Depends(get_current_user)) -> str:
    return str(user.id)
