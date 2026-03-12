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
        _trusted_networks = [
            ipaddress.ip_network(cidr.strip(), strict=False) for cidr in raw.split(",") if cidr.strip()
        ]
    return _trusted_networks


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


def _extract_client_ip(request: Request) -> str | None:
    direct_host = request.client.host if request.client is not None else None
    if _is_trusted_proxy(direct_host):
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            first = forwarded_for.split(",", maxsplit=1)[0].strip()
            if first:
                return first
        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip.strip() or None
    return direct_host


def get_request_metadata(request: Request) -> RequestMetadata:
    request_id = getattr(request.state, "request_id", None)
    return RequestMetadata(request_id=request_id, ip_address=_extract_client_ip(request))


def get_current_user(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
    db: Session = Depends(get_db),
) -> User:
    token: str | None = None
    if authorization:
        scheme, _, candidate = authorization.partition(" ")
        if scheme.lower() != "bearer" or not candidate:
            raise AuthenticationError("Bearer token is required.")
        token = candidate
    else:
        token = request.cookies.get("__session")

    if not token:
        raise AuthenticationError()

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
