from __future__ import annotations

import ipaddress
import threading
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

_token_verifier: ClerkTokenVerifier | None = None
_verifier_lock = threading.Lock()


def get_token_verifier() -> ClerkTokenVerifier:
    global _token_verifier
    if _token_verifier is None:
        with _verifier_lock:
            if _token_verifier is None:
                _token_verifier = ClerkTokenVerifier()
    return _token_verifier


def reset_token_verifier() -> None:
    """Clear the cached verifier so it is recreated on next use."""
    global _token_verifier
    with _verifier_lock:
        _token_verifier = None


_trusted_networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] | None = None
_trusted_networks_lock = threading.Lock()


def _get_trusted_networks() -> list[ipaddress.IPv4Network | ipaddress.IPv6Network]:
    global _trusted_networks
    if _trusted_networks is None:
        with _trusted_networks_lock:
            if _trusted_networks is None:
                raw = get_settings().trusted_proxy_cidrs
                entries = [cidr.strip() for cidr in raw.split(",") if cidr.strip()]
                _trusted_networks = [ipaddress.ip_network(cidr, strict=False) for cidr in entries]
    return _trusted_networks


def reset_trusted_networks() -> None:
    """Clear cached networks so they are re-read from settings on next call."""
    global _trusted_networks
    with _trusted_networks_lock:
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
    """Extract the real client IP using the rightmost-untrusted approach.

    Walks the X-Forwarded-For chain from right to left, skipping entries
    that belong to trusted proxies.  The first non-trusted entry is the
    actual client IP.  This prevents spoofing via a forged leftmost entry.
    """
    direct_host = request.client.host if request.client is not None else None
    if _is_trusted_proxy(direct_host):
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            parts = [p.strip() for p in forwarded_for.split(",") if p.strip()]
            for candidate in reversed(parts):
                validated = _validate_ip(candidate)
                if validated and not _is_trusted_proxy(validated):
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


def _normalize_origin(value: str) -> str:
    """Normalize an origin for comparison: lowercase, strip trailing slashes,
    and remove default ports (:443 for https, :80 for http)."""
    v = value.strip().lower().rstrip("/")
    if v.startswith("https://") and v.endswith(":443"):
        v = v[:-4]
    elif v.startswith("http://") and v.endswith(":80"):
        v = v[:-3]
    return v


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
        if token and request.method in {"POST", "PATCH", "DELETE"}:
            xrw = request.headers.get("x-requested-with")
            if not xrw or xrw.strip().lower() != "xmlhttprequest":
                raise AuthenticationError(
                    "Cookie-based authentication requires the X-Requested-With: XMLHttpRequest header for state-changing requests."
                )
        if token:
            origin = request.headers.get("origin")
            if origin:
                normalized_origin = _normalize_origin(origin)
                allowed_origins = [_normalize_origin(o) for o in get_settings().web_cors_origins]
                if normalized_origin not in allowed_origins:
                    raise AuthenticationError(
                        "Cookie-based request origin not in allowed list."
                    )

    if not token:
        raise AuthenticationError()

    if len(token) > 8192:
        raise AuthenticationError("Token too large.")

    principal = get_token_verifier().verify_bearer_token(token)
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
