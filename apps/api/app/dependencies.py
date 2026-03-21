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
from backtestforecast.db.session import create_session, get_db, get_readonly_db
from backtestforecast.errors import AuthenticationError
from backtestforecast.models import User
from backtestforecast.observability.logging import short_hash
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


def _get_trusted_networks() -> list[ipaddress.IPv4Network | ipaddress.IPv6Network]:
    raw = get_settings().trusted_proxy_cidrs
    entries = [cidr.strip() for cidr in raw.split(",") if cidr.strip()]
    return [ipaddress.ip_network(cidr, strict=False) for cidr in entries]


def reset_trusted_networks() -> None:
    """Compatibility hook for config invalidation callbacks."""
    return None


def _is_trusted_proxy(host: str | None) -> bool:
    if not host:
        return False
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return False
    return any(addr in network for network in _get_trusted_networks())


@dataclass(frozen=True, slots=True)
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
    that belong to trusted proxies. The first non-trusted entry is the
    actual client IP. This prevents spoofing via a forged leftmost entry.
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
    """Normalize an origin for comparison.

    Delegates to the canonical implementation in the security module.
    """
    from backtestforecast.security.http import normalize_origin

    return normalize_origin(value)


def _get_allowed_origins() -> list[str]:
    """Return the current normalized CORS-origin allowlist."""
    return [_normalize_origin(origin) for origin in get_settings().web_cors_origins]


def _bind_user_context(user: User) -> None:
    structlog.contextvars.bind_contextvars(
        user_id=str(user.id),
        clerk_user_id_hash=short_hash(user.clerk_user_id),
    )


def _resolve_current_user(
    request: Request,
    *,
    authorization: str | None,
    db: Session,
    allow_write_fallback: bool,
) -> User:
    """Authenticate via Bearer token (primary) or ``__session`` cookie (fallback).

    Cookie auth exists for Clerk's server-side rendering flow, where the JWT
    is placed in the ``__session`` cookie rather than an Authorization header.
    State-changing methods require ``X-Requested-With`` to mitigate CSRF when
    using cookie auth. This is intentional and should not be removed without
    replacing the SSR auth flow.
    """
    token: str | None = None
    if authorization:
        scheme, _, candidate = authorization.partition(" ")
        if scheme.lower() != "bearer" or not candidate:
            raise AuthenticationError("Bearer token is required.")
        token = candidate
    else:
        token = request.cookies.get("__session")
        if token:
            fetch_site = request.headers.get("sec-fetch-site")
            if fetch_site and fetch_site not in ("same-origin", "same-site", "none"):
                structlog.get_logger("security").warning(
                    "auth.cookie_cross_site_rejected",
                    sec_fetch_site=fetch_site,
                    method=request.method,
                )
                raise AuthenticationError(
                    "Cookie-based authentication is not allowed from cross-site contexts."
                )
            fetch_dest = request.headers.get("sec-fetch-dest")
            if fetch_dest and fetch_dest in ("document", "iframe", "embed", "object"):
                structlog.get_logger("security").warning(
                    "auth.cookie_navigation_rejected",
                    sec_fetch_dest=fetch_dest,
                    method=request.method,
                )
                raise AuthenticationError(
                    "Cookie-based authentication is not allowed for navigation requests."
                )
        if token and request.method in {"POST", "PUT", "PATCH", "DELETE"}:
            xrw = request.headers.get("x-requested-with")
            if not xrw or xrw.strip().lower() != "xmlhttprequest":
                raise AuthenticationError(
                    "Cookie-based authentication requires the X-Requested-With: XMLHttpRequest header for state-changing requests."
                )
        if token:
            allowed_origins = _get_allowed_origins()
            origin = request.headers.get("origin")
            if origin:
                if _normalize_origin(origin) not in allowed_origins:
                    structlog.get_logger("security").warning(
                        "auth.cookie_origin_rejected",
                        origin=_normalize_origin(origin),
                    )
                    raise AuthenticationError(
                        "Cookie-based request origin not in allowed list."
                    )
            else:
                referer = request.headers.get("referer")
                if referer:
                    from urllib.parse import urlparse as _urlparse

                    referer_origin = _normalize_origin(
                        f"{_urlparse(referer).scheme}://{_urlparse(referer).netloc}"
                    )
                    if referer_origin and referer_origin not in allowed_origins:
                        structlog.get_logger("security").warning(
                            "auth.cookie_referer_rejected",
                            referer_origin=referer_origin,
                            method=request.method,
                        )
                        raise AuthenticationError(
                            "Cookie-based request referer not in allowed list."
                        )
                elif request.method in {"POST", "PUT", "PATCH", "DELETE"}:
                    structlog.get_logger("security").warning(
                        "auth.cookie_no_origin_or_referer",
                        method=request.method,
                        path=request.url.path,
                    )
                    raise AuthenticationError(
                        "Cookie-based state-changing requests must include an Origin or Referer header."
                    )

    if not token:
        raise AuthenticationError()

    if len(token) > 4096:
        raise AuthenticationError("Token too large.")

    principal = get_token_verifier().verify_bearer_token(token)
    repository = UserRepository(db)
    user = repository.get_by_clerk_user_id(principal.clerk_user_id)
    email_needs_sync = bool(user is not None and repository.sync_email_if_needed(user, principal.email))
    if user is not None and not email_needs_sync:
        _bind_user_context(user)
        return user

    if not allow_write_fallback:
        if user is None:
            raise AuthenticationError("User account not initialized.")
        _bind_user_context(user)
        return user

    with create_session() as write_db:
        write_repository = UserRepository(write_db)
        user = write_repository.get_by_clerk_user_id(principal.clerk_user_id)
        if user is None:
            user = write_repository.get_or_create(principal.clerk_user_id, principal.email)
        elif not write_repository.sync_email_if_needed(user, principal.email):
            _bind_user_context(user)
            return user
        try:
            write_db.commit()
        except IntegrityError:
            write_db.rollback()
            user = write_repository.get_by_clerk_user_id(principal.clerk_user_id)
            if user is None:
                raise
        write_db.refresh(user)
    _bind_user_context(user)
    return user


def get_current_user(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
    db: Session = Depends(get_db),
) -> User:
    return _resolve_current_user(
        request,
        authorization=authorization,
        db=db,
        allow_write_fallback=True,
    )


def get_current_user_readonly(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
    db: Session = Depends(get_readonly_db),
) -> User:
    return _resolve_current_user(
        request,
        authorization=authorization,
        db=db,
        allow_write_fallback=False,
    )
