from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any

import structlog
import jwt
from jwt import InvalidTokenError, PyJWKClient

from backtestforecast.config import Settings, get_settings
from backtestforecast.errors import AuthenticationError, ConfigurationError

_logger = structlog.get_logger("auth.verification")


@dataclass(slots=True)
class AuthenticatedPrincipal:
    clerk_user_id: str
    session_id: str | None
    email: str | None
    claims: dict[str, Any]


class ClerkTokenVerifier:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._jwks_client: PyJWKClient | None = None
        self._jwks_lock = threading.Lock()

    def verify_bearer_token(self, token: str) -> AuthenticatedPrincipal:
        signing_key = self._resolve_signing_key(token)

        decode_options: dict[str, Any] = {
            "require": ["sub", "exp", "nbf", "iat"],
        }
        audience = self.settings.clerk_audience or None
        issuer = self.settings.clerk_issuer or None
        is_prod = self.settings.app_env in ("production", "staging")
        if self.settings.clerk_audience is not None and self.settings.clerk_audience == "":
            if is_prod:
                raise ConfigurationError("CLERK_AUDIENCE must not be empty in production/staging; set a valid audience or remove the variable.")
            _logger.warning("auth.empty_clerk_audience", hint="CLERK_AUDIENCE is set to an empty string; audience verification is disabled")
        if self.settings.clerk_issuer is not None and self.settings.clerk_issuer == "":
            if is_prod:
                raise ConfigurationError("CLERK_ISSUER must not be empty in production/staging; set a valid issuer or remove the variable.")
            _logger.warning("auth.empty_clerk_issuer", hint="CLERK_ISSUER is set to an empty string; issuer verification is disabled")
        # WARNING: When clerk_audience/clerk_issuer are not set (None), audience
        # and issuer verification are disabled. In production, the lifespan
        # handler in main.py enforces that these are set. In development, tokens
        # from other Clerk applications using the same key pair will be accepted.
        if audience:
            decode_options["verify_aud"] = True
        else:
            decode_options["verify_aud"] = False
        if issuer:
            decode_options["verify_iss"] = True
        else:
            decode_options["verify_iss"] = False

        try:
            claims = jwt.decode(
                token,
                key=signing_key,
                algorithms=["RS256"],
                audience=audience,
                issuer=issuer,
                leeway=15,
                options=decode_options,
            )
        except InvalidTokenError as exc:
            raise AuthenticationError("Invalid Clerk session token.") from exc

        azp = claims.get("azp")
        if self.settings.clerk_authorized_parties:
            if not azp:
                raise AuthenticationError("Token is missing the authorized party (azp) claim.")
            if azp not in self.settings.clerk_authorized_parties:
                raise AuthenticationError("Token authorized party is not allowed.")

        subject = claims.get("sub")
        if not isinstance(subject, str) or not subject:
            raise AuthenticationError("Token subject is missing.")

        email: str | None = None
        raw_email = claims.get("email") or claims.get("primary_email_address")
        if isinstance(raw_email, str) and raw_email:
            email = raw_email

        session_id = claims.get("sid") if isinstance(claims.get("sid"), str) else None

        return AuthenticatedPrincipal(
            clerk_user_id=subject,
            session_id=session_id,
            email=email,
            claims=claims,
        )

    def _resolve_signing_key(self, token: str) -> Any:
        if self.settings.clerk_jwt_key:
            key = self.settings.clerk_jwt_key
            if isinstance(key, str) and key.strip().startswith("-----BEGIN"):
                if "-----END" not in key:
                    raise ConfigurationError(
                        "CLERK_JWT_KEY appears to be a truncated PEM key. "
                        "Ensure the full key including the END marker is provided."
                    )
            return key

        jwks_client = self._get_jwks_client()
        try:
            signing_key = jwks_client.get_signing_key_from_jwt(token)
        except Exception as exc:  # pragma: no cover - external lib surface
            raise AuthenticationError("Unable to resolve Clerk signing key.") from exc
        return signing_key.key

    def _get_jwks_client(self) -> PyJWKClient:
        if self._jwks_client is not None:
            return self._jwks_client

        with self._jwks_lock:
            if self._jwks_client is not None:
                return self._jwks_client

            jwks_url = self.settings.clerk_jwks_url
            if not jwks_url:
                if self.settings.clerk_issuer:
                    issuer = self.settings.clerk_issuer.rstrip("/")
                    jwks_url = f"{issuer}/.well-known/jwks.json"
                else:
                    raise ConfigurationError("Set CLERK_JWT_KEY or CLERK_JWKS_URL (or CLERK_ISSUER) to enable auth.")

            self._jwks_client = PyJWKClient(
                jwks_url, timeout=10, cache_jwk_set=True, lifespan=300,
            )
            return self._jwks_client

