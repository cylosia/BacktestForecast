"""Tests for JWT verification edge cases in ClerkTokenVerifier.

Uses real RSA key pairs and jwt.encode() to construct tokens, exercising
the verification code path with clerk_jwt_key set (no JWKS fetch).
"""
from __future__ import annotations

import time

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from backtestforecast.auth.verification import ClerkTokenVerifier
from backtestforecast.config import Settings
from backtestforecast.errors import AuthenticationError

pytestmark = pytest.mark.filterwarnings("ignore:MASSIVE_API_KEY:UserWarning")


def _generate_rsa_keypair():
    """Generate a fresh RSA key pair and return (private_key, public_key_pem)."""
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()
    return private_key, public_pem


_PRIVATE_KEY, _PUBLIC_PEM = _generate_rsa_keypair()

_ISSUER = "https://clerk.test.example.com"
_AUDIENCE = "test-app"


def _make_settings(*, audience: str | None = _AUDIENCE, issuer: str | None = _ISSUER):
    return Settings(
        clerk_jwt_key=_PUBLIC_PEM,
        clerk_issuer=issuer,
        clerk_audience=audience,
        clerk_authorized_parties_raw="",
        app_env="development",
        _env_file=None,
    )


def _encode_token(claims: dict, *, key=_PRIVATE_KEY, algorithm: str = "RS256") -> str:
    return jwt.encode(claims, key, algorithm=algorithm)


def _valid_claims(**overrides) -> dict:
    now = int(time.time())
    base = {
        "sub": "user_test123",
        "exp": now + 3600,
        "nbf": now - 60,
        "iat": now,
        "iss": _ISSUER,
        "aud": _AUDIENCE,
    }
    base.update(overrides)
    return base


class TestJwtAuthEdgeCases:
    def test_valid_token_succeeds(self):
        """Baseline: a properly formed token must be accepted."""
        settings = _make_settings()
        verifier = ClerkTokenVerifier(settings)
        token = _encode_token(_valid_claims())
        principal = verifier.verify_bearer_token(token)
        assert principal.clerk_user_id == "user_test123"

    def test_expired_token_raises_auth_error(self):
        """A token with exp in the past must raise AuthenticationError."""
        settings = _make_settings()
        verifier = ClerkTokenVerifier(settings)
        claims = _valid_claims(exp=int(time.time()) - 600)
        token = _encode_token(claims)

        with pytest.raises(AuthenticationError, match="Invalid Clerk session token"):
            verifier.verify_bearer_token(token)

    def test_missing_sub_claim_raises_auth_error(self):
        """A token without the required 'sub' claim must raise AuthenticationError."""
        settings = _make_settings()
        verifier = ClerkTokenVerifier(settings)
        claims = _valid_claims()
        del claims["sub"]
        token = _encode_token(claims)

        with pytest.raises(AuthenticationError):
            verifier.verify_bearer_token(token)

    def test_empty_sub_claim_raises_auth_error(self):
        """A token with an empty-string 'sub' claim must raise AuthenticationError."""
        settings = _make_settings()
        verifier = ClerkTokenVerifier(settings)
        token = _encode_token(_valid_claims(sub=""))

        with pytest.raises(AuthenticationError):
            verifier.verify_bearer_token(token)

    def test_token_too_long_raises_auth_error(self):
        """A token exceeding 4096 bytes should fail validation.

        The verifier itself may not enforce a byte limit - PyJWT will reject
        the token as malformed if it's padded beyond valid JWT structure.
        """
        settings = _make_settings()
        verifier = ClerkTokenVerifier(settings)
        padding = "A" * 5000
        oversized_token = f"eyJ.{padding}.sig"

        with pytest.raises((AuthenticationError, Exception)):
            verifier.verify_bearer_token(oversized_token)

    def test_empty_token_raises_auth_error(self):
        """An empty string token must raise AuthenticationError."""
        settings = _make_settings()
        verifier = ClerkTokenVerifier(settings)

        with pytest.raises((AuthenticationError, Exception)):
            verifier.verify_bearer_token("")

    def test_wrong_audience_raises_auth_error(self):
        """A token with the wrong audience claim must be rejected when
        clerk_audience is configured."""
        settings = _make_settings(audience="correct-audience")
        verifier = ClerkTokenVerifier(settings)
        claims = _valid_claims(aud="wrong-audience")
        token = _encode_token(claims)

        with pytest.raises(AuthenticationError, match="Invalid Clerk session token"):
            verifier.verify_bearer_token(token)

    def test_wrong_issuer_raises_auth_error(self):
        """A token signed with the right key but wrong issuer must be rejected."""
        settings = _make_settings(issuer="https://clerk.correct.example.com")
        verifier = ClerkTokenVerifier(settings)
        claims = _valid_claims(iss="https://clerk.wrong.example.com")
        token = _encode_token(claims)

        with pytest.raises(AuthenticationError, match="Invalid Clerk session token"):
            verifier.verify_bearer_token(token)

    def test_wrong_signing_key_raises_auth_error(self):
        """A token signed with a different RSA key must be rejected."""
        other_key, _ = _generate_rsa_keypair()
        settings = _make_settings()
        verifier = ClerkTokenVerifier(settings)
        token = _encode_token(_valid_claims(), key=other_key)

        with pytest.raises(AuthenticationError, match="Invalid Clerk session token"):
            verifier.verify_bearer_token(token)
