"""Tests for Clerk JWT verification edge cases."""
from __future__ import annotations

import time
from unittest.mock import MagicMock, patch
from uuid import uuid4

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

from backtestforecast.auth.verification import ClerkTokenVerifier
from backtestforecast.config import Settings
from backtestforecast.errors import AuthenticationError


def _generate_rsa_keypair():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return private_pem, public_pem


@pytest.fixture()
def rsa_keys():
    return _generate_rsa_keypair()


def _make_token(private_pem: bytes, claims: dict) -> str:
    now = int(time.time())
    defaults = {
        "sub": f"user_{uuid4().hex[:12]}",
        "iat": now - 10,
        "nbf": now - 10,
        "exp": now + 3600,
    }
    defaults.update(claims)
    return jwt.encode(defaults, private_pem, algorithm="RS256")


def _make_verifier(public_pem: bytes, *, authorized_parties: str = "", **kwargs):
    settings = Settings(
        app_env="test",
        clerk_jwt_key=public_pem.decode(),
        clerk_authorized_parties_raw=authorized_parties,
        **kwargs,
    )
    return ClerkTokenVerifier(settings)


class TestAzpEnforcement:
    def test_missing_azp_rejected_when_authorized_parties_set(self, rsa_keys):
        private_pem, public_pem = rsa_keys
        verifier = _make_verifier(public_pem, authorized_parties="http://localhost:3000")
        token = _make_token(private_pem, {})
        with pytest.raises(AuthenticationError, match="azp"):
            verifier.verify_bearer_token(token)

    def test_valid_azp_accepted(self, rsa_keys):
        private_pem, public_pem = rsa_keys
        verifier = _make_verifier(public_pem, authorized_parties="http://localhost:3000")
        token = _make_token(private_pem, {"azp": "http://localhost:3000"})
        principal = verifier.verify_bearer_token(token)
        assert principal.clerk_user_id is not None

    def test_wrong_azp_rejected(self, rsa_keys):
        private_pem, public_pem = rsa_keys
        verifier = _make_verifier(public_pem, authorized_parties="http://localhost:3000")
        token = _make_token(private_pem, {"azp": "http://evil.com"})
        with pytest.raises(AuthenticationError, match="authorized party"):
            verifier.verify_bearer_token(token)

    def test_no_authorized_parties_allows_missing_azp(self, rsa_keys):
        private_pem, public_pem = rsa_keys
        verifier = _make_verifier(public_pem, authorized_parties="")
        token = _make_token(private_pem, {})
        principal = verifier.verify_bearer_token(token)
        assert principal.clerk_user_id is not None


class TestExpiredAndMalformedTokens:
    def test_expired_token_raises_error(self, rsa_keys):
        """A JWT with exp in the past must raise AuthenticationError."""
        private_pem, public_pem = rsa_keys
        verifier = _make_verifier(public_pem, authorized_parties="")
        token = _make_token(private_pem, {"exp": int(time.time()) - 3600})
        with pytest.raises(AuthenticationError):
            verifier.verify_bearer_token(token)

    def test_malformed_token_raises_error(self, rsa_keys):
        """Garbage string as a token must raise AuthenticationError."""
        _private_pem, public_pem = rsa_keys
        verifier = _make_verifier(public_pem, authorized_parties="")
        with pytest.raises(AuthenticationError):
            verifier.verify_bearer_token("this-is-not-a-jwt")


class TestEmptyClerkAudienceBehavior:
    """Item 75: Test that empty CLERK_AUDIENCE is rejected in production
    (by Settings model validation) and that ClerkTokenVerifier raises
    ConfigurationError for empty audience in production-like environments,
    but only warns in development."""

    def test_empty_audience_rejected_by_settings_in_production(self, rsa_keys):
        """Settings model_validator rejects empty CLERK_AUDIENCE in production."""
        _private_pem, public_pem = rsa_keys
        from pydantic import ValidationError as PydanticValidationError

        with pytest.raises(PydanticValidationError, match="CLERK_AUDIENCE"):
            Settings(
                app_env="production",
                clerk_jwt_key=public_pem.decode(),
                clerk_secret_key="sk_test_dummy",
                clerk_authorized_parties_raw="http://localhost:3000",
                clerk_audience="",
                clerk_issuer="https://clerk.example.com",
                log_json=True,
                ip_hash_salt="a-secure-salt-for-testing-1234567890",
                metrics_token="test-metrics-token",
                redis_password="test-redis-password",
                database_url="postgresql+psycopg://u:p@localhost/db?sslmode=require",
            )

    def test_verifier_raises_for_empty_audience_in_production(self, rsa_keys):
        """Defense-in-depth: the verifier itself raises ConfigurationError
        when clerk_audience is empty in production. Simulate by constructing
        a Settings with app_env='staging' workaround bypassed."""
        private_pem, public_pem = rsa_keys
        from backtestforecast.errors import ConfigurationError
        from unittest.mock import patch

        settings = Settings(
            app_env="test",
            clerk_jwt_key=public_pem.decode(),
            clerk_authorized_parties_raw="",
            clerk_audience="",
        )
        settings.app_env = "production"

        verifier = ClerkTokenVerifier(settings)
        token = _make_token(private_pem, {"aud": "something"})
        with pytest.raises(ConfigurationError, match="CLERK_AUDIENCE must not be empty"):
            verifier.verify_bearer_token(token)

    def test_empty_audience_warns_in_development(self, rsa_keys):
        private_pem, public_pem = rsa_keys
        settings = Settings(
            app_env="development",
            clerk_jwt_key=public_pem.decode(),
            clerk_authorized_parties_raw="",
            clerk_audience="",
        )
        verifier = ClerkTokenVerifier(settings)
        token = _make_token(private_pem, {})
        principal = verifier.verify_bearer_token(token)
        assert principal.clerk_user_id is not None

    def test_none_audience_skips_check(self, rsa_keys):
        """When clerk_audience is None (not set at all), no error or warning."""
        private_pem, public_pem = rsa_keys
        settings = Settings(
            app_env="development",
            clerk_jwt_key=public_pem.decode(),
            clerk_authorized_parties_raw="",
            clerk_audience=None,
        )
        verifier = ClerkTokenVerifier(settings)
        token = _make_token(private_pem, {})
        principal = verifier.verify_bearer_token(token)
        assert principal.clerk_user_id is not None
