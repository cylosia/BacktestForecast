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
