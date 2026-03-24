"""Placeholder for real JWT integration tests.

These tests require a valid Clerk test environment with real keys.
They are skipped by default and should be run manually against a staging
Clerk instance to verify end-to-end JWT verification.
"""
from __future__ import annotations

import pytest


@pytest.mark.skip(reason="Requires real Clerk test environment - run manually")
def test_real_jwt_verification():
    """Verify JWT parsing with real Clerk tokens."""
    # TODO: Configure CLERK_JWT_KEY or CLERK_JWKS_URL with test credentials
    # and verify that a real Clerk token is accepted and a forged token is
    # rejected.
    pass


@pytest.mark.skip(reason="Requires real Clerk test environment - run manually")
def test_expired_jwt_rejected():
    """Verify expired tokens are rejected."""
    pass
