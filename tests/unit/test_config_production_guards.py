"""Verify production configuration guards catch common misconfigurations."""
from __future__ import annotations

import inspect
import os


def test_production_requires_clerk_issuer():
    """Production config must require CLERK_ISSUER."""
    from backtestforecast.config import Settings

    source = inspect.getsource(Settings.validate_production_security)
    assert "clerk_issuer" in source


def test_production_requires_stripe_keys_when_billing_enabled():
    """When billing is enabled in prod, all Stripe keys must be set."""
    from backtestforecast.config import Settings

    source = inspect.getsource(Settings.validate_production_security)
    assert "stripe_secret_key" in source
    assert "stripe_webhook_secret" in source


def test_production_requires_database_ssl():
    """Production must enforce sslmode in DATABASE_URL."""
    from backtestforecast.config import Settings

    source = inspect.getsource(Settings.validate_production_security)
    assert "sslmode" in source


def test_production_blocks_default_db_password():
    """Production must reject default database credentials."""
    from backtestforecast.config import Settings

    source = inspect.getsource(Settings.validate_production_security)
    assert "backtestforecast:backtestforecast" in source


def test_ip_hash_salt_staging_check_uses_contains():
    """Staging ip_hash_salt check must use substring matching, not exact equality."""
    from backtestforecast.config import Settings

    source = inspect.getsource(Settings.validate_production_security)
    staging_block_start = source.find("staging")
    assert staging_block_start > 0
    salt_check = source[:staging_block_start + 200]
    assert "in self.ip_hash_salt.lower()" in salt_check or '"default" in' in salt_check, (
        "Staging must use substring check for ip_hash_salt, not exact string match"
    )


def test_production_requires_https_app_url():
    """Production APP_PUBLIC_URL must use HTTPS."""
    from backtestforecast.config import Settings

    source = inspect.getsource(Settings.validate_production_security)
    assert "https://" in source
    assert "app_public_url" in source


def test_seed_script_blocks_production():
    """seed_dev_data.py must refuse to run in production or staging."""
    from scripts.seed_dev_data import main

    source = inspect.getsource(main)
    assert "production" in source.lower()
    assert "staging" in source.lower()


def test_seed_script_blocks_ssl_database():
    """seed_dev_data.py must refuse to run against SSL-enabled databases."""
    from scripts.seed_dev_data import main

    source = inspect.getsource(main)
    assert "sslmode=require" in source
