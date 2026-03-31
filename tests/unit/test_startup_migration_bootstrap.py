from __future__ import annotations

from types import SimpleNamespace

import pytest


@pytest.mark.asyncio
async def test_lifespan_blocks_production_when_migration_head_resolution_fails(monkeypatch) -> None:
    from apps.api.app import main as main_module
    from backtestforecast.db import session as session_module

    monkeypatch.setattr(
        main_module,
        "_startup_settings",
        SimpleNamespace(
            app_env="production",
            clerk_audience="audience",
            clerk_issuer="https://clerk.example.com",
            admin_token="admin-token",
            clerk_authorized_parties=("frontend-client",),
            feature_exports_enabled=False,
            s3_bucket=None,
            web_cors_origins=("https://app.example.com",),
            api_allowed_hosts=("app.example.com",),
            clerk_jwks_url="",
            sentry_dsn=None,
        ),
    )
    monkeypatch.setattr(session_module, "get_missing_schema_tables", lambda: ())
    monkeypatch.setattr(
        session_module,
        "get_migration_status",
        lambda: {
            "aligned": False,
            "applied_revision": "20260330_0013",
            "expected_revision": None,
            "error": "ModuleNotFoundError: demo",
        },
    )
    monkeypatch.setattr(
        session_module,
        "get_database_timezones",
        lambda: {"session_timezone": "UTC", "server_timezone": "UTC"},
    )
    monkeypatch.setattr(main_module, "_register_startup_invalidation_callbacks", lambda: None)

    lifespan = main_module._lifespan(main_module.app)

    with pytest.raises(RuntimeError, match="Unable to resolve Alembic head for readiness verification"):
        await lifespan.__aenter__()
