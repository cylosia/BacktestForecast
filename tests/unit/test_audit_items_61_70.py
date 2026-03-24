from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from backtestforecast.db import session as db_session

ROOT = Path(__file__).resolve().parents[2]


def _read(relpath: str) -> str:
    return (ROOT / relpath).read_text(encoding="utf-8")


class TestReadReplicaSessionSelection:
    def test_create_readonly_session_falls_back_to_primary_when_replica_unconfigured(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        primary_session = MagicMock(name="primary_session")
        monkeypatch.setattr(db_session, "_get_readonly_session_factory", lambda: None)
        monkeypatch.setattr(db_session, "create_session", lambda: primary_session)

        assert db_session.create_readonly_session() is primary_session

    def test_create_readonly_session_prefers_replica_factory_when_available(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        replica_session = MagicMock(name="replica_session")
        replica_factory = MagicMock(name="replica_factory", return_value=replica_session)
        create_session = MagicMock(name="create_session")

        monkeypatch.setattr(db_session, "_get_readonly_session_factory", lambda: replica_factory)
        monkeypatch.setattr(db_session, "create_session", create_session)

        assert db_session.create_readonly_session() is replica_session
        replica_factory.assert_called_once_with()
        create_session.assert_not_called()


class TestRuntimeReloadableMiddlewareContracts:
    def test_runtime_http_policy_is_resolved_via_per_request_resolvers(self) -> None:
        source = _read("apps/api/app/main.py")

        assert "app.add_middleware(ApiSecurityHeadersMiddleware, app_env_resolver=lambda: _get_runtime_http_policy().app_env)" in source
        assert "app.add_middleware(RequestBodyLimitMiddleware, max_body_bytes=lambda: _get_runtime_http_policy().request_max_body_bytes)" in source
        assert "allow_origins=lambda: _get_runtime_http_policy().cors_origins," in source
        assert "app.add_middleware(DynamicTrustedHostMiddleware, allowed_hosts=lambda: _get_runtime_http_policy().trusted_hosts)" in source
        assert "_startup_settings is reserved for process-start concerns" in source


class TestVersionStringCentralization:
    def test_backend_public_version_flows_from_shared_helper(self) -> None:
        assert 'version=get_public_version()' in _read("apps/api/app/main.py")
        assert 'resp["version"] = get_public_version()' in _read("apps/api/app/routers/health.py")
        meta_source = _read("apps/api/app/routers/meta.py")
        assert '"version": get_public_version(),' in meta_source
        assert 'from backtestforecast import __version__' in _read("src/backtestforecast/version.py")

    def test_web_health_version_flows_from_package_json(self) -> None:
        build_info = _read("apps/web/lib/build-info.ts")
        assert 'import packageJson from "@/package.json";' in build_info
        assert 'export const WEB_PACKAGE_VERSION = packageJson.version;' in build_info

        health_route = _read("apps/web/app/api/health/route.ts")
        assert 'version: WEB_PACKAGE_VERSION' in health_route


class TestPaginationPlumbingContracts:
    def test_server_fetch_helpers_use_shared_pagination_builders(self) -> None:
        source = _read("apps/web/lib/api/server.ts")

        for expected in (
            'buildPaginatedListPath("/v1/backtests", limit, offset, 100, cursor)',
            'buildPaginatedListPath("/v1/scans", limit, offset, 50, cursor)',
            'buildPaginatedListPath("/v1/sweeps", limit, offset, 50, cursor)',
            'buildPaginatedListPath("/v1/analysis", limit, offset, 50, cursor)',
            'buildCursorPaginatedPath("/v1/daily-picks/history", limit, 30, cursor)',
        ):
            assert expected in source

    def test_daily_picks_page_maps_next_cursor_url_param_back_to_backend_cursor_param(self) -> None:
        source = _read("apps/web/app/app/daily-picks/page.tsx")

        assert 'const cursor = params.next_cursor?.trim() || params.cursor?.trim() || undefined;' in source
        assert 'cursorParamName="next_cursor"' in source
        assert "getDailyPicksHistory(HISTORY_PAGE_SIZE, cursor)" in source
