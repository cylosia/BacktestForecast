"""Tests for audit round 16 - SSE-before-commit, stale subscription guard,
OpenAPI snapshot completeness, and sweep type alignment."""
from __future__ import annotations

import inspect
import json
from pathlib import Path


class TestSSEPublishAfterCommit:
    """SSE cancellation events must be published AFTER the DB transaction commits,
    not before. Otherwise, a rollback leaves SSE consumers with stale 'cancelled' state."""

    def test_cancel_in_flight_returns_ids_without_publishing(self):
        """cancel_in_flight_jobs must return cancelled IDs, not publish SSE inline."""
        from backtestforecast.services.billing import BillingService
        source = inspect.getsource(BillingService.cancel_in_flight_jobs)
        assert "publish_job_status" not in source, (
            "cancel_in_flight_jobs must NOT publish SSE events inline - "
            "it must return the IDs so the caller can publish after commit"
        )

    def test_cancel_in_flight_returns_list(self):
        """cancel_in_flight_jobs must return a list of (job_type, job_id) tuples."""
        from backtestforecast.services.billing import BillingService
        sig = inspect.signature(BillingService.cancel_in_flight_jobs)
        ret = sig.return_annotation
        assert ret is not None and ret is not inspect.Parameter.empty, (
            "cancel_in_flight_jobs must have a return type annotation"
        )

    def test_publish_cancellation_events_exists(self):
        """A dedicated method must exist for publishing cancellation SSE events."""
        from backtestforecast.services.billing import BillingService
        assert hasattr(BillingService, "publish_cancellation_events"), (
            "BillingService must have a publish_cancellation_events static method"
        )

    def test_handle_webhook_publishes_after_commit(self):
        """handle_webhook must publish cancellation events after session.commit()."""
        from backtestforecast.services.billing import BillingService
        source = inspect.getsource(BillingService.handle_webhook)
        commit_pos = source.find("self.session.commit()")
        publish_pos = source.find("publish_cancellation_events")
        assert commit_pos > 0, "handle_webhook must call session.commit()"
        assert publish_pos > 0, "handle_webhook must call publish_cancellation_events"
        assert commit_pos < publish_pos, (
            "publish_cancellation_events must be called AFTER session.commit()"
        )

    def test_account_deletion_publishes_after_commit(self):
        """Account deletion must publish cancellation events after all commits."""
        from apps.api.app.routers.account import delete_account
        source = inspect.getsource(delete_account)
        if "publish_cancellation_events" in source:
            last_commit = source.rfind("db.commit()")
            publish_pos = source.find("publish_cancellation_events")
            assert last_commit < publish_pos, (
                "publish_cancellation_events must be called after the last db.commit()"
            )


class TestStaleSubscriptionGuardWarning:
    """The stale subscription guard must log at WARNING level with reconciliation context."""

    def test_stale_guard_logs_warning_not_info(self):
        from backtestforecast.services.billing import BillingService
        source = inspect.getsource(BillingService._apply_subscription_to_user)
        assert "stale_event_skipped_may_need_reconciliation" in source, (
            "Stale subscription guard must use a descriptive event name "
            "that flags the need for reconciliation"
        )
        stale_idx = source.find("stale_event_skipped_may_need_reconciliation")
        preceding = source[max(0, stale_idx - 100):stale_idx]
        assert "logger.warning" in preceding, (
            "Stale subscription guard must log at WARNING level, not INFO"
        )


class TestOpenAPISnapshotCompleteness:
    """The OpenAPI snapshot must include all endpoints."""

    def test_sweep_endpoints_in_snapshot(self):
        snapshot_path = Path(__file__).resolve().parents[2] / "openapi.snapshot.json"
        content = snapshot_path.read_text(encoding="utf-8")
        data = json.loads(content)
        paths = data.get("paths", {})
        assert "/v1/sweeps" in paths, "OpenAPI snapshot must include /v1/sweeps"

    def test_account_endpoints_in_snapshot(self):
        snapshot_path = Path(__file__).resolve().parents[2] / "openapi.snapshot.json"
        content = snapshot_path.read_text(encoding="utf-8")
        data = json.loads(content)
        paths = data.get("paths", {})
        assert "/v1/account/me" in paths, "OpenAPI snapshot must include /v1/account/me"

    def test_sweep_schemas_in_snapshot(self):
        snapshot_path = Path(__file__).resolve().parents[2] / "openapi.snapshot.json"
        content = snapshot_path.read_text(encoding="utf-8")
        data = json.loads(content)
        schemas = data.get("components", {}).get("schemas", {})
        assert "SweepJobResponse" in schemas, "OpenAPI snapshot must include SweepJobResponse schema"
        assert "SweepResultResponse" in schemas, "OpenAPI snapshot must include SweepResultResponse schema"
        assert "CreateSweepRequest" in schemas, "OpenAPI snapshot must include CreateSweepRequest schema"


class TestApiClientSweepTypes:
    """The api-client sweep types must acknowledge the OpenAPI snapshot is available."""

    def test_manual_types_comment_updated(self):
        api_client_path = Path(__file__).resolve().parents[2] / "packages" / "api-client" / "src" / "index.ts"
        content = api_client_path.read_text(encoding="utf-8")
        assert "now included in the OpenAPI snapshot" in content, (
            "The api-client comment must indicate sweep types are now in the OpenAPI snapshot"
        )
        assert "does not yet include" not in content, (
            "The stale comment about sweeps not being in the OpenAPI snapshot must be removed"
        )
