"""Verify export cleanup counts DB cleanups separately from storage."""
from __future__ import annotations
from unittest.mock import MagicMock, patch, PropertyMock
from types import SimpleNamespace


class TestExportCleanupCounting:
    def test_cleanup_counts_all_db_cleanups_despite_storage_failures(self):
        from backtestforecast.services.exports import ExportService

        service = ExportService.__new__(ExportService)
        service.session = MagicMock()
        service.exports = MagicMock()
        service.backtests = MagicMock()
        service.audit = MagicMock()
        service.backtest_service = MagicMock()

        storage = MagicMock()
        storage.delete = MagicMock(side_effect=Exception("S3 error"))
        service._storage = storage

        job1 = SimpleNamespace(id="j1", storage_key="s3://key1", status="succeeded")
        job2 = SimpleNamespace(id="j2", storage_key="s3://key2", status="succeeded")
        job3 = SimpleNamespace(id="j3", storage_key=None, status="succeeded")

        service.exports.list_expired_for_cleanup = MagicMock(
            side_effect=[[job1, job2, job3], []]
        )
        service.session.execute = MagicMock()
        service.session.commit = MagicMock()

        cleaned = service.cleanup_expired_exports(batch_size=100, max_batches=10)
        # DB cleanup succeeds for all 3, but storage fails for 2 with keys
        # The cleaned count should reflect DB success = 3
        assert cleaned == 3
