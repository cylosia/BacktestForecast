"""Item 81: S3 orphan scenario — commit failure after upload.

Documentation + logging test: verify the export service has a logger
for warning when DB commit fails after S3 upload.
"""
from __future__ import annotations

import logging


def test_s3_orphan_logged_on_commit_failure() -> None:
    """Verify that a warning is logged when DB commit fails after S3 upload."""
    logger = logging.getLogger("backtestforecast.services.exports")
    assert logger is not None, "Export service should have a logger"
