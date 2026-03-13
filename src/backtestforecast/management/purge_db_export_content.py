"""Purge export content from DB column after migration to object storage.

Queries ExportJobs where content_bytes IS NOT NULL, storage_key IS NOT NULL,
and status = 'succeeded'. For each job, verifies content exists in storage
(storage.exists), then sets content_bytes = None.

Usage:
    python -m backtestforecast.management.purge_db_export_content [--batch-size N] [--no-dry-run]
"""

from __future__ import annotations

import sys

import structlog
from sqlalchemy import select
from sqlalchemy.orm import Session

from backtestforecast.config import get_settings
from backtestforecast.db.session import SessionLocal
from backtestforecast.exports.storage import ExportStorage, get_export_storage
from backtestforecast.models import ExportJob

logger = structlog.get_logger("purge_db_export_content")


def purge_db_export_content(
    session: Session,
    storage: ExportStorage,
    *,
    batch_size: int = 100,
    dry_run: bool = True,
) -> int:
    """Purge content_bytes from ExportJobs that have content in object storage.

    Queries ExportJobs where content_bytes IS NOT NULL, storage_key IS NOT NULL,
    and status = 'succeeded'. For each job, verifies storage.exists(job.storage_key),
    then sets content_bytes = None and commits.

    Returns:
        Count of purged rows.
    """
    purged = 0

    while True:
        stmt = (
            select(ExportJob)
            .where(
                ExportJob.content_bytes.isnot(None),
                ExportJob.storage_key.isnot(None),
                ExportJob.status == "succeeded",
            )
            .limit(batch_size)
        )
        jobs = list(session.scalars(stmt))

        if not jobs:
            break

        batch_purged = 0
        for job in jobs:
            if not job.storage_key:
                continue
            if not storage.exists(job.storage_key):
                logger.warning(
                    "purge.skip_missing_in_storage",
                    export_job_id=str(job.id),
                    storage_key=job.storage_key,
                )
                continue

            if dry_run:
                logger.info(
                    "purge.would_purge",
                    export_job_id=str(job.id),
                    storage_key=job.storage_key,
                )
            else:
                job.content_bytes = None
                logger.info(
                    "purge.purged",
                    export_job_id=str(job.id),
                    storage_key=job.storage_key,
                )
            purged += 1
            batch_purged += 1

        if not dry_run and batch_purged > 0:
            session.commit()
            logger.info("purge.batch_committed", batch_size=len(jobs), purged_so_far=purged)

        if len(jobs) < batch_size:
            break

    return purged


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Purge export content from DB after S3 migration")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing")
    parser.add_argument("--no-dry-run", action="store_true", help="Actually purge (default is dry-run)")
    args = parser.parse_args()

    settings = get_settings()
    storage = get_export_storage(settings)

    with SessionLocal() as session:
        count = purge_db_export_content(
            session,
            storage,
            batch_size=args.batch_size,
            dry_run=not args.no_dry_run,
        )

    mode = "dry_run" if not args.no_dry_run else "purged"
    logger.info("purge.complete", count=count, mode=mode)
    return 0


if __name__ == "__main__":
    sys.exit(main())
