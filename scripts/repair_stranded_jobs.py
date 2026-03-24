#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import structlog
from sqlalchemy.exc import SQLAlchemyError

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _bootstrap import bootstrap_repo

ROOT = bootstrap_repo(load_api_env=True)

from backtestforecast.db.session import create_session
from backtestforecast.services.dispatch_recovery import DISPATCH_TARGETS, find_stranded_jobs, repair_stranded_jobs


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect or repair queued jobs that never reached dispatch state.")
    parser.add_argument("--action", choices=("list", "requeue", "fail"), default="list")
    parser.add_argument("--older-than-minutes", type=int, default=10)
    args = parser.parse_args()

    if "DATABASE_URL" not in os.environ:
        print(
            "DATABASE_URL is not set. Auto-loaded .env defaults were insufficient; "
            "set DATABASE_URL or create apps/api/.env before running this script."
        )
        return 2

    logger = structlog.get_logger("scripts.repair_stranded_jobs")
    older_than = timedelta(minutes=max(args.older_than_minutes, 1))

    with create_session() as session:
        try:
            if args.action == "list":
                cutoff = datetime.now(UTC) - older_than
                rows = find_stranded_jobs(session, cutoff=cutoff, targets=DISPATCH_TARGETS)
                if not rows:
                    print("No stranded queued jobs found.")
                    return 0
                for target, job in rows:
                    print(f"{target.model_name} {job.id} created_at={job.created_at} status={job.status}")
                print(f"Found {len(rows)} stranded queued job(s).")
                return 0

            counts = repair_stranded_jobs(
                session,
                logger=logger,
                action=args.action,
                older_than=older_than,
            )
            print(counts)
            return 0
        except SQLAlchemyError as exc:
            print(f"Database unavailable: {exc}")
            return 2


if __name__ == "__main__":
    raise SystemExit(main())
