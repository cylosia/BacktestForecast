"""Tests for long-term architectural improvements.

Covers:
- #31: CD pipeline has database backup step
- #33: CI has DAST scanning job
- #40: Beat healthcheck verifies scheduling, not just PID
"""
from __future__ import annotations

import inspect
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class TestCDDatabaseBackup:
    """#31: CD pipeline must backup the database before running migrations."""

    def test_staging_has_backup_step(self) -> None:
        cd_yml = (PROJECT_ROOT / ".github" / "workflows" / "cd.yml").read_text()
        assert "Backup staging database" in cd_yml, (
            "cd.yml must have a 'Backup staging database' step before staging migrations"
        )
        assert "pg_dump" in cd_yml, (
            "cd.yml must use pg_dump for database backup"
        )

    def test_production_has_backup_step(self) -> None:
        cd_yml = (PROJECT_ROOT / ".github" / "workflows" / "cd.yml").read_text()
        assert "Backup production database" in cd_yml, (
            "cd.yml must have a 'Backup production database' step before production migrations"
        )

    def test_backup_before_migration(self) -> None:
        cd_yml = (PROJECT_ROOT / ".github" / "workflows" / "cd.yml").read_text()
        backup_pos = cd_yml.index("Backup production database")
        migration_pos = cd_yml.index("Run database migrations")
        assert backup_pos < migration_pos, (
            "Database backup step must appear BEFORE migration step in cd.yml"
        )


class TestDASTScanning:
    """#33: CI must have a DAST scanning job."""

    def test_dast_job_exists_and_not_commented(self) -> None:
        ci_yml = (PROJECT_ROOT / ".github" / "workflows" / "ci.yml").read_text()
        assert "dast-scan:" in ci_yml, (
            "ci.yml must have a 'dast-scan' job (not commented out)"
        )
        lines = ci_yml.split("\n")
        for line in lines:
            if "dast-scan:" in line:
                assert not line.strip().startswith("#"), (
                    "dast-scan job must not be commented out"
                )
                break

    def test_dast_uses_zap(self) -> None:
        ci_yml = (PROJECT_ROOT / ".github" / "workflows" / "ci.yml").read_text()
        assert "zaproxy" in ci_yml, (
            "DAST scanning should use ZAP (zaproxy)"
        )


class TestBeatHealthcheck:
    """#40: Beat healthcheck must verify scheduling, not just PID."""

    def test_beat_init_signal_connected(self) -> None:
        from apps.worker.app.celery_app import _on_beat_init
        assert callable(_on_beat_init)

    def test_beat_heartbeat_file_defined(self) -> None:
        from apps.worker.app.celery_app import _BEAT_HEARTBEAT_FILE
        assert _BEAT_HEARTBEAT_FILE == "/tmp/celerybeat_heartbeat"

    def test_prod_healthcheck_verifies_heartbeat(self) -> None:
        compose = (PROJECT_ROOT / "docker-compose.prod.yml").read_text()
        assert "celerybeat_heartbeat" in compose, (
            "Beat healthcheck must verify the heartbeat file, not just PID"
        )
        assert "age < 120" in compose or "stale" in compose, (
            "Beat healthcheck must check heartbeat age for staleness"
        )
