from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _read(relpath: str) -> str:
    return (ROOT / relpath).read_text(encoding="utf-8")


def test_router_dispatch_transaction_guard_script_exists_and_checks_commits() -> None:
    source = _read("scripts/check_router_dispatch_transactions.py")
    assert "db.commit()" in source
    assert "dispatch_celery_task()" in source
    assert "create_and_dispatch_export(" in source


def test_scripts_bootstrap_env_files_for_local_defaults() -> None:
    source = _read("scripts/_bootstrap.py")
    assert "apps\" / \"api\" / \".env.example\"" in source
    assert "apps\" / \"web\" / \".env.example\"" in source
    assert "override=True" in source
    assert "protected_keys = set(os.environ)" in source


def test_repair_script_has_precondition_error_for_missing_database_url() -> None:
    source = _read("scripts/repair_stranded_jobs.py")
    assert '"DATABASE_URL" not in os.environ' in source
    assert "Auto-loaded .env defaults were insufficient" in source


def test_exports_router_supports_retrying_failed_exports() -> None:
    router_source = _read("apps/api/app/routers/exports.py")
    service_source = _read("src/backtestforecast/services/exports.py")
    assert '@router.post("/{export_job_id}/retry"' in router_source
    assert "regenerate_failed_export" in service_source
    assert "export.regenerated" in service_source


def test_dispatch_repair_flows_are_audited() -> None:
    source = _read("src/backtestforecast/services/dispatch_recovery.py")
    assert "dispatch.repaired" in source
    assert "dispatch.repair_failed" in source
    assert "dispatch.idempotency_requeued" in source


def test_ci_runs_router_dispatch_guard() -> None:
    source = _read(".github/workflows/ci.yml")
    assert "Guard router dispatch transaction boundaries" in source
    assert "python scripts/check_router_dispatch_transactions.py" in source


def test_github_workflows_use_python_module_alembic_invocations() -> None:
    workflow_paths = (
        ".github/workflows/ci.yml",
        ".github/workflows/cd.yml",
        ".github/workflows/playwright.yml",
        ".github/workflows/live-provider-nightly.yml",
    )
    bare_command = re.compile(r"(?<!python -m )\balembic\b\s+(upgrade|downgrade|current|heads|check)\b")

    for relpath in workflow_paths:
        source = _read(relpath)
        disallowed = [
            line.strip()
            for line in source.splitlines()
            if bare_command.search(line) and not line.strip().startswith("- name:")
        ]
        assert not disallowed, f"{relpath} contains bare alembic command snippets: {disallowed}"


def test_ci_has_repo_root_bootstrap_smoke_check() -> None:
    source = _read(".github/workflows/ci.yml")

    assert "Verify repo-root Python bootstrap beats stale path contamination" in source
    assert "PYTHONPATH: .ci-stale-checkout" in source
    assert "from apps.api.app import main" in source


def test_cd_rollback_guidance_requires_reviewed_migration_reversibility() -> None:
    source = _read(".github/workflows/cd.yml")

    assert "alembic downgrade -1" not in source
    assert "Do not run a schema downgrade as a default rollback step." in source
    assert "migration is proven reversible for this deployment" in source


def test_gitignore_covers_local_runtime_and_audit_artifacts() -> None:
    source = _read(".gitignore")

    assert "/celery-broker-*.sqlite3" in source
    assert "/celerybeat-schedule*" in source
    assert "/apps/web/playwright-report/" in source
    assert "/logs/" in source
    assert "/.bench_baseline*" in source
