from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (PROJECT_ROOT / path).read_text(encoding="utf-8")


def test_prod_like_canary_covers_required_runtime_flows() -> None:
    source = _read("scripts/prod_like_canary.py")
    assert '"/health/live"' in source
    assert '"/health/ready"' in source
    assert '"/v1/meta"' in source
    assert '"/v1/me"' in source
    assert '"/v1/backtests"' in source
    assert '/cancel",' in source or "/cancel'" in source
    assert "repair_stranded_jobs(" in source
    assert "_seed_queued_backtest_run(" in source
    assert "_poll_until_not_queued(" in source


def test_freshness_script_bootstraps_repo_and_reads_summary() -> None:
    source = _read("scripts/check_historical_data_freshness.py")
    assert "bootstrap_repo(load_api_env=True)" in source
    assert "get_freshness_summary()" in source


def test_shared_script_bootstrap_adds_repo_root_before_imports() -> None:
    source = _read("scripts/_bootstrap.py")
    assert "if str(ROOT) not in sys.path:" in source
    assert "sys.path.insert(0, str(ROOT))" in source
    assert "from repo_bootstrap import ensure_repo_import_paths" in source
