from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from shutil import rmtree
from uuid import uuid4

import repo_bootstrap


def test_ensure_repo_import_paths_places_repo_src_first(monkeypatch) -> None:
    fake_root = Path(f".test-repo-bootstrap-{uuid4().hex}")
    fake_src = fake_root / "src"
    fake_root.mkdir()
    fake_src.mkdir()

    original_sys_path = list(sys.path)
    monkeypatch.setattr(repo_bootstrap, "ROOT", fake_root)
    monkeypatch.setattr(repo_bootstrap, "SRC_DIR", fake_src)
    sys.path[:] = [str(fake_root / "stale"), str(fake_root), str(fake_src), *original_sys_path[:3]]

    try:
        root, src = repo_bootstrap.ensure_repo_import_paths()
        assert root == fake_root
        assert src == fake_src
        assert sys.path[0] == str(fake_src)
        assert sys.path[1] == str(fake_root)
        assert sys.path.count(str(fake_root)) == 1
        assert sys.path.count(str(fake_src)) == 1
    finally:
        sys.path[:] = original_sys_path
        rmtree(fake_root, ignore_errors=True)


def test_get_migration_status_surfaces_head_resolution_errors(monkeypatch) -> None:
    from backtestforecast.db import session as session_module

    monkeypatch.setattr(
        session_module,
        "_get_expected_revision_details",
        lambda: (None, "ModuleNotFoundError: demo"),
    )
    monkeypatch.setattr(session_module, "get_applied_revision", lambda: "20260330_0013")

    status = session_module.get_migration_status()

    assert status["aligned"] is False
    assert status["expected_revision"] is None
    assert status["applied_revision"] == "20260330_0013"
    assert status["error"] == "ModuleNotFoundError: demo"


def test_repo_root_python_import_uses_workspace_src() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import backtestforecast; print(backtestforecast.__file__)",
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=repo_root,
    )

    expected = repo_root / "src" / "backtestforecast" / "__init__.py"
    assert result.stdout.strip() == str(expected)
