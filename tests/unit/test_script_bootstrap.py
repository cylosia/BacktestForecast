from __future__ import annotations

import os
import shutil
from pathlib import Path
from uuid import uuid4

from scripts import _bootstrap
from scripts._bootstrap import _load_env_file


def test_load_env_file_sets_missing_defaults(monkeypatch) -> None:
    env_file = Path(f".test-bootstrap-{uuid4().hex}.env")
    env_file.write_text("EXAMPLE_KEY=hello\nEXAMPLE_OTHER='world'\n", encoding="utf-8")
    monkeypatch.delenv("EXAMPLE_KEY", raising=False)
    monkeypatch.delenv("EXAMPLE_OTHER", raising=False)

    try:
        _load_env_file(env_file)

        assert os.environ["EXAMPLE_KEY"] == "hello"
        assert os.environ["EXAMPLE_OTHER"] == "world"
    finally:
        env_file.unlink(missing_ok=True)


def test_load_env_file_override_replaces_earlier_file_values(monkeypatch) -> None:
    env_file = Path(f".test-bootstrap-{uuid4().hex}.env")
    env_file.write_text("AWS_ACCESS_KEY_ID=from-api-env\n", encoding="utf-8")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "")

    try:
        _load_env_file(env_file, override=True)
        assert os.environ["AWS_ACCESS_KEY_ID"] == "from-api-env"
    finally:
        env_file.unlink(missing_ok=True)


def test_bootstrap_repo_prefers_api_env_over_root_blank_defaults(monkeypatch) -> None:
    root = Path(f".test-bootstrap-root-{uuid4().hex}")
    root.mkdir()
    try:
        (root / ".env").write_text("AWS_ACCESS_KEY_ID=\nAWS_SECRET_ACCESS_KEY=\n", encoding="utf-8")
        api_dir = root / "apps" / "api"
        api_dir.mkdir(parents=True)
        (api_dir / ".env.example").write_text("AWS_ACCESS_KEY_ID=\n", encoding="utf-8")
        (api_dir / ".env").write_text(
            "AWS_ACCESS_KEY_ID=real-key\nAWS_SECRET_ACCESS_KEY=real-secret\n",
            encoding="utf-8",
        )

        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
        monkeypatch.setattr(_bootstrap, "ROOT", root)

        _bootstrap.bootstrap_repo(load_api_env=True)

        assert os.environ["AWS_ACCESS_KEY_ID"] == "real-key"
        assert os.environ["AWS_SECRET_ACCESS_KEY"] == "real-secret"
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_bootstrap_repo_does_not_override_real_process_env(monkeypatch) -> None:
    root = Path(f".test-bootstrap-root-{uuid4().hex}")
    root.mkdir()
    try:
        (root / ".env").write_text("AWS_ACCESS_KEY_ID=file-key\n", encoding="utf-8")
        api_dir = root / "apps" / "api"
        api_dir.mkdir(parents=True)
        (api_dir / ".env").write_text("AWS_ACCESS_KEY_ID=api-key\n", encoding="utf-8")

        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "process-key")
        monkeypatch.setattr(_bootstrap, "ROOT", root)

        _bootstrap.bootstrap_repo(load_api_env=True)

        assert os.environ["AWS_ACCESS_KEY_ID"] == "process-key"
    finally:
        shutil.rmtree(root, ignore_errors=True)
