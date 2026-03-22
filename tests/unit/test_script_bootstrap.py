from __future__ import annotations

import os
from pathlib import Path

from scripts._bootstrap import _load_env_file


def test_load_env_file_sets_missing_defaults(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("EXAMPLE_KEY=hello\nEXAMPLE_OTHER='world'\n", encoding="utf-8")
    monkeypatch.delenv("EXAMPLE_KEY", raising=False)
    monkeypatch.delenv("EXAMPLE_OTHER", raising=False)

    _load_env_file(env_file)

    assert os.environ["EXAMPLE_KEY"] == "hello"
    assert os.environ["EXAMPLE_OTHER"] == "world"
