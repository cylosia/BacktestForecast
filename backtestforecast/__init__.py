"""Repo-root import shim for local Python invocations.

This workspace uses a ``src/`` layout, but ad hoc ``python`` commands run from
the repo root do not automatically add ``src`` ahead of stale editable installs
or other checkouts on ``sys.path``. Load the real package in-place so
``import backtestforecast`` resolves to this checkout's source tree.
"""

from __future__ import annotations

from pathlib import Path

_SRC_PACKAGE_DIR = Path(__file__).resolve().parent.parent / "src" / "backtestforecast"
_SRC_INIT = _SRC_PACKAGE_DIR / "__init__.py"

__file__ = str(_SRC_INIT)
__path__ = [str(_SRC_PACKAGE_DIR)]

with _SRC_INIT.open("rb") as _src_init_file:
    exec(compile(_src_init_file.read(), __file__, "exec"))
