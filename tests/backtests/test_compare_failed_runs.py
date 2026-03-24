"""Verify compare rejects non-succeeded runs."""
from __future__ import annotations

import pytest


def test_compare_schema_requires_unique_ids():
    import uuid

    from backtestforecast.schemas.backtests import CompareBacktestsRequest

    same_id = uuid.uuid4()
    with pytest.raises(Exception):
        CompareBacktestsRequest(run_ids=[same_id, same_id])
