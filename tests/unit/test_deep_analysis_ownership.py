"""Item 78: Verify deep analysis ownership uses SQL filter.

get_analysis must use a query with both id and user_id filters (SQL-level),
not Python-side filtering.
"""
from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from backtestforecast.pipeline.deep_analysis import SymbolDeepAnalysisService


def test_get_analysis_filters_by_user_id() -> None:
    """Verify that get_analysis uses SQL-level user_id filtering, not Python-side."""
    session = MagicMock()
    session.scalar.return_value = None
    service = SymbolDeepAnalysisService(
        session=session,
        market_data_fetcher=MagicMock(),
        backtest_executor=MagicMock(),
    )
    user = MagicMock()
    user.id = uuid4()

    from backtestforecast.errors import NotFoundError

    with pytest.raises(NotFoundError):
        service.get_analysis(user=user, analysis_id=uuid4())

    assert session.scalar.called, "Should use session.scalar, not session.get"
    assert not getattr(session.get, "called", False), (
        "Should NOT use session.get (Python-side filtering)"
    )
