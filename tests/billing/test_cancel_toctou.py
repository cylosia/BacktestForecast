"""Verify cancel_in_flight_jobs uses RETURNING to avoid TOCTOU race."""
from __future__ import annotations

import inspect

from backtestforecast.services.billing import BillingService


def test_cancel_uses_returning():
    """cancel_in_flight_jobs should use RETURNING clause on UPDATE.

    Ideally this would invoke the method with a mock session and verify the
    emitted SQL contains RETURNING, but that requires full DB/session
    infrastructure. As a pragmatic middle ground we inspect the source.
    """
    source = inspect.getsource(BillingService.cancel_in_flight_jobs)
    assert ".returning(" in source, (
        "cancel_in_flight_jobs should use UPDATE...RETURNING to get actually-cancelled "
        "job IDs, avoiding TOCTOU race with concurrent job completion."
    )
