"""Load test: scanner with 25 symbols.

Run manually with: pytest tests/load/test_scanner_load.py -k scanner --no-header -s
Requires DATABASE_URL and MASSIVE_API_KEY environment variables.
"""
from __future__ import annotations

import pytest


@pytest.mark.skip(reason="load test — run manually against staging")
def test_scanner_with_25_symbols_completes():
    """Verify a 25-symbol scan completes within the 9-minute timeout.

    Success criteria:
    - Job reaches 'succeeded' status
    - At least 1 recommendation produced
    - Total execution time < 540s

    Steps to implement:
    1. Create a scan with 25 symbols via TestClient with real DB
    2. Execute the scan synchronously (or via Celery in-process)
    3. Assert completion within timeout
    4. Assert recommendation count > 0
    """
    pytest.skip("Not yet implemented — see docstring for specification")
