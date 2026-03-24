"""Load test: sweep with 100 candidates.

Run manually with: pytest tests/load/test_sweep_load.py -k sweep --no-header -s
Requires DATABASE_URL and MASSIVE_API_KEY environment variables.
"""
from __future__ import annotations

import pytest


@pytest.mark.skip(reason="load test - run manually against staging")
def test_sweep_with_100_candidates_completes():
    """Verify a 100-candidate sweep completes within the 60-minute timeout.

    Success criteria:
    - Job reaches 'succeeded' status
    - At least 1 result row produced
    - Total execution time < 3600s

    Steps to implement:
    1. Create a sweep with 100 candidate parameter sets via TestClient
    2. Execute the sweep synchronously (or via Celery in-process)
    3. Assert completion within timeout
    4. Assert result count > 0
    """
    pytest.skip("Not yet implemented - see docstring for specification")
