"""Test rate limiter fallback behavior when Redis is unavailable."""


def test_fallback_documents_expected_behavior():
    """When Redis is down, the rate limiter falls back to in-memory counters.

    Key behaviors to verify:
    - In-memory counters are per-process (not shared across workers)
    - fail_closed=True causes requests to be rejected when Redis is down
    - Memory cap evicts stale keys when counter size exceeds limit
    """
    pass  # Placeholder documenting expected behavior
