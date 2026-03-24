"""Verify dispatch retry constants are reasonable."""
from apps.api.app.dispatch import _SEND_MAX_ATTEMPTS, _SEND_RETRY_DELAY


def test_dispatch_retry_constants():
    assert _SEND_MAX_ATTEMPTS == 3
    assert _SEND_RETRY_DELAY == 0.5
    max_block_time = sum(_SEND_RETRY_DELAY * i for i in range(1, _SEND_MAX_ATTEMPTS))
    assert max_block_time <= 2.0, f"Max retry block time {max_block_time}s exceeds 2s"
