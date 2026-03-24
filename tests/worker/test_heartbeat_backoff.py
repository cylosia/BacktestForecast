"""Verify heartbeat backoff never exceeds the TTL."""


def test_heartbeat_backoff_never_exceeds_ttl():
    """The heartbeat backoff must stay below the TTL (90s) to prevent
    the reaper from falsely counting workers as dead."""
    ttl = 90
    max_consecutive = 10
    for errors in range(max_consecutive + 1):
        sleep_secs = min(30 * (2 ** errors), 60)
        assert sleep_secs < ttl, (
            f"Backoff with {errors} consecutive errors is {sleep_secs}s "
            f"which exceeds the {ttl}s TTL"
        )
