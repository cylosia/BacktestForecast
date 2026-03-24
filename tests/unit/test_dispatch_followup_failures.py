from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _read(relpath: str) -> str:
    return (ROOT / relpath).read_text(encoding="utf-8")


def test_create_dispatch_regressions_cover_broker_outage_and_post_commit_recovery() -> None:
    source = _read("tests/unit/test_create_dispatch_regressions.py")
    assert 'send_task.side_effect = ConnectionError("broker down")' in source
    assert "_assert_pending_outbox" in source
    assert "create_and_dispatch(" in source


def test_repo_has_redis_outage_regression_coverage_for_sse_and_rate_limiter_paths() -> None:
    sse_source = _read("tests/unit/test_sse_release_redis_fail.py")
    rate_limiter_source = _read("tests/unit/test_rate_limiter_fallback.py")

    assert "test_release_slot_handles_redis_error" in sse_source
    assert "test_refresh_slot_renews_distributed_ttl" in sse_source
    assert "TestInMemoryFallbackWhenRedisDown" in rate_limiter_source
    assert "test_redis_error_triggers_fallback" in rate_limiter_source
