"""Contract test: SSE event payloads match frontend expectations.

The frontend ``useSSE`` hook (hooks/use-sse.ts) and ``sweep-job-poller``
expect SSE ``status`` events to carry a JSON payload with at least:
  - ``v`` (schema version, always 1)
  - ``status`` (string matching JobStatus values)
  - ``job_id`` (UUID string)

Additional metadata keys are allowed but the three above are mandatory.
"""
from __future__ import annotations

import json
from uuid import UUID, uuid4

import pytest

from backtestforecast.events import publish_job_status
from backtestforecast.schemas.common import JobStatus

_REQUIRED_KEYS = {"v", "status", "job_id"}


class _CapturingRedis:
    """Minimal Redis stub that captures published payloads."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []

    def publish(self, channel: str, message: str) -> None:
        self.published.append((channel, message))

    def close(self) -> None:
        pass


@pytest.fixture()
def capture_redis(monkeypatch: pytest.MonkeyPatch) -> _CapturingRedis:
    fake = _CapturingRedis()
    import backtestforecast.events as events_mod
    monkeypatch.setattr(events_mod, "_redis_client", fake)
    yield fake
    monkeypatch.setattr(events_mod, "_redis_client", None)


def test_backtest_status_event_has_required_fields(capture_redis: _CapturingRedis) -> None:
    """The payload published for a backtest status change contains v, status, job_id."""
    job_id = uuid4()
    publish_job_status("backtest", job_id, JobStatus.SUCCEEDED)

    assert len(capture_redis.published) == 1
    channel, raw = capture_redis.published[0]
    payload = json.loads(raw)

    assert _REQUIRED_KEYS.issubset(payload.keys()), (
        f"Missing required keys: {_REQUIRED_KEYS - payload.keys()}"
    )
    assert payload["v"] == 1
    assert payload["status"] == JobStatus.SUCCEEDED
    assert payload["job_id"] == str(job_id)
    assert channel == f"job:backtest:{job_id}:status"


def test_sweep_status_event_has_required_fields(capture_redis: _CapturingRedis) -> None:
    """Sweep events carry the same contract as backtest events."""
    job_id = uuid4()
    publish_job_status("sweep", job_id, "running", metadata={"evaluated_candidate_count": 5})

    assert len(capture_redis.published) == 1
    _, raw = capture_redis.published[0]
    payload = json.loads(raw)

    assert _REQUIRED_KEYS.issubset(payload.keys())
    assert payload["v"] == 1
    assert payload["status"] == "running"
    assert payload["evaluated_candidate_count"] == 5


def test_metadata_keys_cannot_overwrite_reserved(capture_redis: _CapturingRedis) -> None:
    """Metadata must not be able to clobber v, status, or job_id."""
    job_id = uuid4()
    publish_job_status(
        "backtest", job_id, JobStatus.FAILED,
        metadata={"v": 99, "status": "hacked", "job_id": "evil"},
    )

    assert len(capture_redis.published) == 1
    _, raw = capture_redis.published[0]
    payload = json.loads(raw)

    assert payload["v"] == 1, "v must not be overridden by metadata"
    assert payload["status"] == JobStatus.FAILED, "status must not be overridden"
    assert payload["job_id"] == str(job_id), "job_id must not be overridden"


def test_channel_format_matches_frontend_pattern(capture_redis: _CapturingRedis) -> None:
    """Frontend subscribes to ``job:{type}:{id}:status`` channels."""
    job_id = uuid4()
    for job_type in ("backtest", "multi_symbol_backtest", "multi_step_backtest", "scan", "export", "sweep", "analysis"):
        publish_job_status(job_type, job_id, "queued")

    channels = [ch for ch, _ in capture_redis.published]
    for job_type in ("backtest", "multi_symbol_backtest", "multi_step_backtest", "scan", "export", "sweep", "analysis"):
        expected = f"job:{job_type}:{job_id}:status"
        assert expected in channels, f"Missing channel {expected}"


def test_payload_is_valid_json(capture_redis: _CapturingRedis) -> None:
    """Every published message must be valid JSON parseable by the frontend."""
    job_id = uuid4()
    publish_job_status("backtest", job_id, "queued", metadata={"progress": 0.5})

    _, raw = capture_redis.published[0]
    payload = json.loads(raw)
    assert isinstance(payload, dict)
    UUID(payload["job_id"])  # must be a valid UUID string


def test_large_metadata_is_truncated(capture_redis: _CapturingRedis) -> None:
    """Payloads exceeding 10 KB metadata are truncated to prevent SSE bloat."""
    job_id = uuid4()
    large = {"data": "x" * 15_000}
    publish_job_status("backtest", job_id, "running", metadata=large)

    assert len(capture_redis.published) == 1
    _, raw = capture_redis.published[0]
    payload = json.loads(raw)
    assert payload.get("_truncated") is True or len(raw) <= 12_000
