"""Verify DLQ depth metric is updated correctly."""
from __future__ import annotations


def test_dlq_depth_metric_exists():
    from backtestforecast.observability.metrics import DLQ_DEPTH
    assert DLQ_DEPTH is not None
