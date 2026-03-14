"""Tests for /metrics and /admin/dlq admin endpoints."""
from __future__ import annotations


def test_metrics_accessible_in_dev(client):
    """In dev/test, /metrics should be accessible without auth."""
    resp = client.get("/metrics")
    assert resp.status_code == 200


def test_dlq_accessible_in_dev(client):
    """In dev/test, /admin/dlq should be accessible without auth."""
    resp = client.get("/admin/dlq")
    assert resp.status_code in (200, 503)
