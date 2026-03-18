"""Verify Prometheus alert rules don't reference non-existent bff_-prefixed metrics."""
from __future__ import annotations

import re
from pathlib import Path

import pytest


def test_no_bff_prefix_in_alert_rules():
    """Alert rules must not use the bff_ prefix (metrics are registered unprefixed)."""
    alerts_path = Path("ops/prometheus_alerts.yml")
    if not alerts_path.exists():
        pytest.skip("ops/prometheus_alerts.yml not found")
    content = alerts_path.read_text()
    matches = re.findall(r"\bbff_\w+", content)
    assert not matches, (
        f"Found bff_-prefixed metrics in alert rules that don't match "
        f"registered metric names: {matches}"
    )
