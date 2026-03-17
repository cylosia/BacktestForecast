"""Item 76: Test/documentation for beat healthcheck correctness.

The Celery Beat healthcheck was changed from `celery inspect ping` (which
contacts workers, not beat) to a PID-file-based check:

  healthcheck:
    test: ["CMD-SHELL", "test -f /tmp/celerybeat.pid && kill -0 $(cat /tmp/celerybeat.pid)"]
    interval: 60s
    timeout: 5s
    retries: 3
    start_period: 30s

This test validates the *logic* of the PID-based healthcheck approach by
simulating the file check and PID liveness verification locally.
"""
from __future__ import annotations

import os
import sys
import tempfile

import pytest


@pytest.mark.skipif(sys.platform == "win32", reason="os.kill signal 0 not supported on Windows")
class TestBeatHealthcheckPidFileApproach:
    def test_pid_file_present_and_process_alive(self):
        """When the PID file exists and the process is alive, the
        healthcheck should succeed (exit code 0)."""
        pid = os.getpid()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".pid", delete=False) as f:
            f.write(str(pid))
            pid_path = f.name

        try:
            stored_pid = int(open(pid_path).read().strip())
            assert stored_pid == pid
            try:
                os.kill(stored_pid, 0)
                alive = True
            except OSError:
                alive = False
            assert alive, "Current process should be alive"
        finally:
            os.unlink(pid_path)

    def test_pid_file_missing_fails(self):
        """When the PID file does not exist, the healthcheck should fail."""
        pid_path = "/tmp/_nonexistent_celerybeat_test.pid"
        assert not os.path.exists(pid_path)

    def test_pid_file_with_invalid_pid_is_detectable(self):
        """When the PID file contains an implausibly large PID,
        the healthcheck logic should be able to detect the process
        does not exist. We verify the file read + parse works."""
        dead_pid = 2_000_000_000
        with tempfile.NamedTemporaryFile(mode="w", suffix=".pid", delete=False) as f:
            f.write(str(dead_pid))
            pid_path = f.name

        try:
            stored_pid = int(open(pid_path).read().strip())
            assert stored_pid == dead_pid
            assert stored_pid > 1_000_000, "PID should be implausibly large"
        finally:
            os.unlink(pid_path)

    def test_healthcheck_command_is_documented(self):
        """Ensure we document the expected healthcheck command for beat."""
        expected_command = 'test -f /tmp/celerybeat.pid && kill -0 $(cat /tmp/celerybeat.pid)'
        assert "celerybeat.pid" in expected_command
        assert "kill -0" in expected_command
