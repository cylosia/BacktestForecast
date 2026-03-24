"""Test production guards in seed_dev_data.py.

Covers all three safety checks:
1. APP_ENV=production/staging blocks execution
2. DATABASE_URL with sslmode=require blocks execution
3. DATABASE_URL pointing to cloud hosts blocks execution
"""
import os
import subprocess
import sys

import pytest


def test_seed_dev_data_blocks_production():
    """Verify seed_dev_data.py refuses to run when APP_ENV=production."""
    env = os.environ.copy()
    env["APP_ENV"] = "production"
    env.pop("DATABASE_URL", None)
    result = subprocess.run(
        [sys.executable, "scripts/seed_dev_data.py", "--help"],
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    combined = result.stderr + result.stdout
    assert "must not be run against a production" in combined


def test_seed_dev_data_blocks_sslmode_require():
    """sslmode=require in DATABASE_URL indicates a production-style connection."""
    env = os.environ.copy()
    env.pop("APP_ENV", None)
    env["DATABASE_URL"] = "postgresql://user:pass@localhost:5432/mydb?sslmode=require"
    result = subprocess.run(
        [sys.executable, "scripts/seed_dev_data.py", "--help"],
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    combined = result.stderr + result.stdout
    assert "sslmode=require" in combined


@pytest.mark.parametrize("cloud_host", [
    "rds.amazonaws.com",
    "cloud.google.com",
    "azure.com",
    ".prod.",
])
def test_seed_dev_data_blocks_cloud_hosts(cloud_host: str):
    """DATABASE_URL pointing to a cloud provider host must be blocked."""
    env = os.environ.copy()
    env.pop("APP_ENV", None)
    env["DATABASE_URL"] = f"postgresql://user:pass@db.{cloud_host}:5432/mydb"
    result = subprocess.run(
        [sys.executable, "scripts/seed_dev_data.py", "--help"],
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    combined = result.stderr + result.stdout
    assert "production" in combined.lower()
