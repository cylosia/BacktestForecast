"""Test production guard in seed_dev_data.py."""
import os
import subprocess
import sys


def test_seed_dev_data_blocks_production():
    """Verify seed_dev_data.py refuses to run when APP_ENV=production."""
    env = os.environ.copy()
    env["APP_ENV"] = "production"
    result = subprocess.run(
        [sys.executable, "scripts/seed_dev_data.py", "--help"],
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert (
        "must not be run against a production" in result.stderr
        or "must not be run against a production" in result.stdout
    )
