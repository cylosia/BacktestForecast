"""Locust load test configuration for critical API endpoints.

Run with:
    locust -f tests/load/locustfile.py --host http://localhost:8000

Requires a valid Bearer token via the LOAD_TEST_TOKEN environment variable.
"""
from __future__ import annotations

import os

from locust import HttpUser, between, task


class BacktestForecastUser(HttpUser):
    wait_time = between(0.5, 2.0)
    _token: str = ""

    def on_start(self) -> None:
        self._token = os.environ.get("LOAD_TEST_TOKEN", "")
        if not self._token:
            raise RuntimeError("Set LOAD_TEST_TOKEN env var to a valid Bearer token")

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}

    @task(5)
    def health_ready(self) -> None:
        self.client.get("/health/ready")

    @task(3)
    def list_backtests(self) -> None:
        self.client.get("/v1/backtests", headers=self._headers())

    @task(2)
    def list_templates(self) -> None:
        self.client.get("/v1/templates", headers=self._headers())

    @task(1)
    def get_feature_access(self) -> None:
        self.client.get("/v1/me/feature-access", headers=self._headers())

    @task(1)
    def get_daily_picks(self) -> None:
        self.client.get("/v1/daily-picks", headers=self._headers())
