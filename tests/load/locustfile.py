"""Locust load test configuration for critical API endpoints.

Run with:
    locust -f tests/load/locustfile.py --host http://localhost:8000

Requires a valid Bearer token via the LOAD_TEST_TOKEN environment variable.
"""
from __future__ import annotations

import json
import os
import time

from locust import HttpUser, between, task


class BacktestForecastUser(HttpUser):
    wait_time = between(0.5, 2.0)
    _token: str = ""
    _latest_run_id: str | None = None

    def on_start(self) -> None:
        self._token = os.environ.get("LOAD_TEST_TOKEN", "")
        if not self._token:
            raise RuntimeError("Set LOAD_TEST_TOKEN env var to a valid Bearer token")

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}

    def _json_headers(self) -> dict[str, str]:
        return {**self._headers(), "Content-Type": "application/json"}

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


    @task(2)
    def me(self) -> None:
        self.client.get("/v1/me", headers=self._headers(), name="/v1/me")

    @task(2)
    def get_backtest_detail(self) -> None:
        if not self._latest_run_id:
            return
        self.client.get(f"/v1/backtests/{self._latest_run_id}", headers=self._headers(), name="/v1/backtests/:id")

    @task(1)
    def create_backtest(self) -> None:
        if os.environ.get("LOAD_TEST_ENABLE_MUTATIONS", "false").lower() != "true":
            return
        payload = {
            "symbol": os.environ.get("LOAD_TEST_SYMBOL", "SPY"),
            "strategy_type": "long_call",
            "start_date": os.environ.get("LOAD_TEST_START_DATE", "2024-01-02"),
            "end_date": os.environ.get("LOAD_TEST_END_DATE", "2024-03-01"),
            "target_dte": 30,
            "dte_tolerance_days": 5,
            "max_holding_days": 10,
            "account_size": "10000",
            "risk_per_trade_pct": "5",
            "commission_per_contract": "1",
            "entry_rules": [{"type": "rsi", "operator": "lte", "threshold": "35", "period": 14}],
            "idempotency_key": f"locust-{int(time.time()*1000)}",
        }
        with self.client.post(
            "/v1/backtests",
            data=json.dumps(payload),
            headers=self._json_headers(),
            name="POST /v1/backtests",
            catch_response=True,
        ) as response:
            if response.status_code not in (200, 202):
                response.failure(f"unexpected status {response.status_code}: {response.text[:300]}")
                return
            try:
                body = response.json()
            except Exception as exc:
                response.failure(f"invalid JSON: {exc}")
                return
            self._latest_run_id = body.get("id")
            if not self._latest_run_id:
                response.failure("backtest create response missing id")
