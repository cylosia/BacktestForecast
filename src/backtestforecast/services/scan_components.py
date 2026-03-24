from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from backtestforecast.models import ScannerJob, User
    from backtestforecast.schemas.scans import (
        CreateScannerJobRequest,
    )
    from backtestforecast.services.scans import ScanService


class ScanJobFactory:
    def __init__(self, service: ScanService) -> None:
        self.service = service

    def create_job(self, user: User, payload: CreateScannerJobRequest) -> ScannerJob:
        return self.service._create_job_impl(user, payload)


class ScanExecutor:
    def __init__(self, service: ScanService) -> None:
        self.service = service

    def run_job(self, job_id):
        return self.service._run_job_impl(job_id)

    def build_forecast(self, *, user, symbol, strategy_type, horizon_days):
        return self.service._build_forecast_impl(
            user=user,
            symbol=symbol,
            strategy_type=strategy_type,
            horizon_days=horizon_days,
        )


class ScanPresenter:
    def __init__(self, service: ScanService) -> None:
        self.service = service

    def list_jobs(self, user, *, limit=50, offset=0, cursor=None):
        return self.service._list_jobs_impl(user, limit=limit, offset=offset, cursor=cursor)

    def get_job(self, user, job_id):
        return self.service._get_job_impl(user, job_id)

    def get_recommendations(self, user, job_id):
        return self.service._get_recommendations_impl(user, job_id)
