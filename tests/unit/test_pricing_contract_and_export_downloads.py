from __future__ import annotations

import inspect
from pathlib import Path


def test_billing_router_exposes_pricing_contract_endpoint() -> None:
    from apps.api.app.routers import billing

    source = inspect.getsource(billing)
    assert '@router.get("/pricing"' in source
    assert 'PricingContractResponse' in source


def test_pricing_page_fetches_backend_contract() -> None:
    source = Path('apps/web/app/pricing/page.tsx').read_text()
    assert '/v1/billing/pricing' in source
    assert 'contract.plans.map' in source


def test_export_download_separates_metadata_from_db_content_load() -> None:
    from backtestforecast.services.exports import ExportService
    from apps.api.app.routers.exports import download_export

    source = inspect.getsource(ExportService.get_export_for_download)
    assert 'include_content=False' in source
    source_router = inspect.getsource(download_export)
    assert 'get_db_content_bytes_for_download' in source_router
