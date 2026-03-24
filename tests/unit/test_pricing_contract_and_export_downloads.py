from __future__ import annotations

import inspect
from pathlib import Path


def test_billing_router_exposes_pricing_contract_endpoint() -> None:
    from apps.api.app.routers import billing

    source = inspect.getsource(billing)
    assert '@router.get("/pricing"' in source
    assert 'PricingContractResponse' in source


def test_backend_pricing_contract_is_centralized() -> None:
    from backtestforecast.services.pricing_contract import build_pricing_contract

    contract = build_pricing_contract()
    plans = {plan.tier: plan for plan in contract.plans}

    assert plans["pro"].monthly is not None
    assert plans["pro"].monthly.display_price == "$29/mo"
    assert plans["premium"].monthly is not None
    assert plans["premium"].monthly.display_price == "$79/mo"


def test_pricing_page_fetches_backend_contract() -> None:
    source = Path('apps/web/app/pricing/page.tsx').read_text()
    assert '/v1/billing/pricing' in source
    assert 'contract.plans.map' in source
    assert 'plan.monthly?.display_price' in source
    assert '$29/mo' not in source
    assert '$79/mo' not in source


def test_export_download_separates_metadata_from_db_content_load() -> None:
    from apps.api.app.routers.exports import download_export
    from backtestforecast.services.exports import ExportService

    source = inspect.getsource(ExportService.get_export_for_download)
    assert 'include_content=False' in source
    source_router = inspect.getsource(download_export)
    assert 'get_db_content_bytes_for_download' in source_router
