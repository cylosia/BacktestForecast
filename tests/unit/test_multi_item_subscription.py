"""Tests for BillingService._extract_price_details with multi-item subscriptions."""
from __future__ import annotations

from unittest.mock import MagicMock

from backtestforecast.services.billing import BillingService


def test_extract_price_details_multi_item_selects_known_price():
    """With 2 items, the one matching a known plan price should be selected."""
    session = MagicMock()
    settings = MagicMock()
    settings.stripe_price_lookup = {
        ("pro", "monthly"): "price_pro_monthly",
        ("pro", "yearly"): "price_pro_yearly",
        ("premium", "monthly"): "price_premium_monthly",
    }
    service = BillingService(session, settings=settings)

    # Subscription with 2 items: add-on first, plan second
    subscription = {
        "id": "sub_123",
        "items": {
            "data": [
                {
                    "price": {
                        "id": "price_addon_unknown",
                        "recurring": {"interval": "month"},
                    },
                },
                {
                    "price": {
                        "id": "price_pro_monthly",
                        "recurring": {"interval": "month"},
                    },
                },
            ],
        },
    }

    price_id, billing_interval = service._extract_price_details(subscription)
    assert price_id == "price_pro_monthly"
    assert billing_interval == "monthly"


def test_extract_price_details_multi_item_first_when_none_match():
    """With multiple items and no plan match, first item is used as fallback."""
    session = MagicMock()
    settings = MagicMock()
    settings.stripe_price_lookup = {
        ("pro", "monthly"): "price_pro_monthly",
    }
    service = BillingService(session, settings=settings)

    subscription = {
        "id": "sub_456",
        "items": {
            "data": [
                {
                    "price": {
                        "id": "price_other_addon",
                        "recurring": {"interval": "month"},
                    },
                },
                {
                    "price": {
                        "id": "price_another_addon",
                        "recurring": {"interval": "year"},
                    },
                },
            ],
        },
    }

    price_id, billing_interval = service._extract_price_details(subscription)
    assert price_id == "price_other_addon"
    assert billing_interval == "monthly"
