"""Item 73: E2E test for scanner symbol limit enforcement.

Verifies PRO basic max_symbols is 5 and PRO advanced/premium allows more.
"""
from __future__ import annotations

import pytest


def test_pro_basic_scanner_max_symbols() -> None:
    """PRO basic mode should allow 5 symbols."""
    from backtestforecast.billing.entitlements import (
        POLICIES,
        PlanTier,
        ScannerAccessPolicy,
        ScannerMode,
    )

    policy = POLICIES[(PlanTier.PRO, ScannerMode.BASIC)]
    assert isinstance(policy, ScannerAccessPolicy)
    assert policy.max_symbols == 5, (
        f"PRO basic should allow 5 symbols, got {policy.max_symbols}"
    )


def test_premium_advanced_allows_more_symbols() -> None:
    """Premium advanced mode should allow more than PRO basic."""
    from backtestforecast.billing.entitlements import (
        POLICIES,
        PlanTier,
        ScannerAccessPolicy,
        ScannerMode,
    )

    pro_basic = POLICIES[(PlanTier.PRO, ScannerMode.BASIC)]
    premium_advanced = POLICIES[(PlanTier.PREMIUM, ScannerMode.ADVANCED)]
    assert isinstance(premium_advanced, ScannerAccessPolicy)
    assert premium_advanced.max_symbols > pro_basic.max_symbols, (
        f"Premium advanced ({premium_advanced.max_symbols}) should allow more "
        f"symbols than PRO basic ({pro_basic.max_symbols})"
    )
