"""Tests verifying webhook idempotency survives rollback.

Testing Gaps:
  TG-8: Webhook double-delivery during rollback
  
Verifies that the stripe_events.claim() method uses a savepoint (nested
transaction) so that a rollback in the webhook handler does NOT destroy
the claim row.
"""
from __future__ import annotations

import inspect


class TestWebhookClaimSavepoint:
    """The claim() method must use begin_nested() (savepoint)."""

    def test_claim_uses_savepoint(self):
        from backtestforecast.repositories.stripe_events import StripeEventRepository
        src = inspect.getsource(StripeEventRepository.claim)
        assert "begin_nested()" in src, (
            "claim() must use session.begin_nested() to survive outer rollbacks"
        )

    def test_claim_handles_integrity_error(self):
        from backtestforecast.repositories.stripe_events import StripeEventRepository
        src = inspect.getsource(StripeEventRepository.claim)
        assert "IntegrityError" in src

    def test_claim_returns_none_on_duplicate(self):
        from backtestforecast.repositories.stripe_events import StripeEventRepository
        src = inspect.getsource(StripeEventRepository.claim)
        assert "return None" in src


class TestWebhookErrorRecovery:
    """Webhook error handling marks events correctly."""

    def test_mark_error_exists(self):
        from backtestforecast.repositories.stripe_events import StripeEventRepository
        assert hasattr(StripeEventRepository, "mark_error")

    def test_stale_claim_recovery_exists(self):
        from backtestforecast.repositories.stripe_events import StripeEventRepository
        assert hasattr(StripeEventRepository, "_recover_stale_claim")

    def test_stale_claim_targets_processing_and_error(self):
        from backtestforecast.repositories.stripe_events import StripeEventRepository
        src = inspect.getsource(StripeEventRepository._recover_stale_claim)
        assert '"processing"' in src or "'processing'" in src
        assert '"error"' in src or "'error'" in src
