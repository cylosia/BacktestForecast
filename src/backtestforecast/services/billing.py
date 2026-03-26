from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy import update as sa_update
from sqlalchemy.orm import Session

from backtestforecast.billing.entitlements import PAID_STATUSES, BillingInterval, PlanTier
from backtestforecast.billing.events import log_billing_event
from backtestforecast.billing.urls import resolve_return_url
from backtestforecast.config import Settings, get_settings
from backtestforecast.errors import (
    AppValidationError,
    AuthenticationError,
    ConfigurationError,
    ExternalServiceError,
    NotFoundError,
)
from backtestforecast.models import User
from backtestforecast.observability import get_logger
from backtestforecast.observability.logging import short_hash
from backtestforecast.observability.metrics import STRIPE_WEBHOOK_EVENTS_TOTAL
from backtestforecast.repositories.audit_events import AuditEventRepository
from backtestforecast.repositories.stripe_events import StripeEventRepository
from backtestforecast.repositories.users import UserRepository
from backtestforecast.schemas.billing import (
    CheckoutSessionResponse,
    CreateCheckoutSessionRequest,
    CreatePortalSessionRequest,
    PortalSessionResponse,
)
from backtestforecast.services.audit import AuditService
from backtestforecast.services.billing_components import (
    CheckoutService,
    PortalService,
    ReconciliationService,
    WebhookHandler,
)

logger = get_logger("billing")
UTC = UTC

_KNOWN_STRIPE_EVENTS = frozenset({
    "checkout.session.completed",
    "customer.subscription.created",
    "customer.subscription.updated",
    "customer.subscription.deleted",
    "customer.subscription.paused",
    "customer.subscription.resumed",
    "invoice.payment_failed",
    "invoice.payment_succeeded",
})


_STRIPE_CIRCUIT_KEY = "bff:stripe_circuit_open"


def _get_stripe_circuit_cooldown() -> int:
    from backtestforecast.config import get_settings
    return getattr(get_settings(), "stripe_circuit_cooldown_seconds", 30)


class BillingService:
    def __init__(self, session: Session, settings: Settings | None = None) -> None:
        self.session = session
        self.settings = settings or get_settings()
        self.users = UserRepository(session)
        self.audit = AuditService(session)
        self.audit_events = AuditEventRepository(session)
        self.stripe_events = StripeEventRepository(session)
        self._stripe_client: Any = None
        self._pending_cancellation_events: list[tuple[str, UUID]] = []
        self.checkout_service = CheckoutService(self)
        self.portal_service = PortalService(self)
        self.webhook_handler = WebhookHandler(self)
        self.reconciliation_service = ReconciliationService(self)

    def __enter__(self) -> BillingService:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is not None:
            try:
                self.session.rollback()
            except Exception:
                logger.warning("billing.context_manager_rollback_failed", exc_info=True)
        return False


    def create_checkout_session(
        self, user: User, payload: CreateCheckoutSessionRequest, *, request_id: str | None = None, ip_address: str | None = None,
    ) -> CheckoutSessionResponse:
        return self.checkout_service.create_checkout_session(user, payload, request_id=request_id, ip_address=ip_address)

    def create_portal_session(
        self, user: User, payload: CreatePortalSessionRequest, *, request_id: str | None = None, ip_address: str | None = None,
    ) -> PortalSessionResponse:
        return self.portal_service.create_portal_session(user, payload, request_id=request_id, ip_address=ip_address)

    def handle_webhook(
        self, payload_bytes: bytes, signature_header: str | None, *, request_id: str | None = None, ip_address: str | None = None,
    ) -> dict[str, str]:
        return self.webhook_handler.handle_webhook(payload_bytes, signature_header, request_id=request_id, ip_address=ip_address)

    def reconcile_subscriptions(self, *, grace_hours: int = 48, dry_run: bool = False) -> list[dict[str, Any]]:
        return self.reconciliation_service.reconcile_subscriptions(grace_hours=grace_hours, dry_run=dry_run)


    def _create_checkout_session_impl(
        self,
        user: User,
        payload: CreateCheckoutSessionRequest,
        *,
        request_id: str | None = None,
        ip_address: str | None = None,
    ) -> CheckoutSessionResponse:
        if payload.tier == PlanTier.FREE.value:
            raise AppValidationError("Free does not require a Stripe checkout session.")
        client = self._get_stripe_client()
        customer_id = self._get_or_create_customer(user)
        price_id = self._price_id_for(payload.tier, payload.billing_interval.value)

        checkout_session = client.checkout.sessions.create(
            params={
                "mode": "subscription",
                "customer": customer_id,
                "line_items": [{"price": price_id, "quantity": 1}],
                "success_url": f"{self.settings.app_public_url}/app/settings/billing?checkout=success",
                "cancel_url": f"{self.settings.app_public_url}/pricing?checkout=cancelled",
                "allow_promotion_codes": True,
                "client_reference_id": str(user.id),
                "metadata": {
                    "user_id": str(user.id),
                    "clerk_user_id": user.clerk_user_id,
                    "requested_tier": payload.tier,
                    "billing_interval": payload.billing_interval.value,
                },
                "subscription_data": {
                    "metadata": {
                        "user_id": str(user.id),
                        "clerk_user_id": user.clerk_user_id,
                        "requested_tier": payload.tier,
                        "billing_interval": payload.billing_interval.value,
                    }
                },
            }
        )
        self.audit.record(
            event_type="billing.checkout_session.created",
            subject_type="stripe_checkout_session",
            subject_id=checkout_session.id,
            user_id=user.id,
            request_id=request_id,
            ip_address=ip_address,
            metadata={
                "tier": payload.tier,
                "billing_interval": payload.billing_interval.value,
                "price_id": price_id,
                "customer_id": customer_id,
            },
        )
        self.session.commit()
        logger.info(
            "billing.checkout_session.created",
            user_id=str(user.id),
            session_id=checkout_session.id,
            tier=payload.tier,
            billing_interval=payload.billing_interval.value,
        )
        return CheckoutSessionResponse(
            session_id=checkout_session.id,
            checkout_url=checkout_session.url,
            tier=payload.tier,
            billing_interval=payload.billing_interval.value,
            expires_at=self._timestamp_to_datetime(checkout_session.expires_at),
        )

    def _create_portal_session_impl(
        self,
        user: User,
        payload: CreatePortalSessionRequest,
        *,
        request_id: str | None = None,
        ip_address: str | None = None,
    ) -> PortalSessionResponse:
        if not user.stripe_customer_id:
            raise NotFoundError("No Stripe customer is attached to this account yet.")
        client = self._get_stripe_client()
        return_url = self._resolve_return_url(payload.return_path)
        portal_session = client.billing_portal.sessions.create(
            params={"customer": user.stripe_customer_id, "return_url": return_url}
        )
        self.audit.record(
            event_type="billing.portal_session.created",
            subject_type="stripe_customer",
            subject_id=user.stripe_customer_id,
            user_id=user.id,
            request_id=request_id,
            ip_address=ip_address,
            metadata={"return_url": return_url},
        )
        self.session.commit()
        logger.info("billing.portal_session.created", user_id=str(user.id))
        return PortalSessionResponse(portal_url=portal_session.url)

    def _handle_webhook_impl(
        self,
        payload_bytes: bytes,
        signature_header: str | None,
        *,
        request_id: str | None = None,
        ip_address: str | None = None,
    ) -> dict[str, str]:
        client = self._get_stripe_client(skip_circuit_check=True)
        if not signature_header:
            raise AuthenticationError("Missing Stripe-Signature header.")
        try:
            event = client.construct_event(
                payload_bytes,
                signature_header,
                self.settings.stripe_webhook_secret,
            )
        except Exception as exc:  # pragma: no cover - third-party surface
            raise AuthenticationError("Invalid Stripe webhook signature.") from exc

        event_type = str(event["type"])
        safe_event_type = event_type if event_type in _KNOWN_STRIPE_EVENTS else "other"
        event_id = self._coerce_stripe_id(event.get("id")) or str(event.get("id") or "")
        if not event_id:
            logger.warning("billing.webhook.missing_event_id", event_type=event_type)
            STRIPE_WEBHOOK_EVENTS_TOTAL.labels(event_type=safe_event_type, result="ignored").inc()
            return {"status": "ignored", "reason": "missing_event_id"}

        from backtestforecast.observability import hash_ip

        claimed = self.stripe_events.claim(
            stripe_event_id=event_id,
            event_type=event_type,
            livemode=bool(event.get("livemode")),
            request_id=request_id,
            ip_hash=hash_ip(ip_address),
            payload_summary={
                "event_type": event_type,
                "livemode": bool(event.get("livemode")),
            },
        )
        if claimed is None:
            logger.info("billing.webhook.duplicate", event_id=event_id, event_type=event_type)
            STRIPE_WEBHOOK_EVENTS_TOTAL.labels(event_type=safe_event_type, result="duplicate").inc()
            return {"status": "duplicate", "event_type": event_type}
        self.session.flush()

        data_object = event["data"]["object"]

        try:
            if event_type == "checkout.session.completed":
                self._sync_checkout_session(data_object)
            elif event_type in {
                "customer.subscription.created",
                "customer.subscription.updated",
                "customer.subscription.deleted",
                "customer.subscription.paused",
                "customer.subscription.resumed",
            }:
                event_created_ts = event.get("created")
                self._sync_subscription(data_object, event_created_ts=event_created_ts)
            else:
                logger.info("billing.webhook.ignored", event_type=event_type)
        except ExternalServiceError as ese:
            self.session.rollback()
            self._mark_stripe_event_error(event_id, str(ese), event_type=event_type, livemode=bool(event.get("livemode")))
            STRIPE_WEBHOOK_EVENTS_TOTAL.labels(event_type=safe_event_type, result="error").inc()
            if not isinstance(ese.__cause__, NotFoundError) and "not found" not in str(ese).lower():
                self._trip_stripe_circuit()
            raise
        except NotFoundError as nfe:
            self.session.rollback()
            self._mark_stripe_event_error(event_id, str(nfe), event_type=event_type, livemode=bool(event.get("livemode")))
            STRIPE_WEBHOOK_EVENTS_TOTAL.labels(event_type=safe_event_type, result="not_found_retry").inc()
            logger.warning(
                "billing.webhook.user_not_found",
                event_id=event_id,
                event_type=event_type,
                error=str(nfe),
                hint="Event marked as error. Stripe retry after 15min will succeed via stale-claim recovery.",
            )
            raise
        except (KeyError, TypeError, ValueError, AttributeError) as programming_exc:
            self.session.rollback()
            self._mark_stripe_event_error(
                event_id,
                f"Processing error ({type(programming_exc).__name__}): {programming_exc}",
                event_type=event_type,
                livemode=bool(event.get("livemode")),
            )
            logger.exception(
                "billing.webhook.likely_programming_error",
                event_id=event_id,
                event_type=event_type,
                error_type=type(programming_exc).__name__,
            )
            STRIPE_WEBHOOK_EVENTS_TOTAL.labels(event_type=safe_event_type, result="error").inc()
            raise
        except Exception:
            self.session.rollback()
            self._mark_stripe_event_error(event_id, "Unhandled processing error", event_type=event_type, livemode=bool(event.get("livemode")))
            logger.exception("billing.webhook.processing_error", event_id=event_id, event_type=event_type)
            STRIPE_WEBHOOK_EVENTS_TOTAL.labels(event_type=safe_event_type, result="error").inc()
            raise

        self.stripe_events.mark_processed(event_id)
        self.session.commit()

        pending = self._pending_cancellation_events
        if pending:
            self.publish_cancellation_events(pending)
            self._pending_cancellation_events = []

        STRIPE_WEBHOOK_EVENTS_TOTAL.labels(event_type=safe_event_type, result="ok").inc()
        return {"status": "ok", "event_type": event_type}

    def _sync_checkout_session(self, checkout_session: Any) -> User | None:
        user = self._find_user_by_metadata(checkout_session)
        if user is None:
            customer_id = self._coerce_stripe_id(checkout_session.get("customer"))
            if customer_id:
                user = self.users.get_by_stripe_customer_id(customer_id)
        if user is None:
            logger.warning("billing.checkout_session.user_not_found", session_id=checkout_session.get("id"))
            raise NotFoundError(
                "User not found for checkout session; Stripe should retry this webhook."
            )

        customer_id = self._coerce_stripe_id(checkout_session.get("customer"))
        subscription_id = self._coerce_stripe_id(checkout_session.get("subscription"))
        if subscription_id:
            try:
                client = self._get_stripe_client()
                subscription = client.subscriptions.retrieve(subscription_id)
            except Exception as exc:
                logger.exception(
                    "billing.stripe_api_error",
                    action="subscriptions.retrieve",
                    subscription_id=subscription_id,
                    user_id=str(user.id),
                )
                raise ExternalServiceError(
                    "Unable to verify subscription with Stripe. The webhook will be retried.",
                ) from exc
            self._apply_subscription_to_user(user, subscription)
        elif customer_id:
            locked_user = self.session.scalar(
                select(User).where(User.id == user.id).with_for_update()
            )
            if locked_user is not None:
                locked_user.stripe_customer_id = customer_id
                self.session.flush()
        return user

    def _sync_subscription(
        self, subscription: Any, *, event_created_ts: int | None = None,
    ) -> User | None:
        user = self._find_user_by_metadata(subscription)
        if user is None:
            subscription_id = self._coerce_stripe_id(subscription.get("id"))
            if subscription_id:
                user = self.users.get_by_stripe_subscription_id(subscription_id)
        if user is None:
            customer_id = self._coerce_stripe_id(subscription.get("customer"))
            if customer_id:
                user = self.users.get_by_stripe_customer_id(customer_id)
        if user is None:
            logger.warning("billing.subscription.user_not_found", subscription_id=subscription.get("id"))
            raise NotFoundError(
                "User not found for subscription event; Stripe should retry this webhook."
            )
        self._apply_subscription_to_user(user, subscription, event_created_ts=event_created_ts)
        return user

    def _apply_subscription_to_user(
        self,
        user: User,
        subscription: Any,
        *,
        event_created_ts: int | None = None,
    ) -> None:
        locked_user = self.session.scalar(
            select(User).where(User.id == user.id).with_for_update()
        )
        if locked_user is None:
            raise NotFoundError("User account no longer exists.")
        user = locked_user

        subscription_id = self._coerce_stripe_id(subscription.get("id"))
        customer_id = self._coerce_stripe_id(subscription.get("customer"))
        status = str(subscription.get("status") or "") or None
        cancel_at_period_end = bool(subscription.get("cancel_at_period_end"))
        current_period_end = self._timestamp_to_datetime(subscription.get("current_period_end"))
        price_id, billing_interval = self._extract_price_details(subscription)
        effective_tier = self._configured_tier_for_price(price_id) or self._tier_from_metadata(subscription)
        if effective_tier is None:
            effective_tier = PlanTier.FREE.value
        if status == "past_due":
            pass
        elif status not in PAID_STATUSES:
            effective_tier = PlanTier.FREE.value

        _TIER_RANK = {"free": 0, "pro": 1, "premium": 2}
        incoming_tier_rank = _TIER_RANK.get(effective_tier, 0)
        current_tier_rank = _TIER_RANK.get(user.plan_tier, 0)
        is_upgrade = incoming_tier_rank > current_tier_rank

        if (
            subscription_id is not None
            and user.stripe_subscription_id is not None
            and subscription_id != user.stripe_subscription_id
            and not is_upgrade
            and user.subscription_status in ("active", "trialing", "past_due")
            and current_period_end is not None
            and user.subscription_current_period_end is not None
            and self._normalize_utc(current_period_end) < self._normalize_utc(user.subscription_current_period_end)
        ):
            logger.warning(
                "billing.subscription.stale_event_skipped",
                user_id=str(user.id),
                incoming_subscription_id=subscription_id,
                incoming_period_end=current_period_end.isoformat() if current_period_end else None,
                current_subscription_id=user.stripe_subscription_id,
                current_period_end=user.subscription_current_period_end.isoformat() if user.subscription_current_period_end else None,
                current_status=user.subscription_status,
                current_plan_tier=user.plan_tier,
                hint="Skipped because incoming subscription has an earlier period end and is not an upgrade.",
            )
            return

        is_terminal = status in ("canceled", "unpaid", "incomplete_expired")
        if (
            not is_terminal
            and not is_upgrade
            and current_period_end is not None
            and user.subscription_current_period_end is not None
            and self._normalize_utc(current_period_end) < self._normalize_utc(user.subscription_current_period_end)
            and subscription_id == user.stripe_subscription_id
        ):
            logger.info(
                "billing.subscription.out_of_order_webhook_skipped",
                user_id=str(user.id),
                incoming_period_end=current_period_end.isoformat(),
                existing_period_end=user.subscription_current_period_end.isoformat(),
            )
            return

        if (
            not is_terminal
            and not is_upgrade
            and event_created_ts is not None
            and current_period_end is not None
            and user.subscription_current_period_end is not None
            and self._normalize_utc(current_period_end) == self._normalize_utc(user.subscription_current_period_end)
            and subscription_id == user.stripe_subscription_id
            and user.plan_updated_at is not None
        ):
            event_created_dt = self._timestamp_to_datetime(event_created_ts)
            if event_created_dt is not None and event_created_dt < user.plan_updated_at:
                logger.info(
                    "billing.subscription.same_period_older_event_skipped",
                    user_id=str(user.id),
                    event_created=event_created_dt.isoformat(),
                    plan_updated_at=user.plan_updated_at.isoformat(),
                )
                return

        old_state = {
            "plan_tier": user.plan_tier,
            "subscription_status": user.subscription_status,
            "stripe_subscription_id": user.stripe_subscription_id,
        }

        if subscription_id is not None:
            user.stripe_subscription_id = subscription_id
        if customer_id is not None:
            user.stripe_customer_id = customer_id
        if price_id is not None:
            user.stripe_price_id = price_id
        user.subscription_status = status
        user.subscription_billing_interval = billing_interval
        user.subscription_current_period_end = current_period_end
        user.cancel_at_period_end = cancel_at_period_end
        user.plan_tier = effective_tier
        user.plan_updated_at = datetime.now(UTC)

        self.session.add(user)
        self.session.flush()

        cancelled_ids: list[tuple[str, UUID]] = []
        if (
            effective_tier == PlanTier.FREE.value
            and old_state.get("plan_tier") != PlanTier.FREE.value
            and status != "past_due"
        ):
            cancelled_ids = self.cancel_in_flight_jobs(user.id)
        self._pending_cancellation_events.extend(cancelled_ids)

        try:
            log_billing_event(
                user_id=user.id,
                event_type="subscription.synced",
                subscription_id=subscription_id,
                old_state=old_state,
                new_state={
                    "plan_tier": effective_tier,
                    "subscription_status": status,
                    "billing_interval": billing_interval,
                    "price_id": price_id,
                    "cancel_at_period_end": cancel_at_period_end,
                },
                session=self.session,
            )
        except Exception:
            logger.warning("billing.log_event_failed", user_id=str(user.id), exc_info=True)
        logger.info(
            "billing.subscription.synced",
            user_id=str(user.id),
            subscription_id=subscription_id,
            plan_tier=effective_tier,
            status=status,
        )

    def _reconcile_subscriptions_impl(self, *, grace_hours: int = 48, dry_run: bool = False) -> list[dict[str, Any]]:
        """Reconcile local subscription records with Stripe.

        Finds users whose subscription is locally marked 'active' but whose
        ``subscription_current_period_end`` is older than *grace_hours* ago,
        then fetches the current subscription from Stripe and updates the
        local record.  Designed to be called from a management command or
        scheduled task.

        Returns a list of reconciliation actions taken (or that would be
        taken, if *dry_run* is True).
        """
        cutoff = datetime.now(UTC) - timedelta(hours=grace_hours)
        stale_user_query = select(User).where(
            User.subscription_status == "active",
            User.subscription_current_period_end < cutoff,
            User.stripe_subscription_id.isnot(None),
        ).with_for_update(skip_locked=True).limit(self.settings.max_reconciliation_users)
        # Keep a non-configurable ceiling so a bad config change cannot turn
        # reconciliation into an unbounded Stripe API burst.
        stale_user_query = stale_user_query.limit(100)
        stale_users: list[User] = list(self.session.scalars(stale_user_query))
        actions: list[dict[str, Any]] = []
        client = self._get_stripe_client() if stale_users else None

        for user in stale_users:
            sub_id = user.stripe_subscription_id
            action: dict[str, Any] = {
                "user_id": str(user.id),
                "stripe_subscription_id": sub_id,
                "local_status": user.subscription_status,
                "local_period_end": user.subscription_current_period_end.isoformat()
                if user.subscription_current_period_end
                else None,
            }
            try:
                if client is None:
                    raise RuntimeError("Stripe client must be available when reconciling stale users.")
                subscription = client.subscriptions.retrieve(sub_id)
                stripe_status = str(subscription.get("status", ""))
                action["stripe_status"] = stripe_status
                if dry_run:
                    action["action"] = "would_sync"
                else:
                    # NOTE: session.commit() below releases the FOR UPDATE
                    # lock acquired by the initial SELECT.  A concurrent
                    # reconciliation run could re-select users that we've
                    # already committed.  This is acceptable because:
                    #   1. The operation is idempotent (re-applying the same
                    #      Stripe data produces the same local state).
                    #   2. skip_locked=True in the SELECT prevents contention
                    #      while the original transaction is open.
                    #   3. Duplicate processing only wastes Stripe API calls.
                    nested = self.session.begin_nested()
                    try:
                        self._apply_subscription_to_user(user, subscription)
                        nested.commit()
                    except Exception:
                        nested.rollback()
                        raise
                    self.session.commit()
                    action["action"] = "synced"
                logger.info(
                    "billing.reconcile.synced",
                    user_id=str(user.id),
                    subscription_id=sub_id,
                    stripe_status=stripe_status,
                    dry_run=dry_run,
                )
            except Exception as exc:
                action["action"] = "error"
                action["error"] = str(exc)
                logger.warning(
                    "billing.reconcile.error",
                    user_id=str(user.id),
                    subscription_id=sub_id,
                    error=str(exc),
                )
                self.session.rollback()
            actions.append(action)

        logger.info(
            "billing.reconcile.complete",
            total_checked=len(stale_users),
            actions=len(actions),
            dry_run=dry_run,
        )
        return actions

    _JOB_TYPE_FOR_MODEL: dict[str, str] = {
        "BacktestRun": "backtest",
        "MultiSymbolRun": "multi_symbol_backtest",
        "MultiStepRun": "multi_step_backtest",
        "ScannerJob": "scan",
        "ExportJob": "export",
        "SymbolAnalysis": "analysis",
        "SweepJob": "sweep",
    }

    def cancel_in_flight_jobs(self, user_id: UUID) -> list[tuple[str, UUID]]:
        """Cancel queued/running jobs when a user's subscription is revoked.

        Flushes cancellations and revokes Celery tasks, but does NOT publish
        SSE events.  The caller must call :meth:`publish_cancellation_events`
        after the enclosing transaction commits so SSE consumers never see
        "cancelled" for jobs that might be rolled back.

        Returns the list of ``(job_type, job_id)`` pairs that were cancelled,
        for the caller to pass to :meth:`publish_cancellation_events`.
        """
        from sqlalchemy import update as sa_update

        from backtestforecast.models import (
            BacktestRun,
            ExportJob,
            MultiStepRun,
            MultiSymbolRun,
            ScannerJob,
            SweepJob,
            SymbolAnalysis,
        )

        _ACTIVE = ("queued", "running")
        task_ids: list[str] = []
        cancelled = 0
        cancelled_job_ids: list[tuple[str, UUID]] = []
        now = datetime.now(UTC)
        for model_cls in (BacktestRun, MultiSymbolRun, MultiStepRun, ScannerJob, ExportJob, SymbolAnalysis, SweepJob):
            cancel_values: dict[str, object] = {
                "status": "cancelled",
                "completed_at": now,
                "updated_at": now,
                "error_code": "subscription_revoked",
                "error_message": "Subscription cancelled; in-flight jobs were stopped.",
            }
            has_celery_task_id = hasattr(model_cls, "celery_task_id")
            returning_cols = [model_cls.id]
            if has_celery_task_id:
                returning_cols.append(model_cls.celery_task_id)
            result = self.session.execute(
                sa_update(model_cls)
                .where(model_cls.user_id == user_id, model_cls.status.in_(_ACTIVE))
                .values(**cancel_values)
                .returning(*returning_cols)
            )
            cancelled_rows = result.all()
            for row in cancelled_rows:
                row_id = row[0]
                row_task_id = row[1] if has_celery_task_id and len(row) > 1 else None
                job_type = self._JOB_TYPE_FOR_MODEL.get(model_cls.__name__, "unknown")
                cancelled_job_ids.append((job_type, row_id))
                if row_task_id is not None:
                    task_ids.append(row_task_id)
            cancelled += len(cancelled_rows)

        self.session.flush()

        if task_ids:
            try:
                from apps.worker.app.celery_app import celery_app
            except Exception:
                logger.warning(
                    "billing.celery_import_unavailable",
                    user_id=str(user_id),
                    task_count=len(task_ids),
                    msg="Cannot revoke Celery tasks: worker module not available in this process.",
                )
            else:
                try:
                    for tid in task_ids:
                        celery_app.control.revoke(tid, terminate=False)
                except Exception:
                    logger.warning("billing.celery_revoke_failed", user_id=str(user_id), task_count=len(task_ids))
        if cancelled > 0:
            logger.info("billing.in_flight_jobs_cancelled", user_id=str(user_id), count=cancelled)
            try:
                from backtestforecast.services.audit import AuditService
                audit = AuditService(self.session)
                for job_type, job_id in cancelled_job_ids:
                    audit.record_always(
                        event_type="job.cancelled_by_billing",
                        subject_type=job_type,
                        subject_id=job_id,
                        user_id=user_id,
                        metadata={
                            "reason": "subscription_revoked",
                            "job_type": job_type,
                        },
                    )
                self.session.flush()
            except Exception:
                logger.warning(
                    "billing.audit_record_failed",
                    user_id=str(user_id),
                    count=cancelled,
                    exc_info=True,
                )
        return cancelled_job_ids

    @staticmethod
    def publish_cancellation_events(cancelled_job_ids: list[tuple[str, UUID]]) -> None:
        """Publish SSE "cancelled" events for jobs that were cancelled.

        Must be called AFTER the transaction that cancelled the jobs has been
        committed, so SSE consumers never see a "cancelled" event for a job
        whose cancellation was rolled back.
        """
        from backtestforecast.events import publish_job_status

        for job_type, job_id in cancelled_job_ids:
            try:
                publish_job_status(job_type, job_id, "cancelled", metadata={"error_code": "subscription_revoked"})
            except Exception:
                logger.debug("billing.sse_publish_failed", job_type=job_type, job_id=str(job_id))

    def _get_or_create_customer(self, user: User) -> str:
        if user.stripe_customer_id:
            return user.stripe_customer_id

        from sqlalchemy import text
        self.session.execute(
            text("SELECT pg_advisory_xact_lock(('x' || left(md5(:key), 16))::bit(64)::bigint)"),
            {"key": f"stripe_customer:{user.id}"},
        )

        self.session.refresh(user)
        if user.stripe_customer_id:
            return user.stripe_customer_id

        client = self._get_stripe_client()
        customer = client.customers.create(
            params={
                "email": user.email,
                "metadata": {
                    "user_id": str(user.id),
                    "clerk_user_id": user.clerk_user_id,
                },
            }
        )
        result = self.session.execute(
            sa_update(User)
            .where(User.id == user.id, User.stripe_customer_id.is_(None))
            .values(stripe_customer_id=customer.id, updated_at=datetime.now(UTC))
        )
        self.session.flush()
        if result.rowcount == 0:
            try:
                client.customers.delete(customer.id)
            except Exception:
                logger.warning(
                    "billing.orphan_customer_cleanup_failed",
                    customer_id_hash=short_hash(customer.id),
                )
                try:
                    from apps.api.app.dispatch import dispatch_outbox_task
                    from backtestforecast.db.session import create_session

                    with create_session() as cleanup_session:
                        dispatch_outbox_task(
                            db=cleanup_session,
                            task_name="maintenance.cleanup_stripe_orphan",
                            task_kwargs={"customer_id": customer.id, "subscription_id": None, "user_id_str": str(user.id)},
                            queue="recovery",
                            logger=logger,
                        )
                except Exception:
                    logger.warning("billing.orphan_customer_cleanup_dispatch_failed", customer_id_hash=short_hash(customer.id))
            self.session.refresh(user)
            if user.stripe_customer_id is None:
                raise ExternalServiceError(
                    "Failed to resolve Stripe customer after concurrent creation race."
                )
            return user.stripe_customer_id
        return customer.id

    def _find_user_by_metadata(self, payload: Any) -> User | None:
        metadata = payload.get("metadata") or {}
        user_id_raw = metadata.get("user_id") or payload.get("client_reference_id")
        if not user_id_raw:
            return None
        try:
            user_id = UUID(str(user_id_raw))
        except (TypeError, ValueError):
            return None
        return self.users.get_by_id(user_id)

    def _price_id_for(self, tier: str, billing_interval: str) -> str:
        price_id = self.settings.stripe_price_lookup.get((tier, billing_interval))
        if price_id:
            return price_id
        raise ConfigurationError(f"Stripe price ID is not configured for {tier}/{billing_interval}.")

    def _configured_tier_for_price(self, price_id: str | None) -> str | None:
        if not price_id:
            return None
        for (tier, _interval), configured_price_id in self.settings.stripe_price_lookup.items():
            if configured_price_id == price_id:
                return tier
        logger.warning(
            "billing.unknown_price_id",
            price_id=price_id,
            configured_prices=list(self.settings.stripe_price_lookup.values()),
            hint="This price ID is not in the configured stripe_price_lookup. "
                 "User may be downgraded to free if metadata also lacks requested_tier.",
        )
        return None

    @staticmethod
    def _tier_from_metadata(subscription: Any) -> str | None:
        metadata = subscription.get("metadata") if isinstance(subscription, dict) else None
        if not isinstance(metadata, dict):
            return None
        requested_tier = metadata.get("requested_tier")
        if requested_tier in {PlanTier.PRO.value, PlanTier.PREMIUM.value}:
            return requested_tier
        return None

    def _extract_price_details(self, subscription: Any) -> tuple[str | None, str | None]:
        """Extract plan price ID and billing interval from subscription items.

        When the subscription has multiple line items, each item's price is
        matched against the configured price lookup. The first item whose
        price_id is a known plan price is used. If none match, the first item
        is used as a fallback.
        """
        items = subscription.get("items", {}).get("data", []) if isinstance(subscription, dict) else []
        if not items:
            return None, None
        known_price_ids = set(self.settings.stripe_price_lookup.values())
        for item in items:
            price = item.get("price", {})
            pid = price.get("id") if isinstance(price, dict) else None
            if pid and pid in known_price_ids:
                return self._interval_from_price(price)
        if len(items) > 1:
            logger.warning(
                "billing.multi_item_subscription_no_plan_match",
                item_count=len(items),
                sub_id=subscription.get("id") if isinstance(subscription, dict) else None,
            )
        price = items[0].get("price", {})
        return self._interval_from_price(price)

    @staticmethod
    def _interval_from_price(price: Any) -> tuple[str | None, str | None]:
        price_id = price.get("id") if isinstance(price, dict) else None
        recurring = price.get("recurring", {}) if isinstance(price, dict) else {}
        interval = recurring.get("interval") if isinstance(recurring, dict) else None
        if interval == "month":
            return price_id, BillingInterval.MONTHLY.value
        if interval == "year":
            return price_id, BillingInterval.YEARLY.value
        logger.warning("billing.unknown_interval", interval=interval, price_id=price_id)
        return price_id, None

    def _resolve_return_url(self, return_path: str | None) -> str:
        return resolve_return_url(self.settings.app_public_url, return_path)

    @staticmethod
    def _normalize_utc(dt: datetime) -> datetime:
        """Ensure a datetime is timezone-aware (UTC) for safe comparison.

        SQLite-backed test sessions may return timezone-naive datetimes even
        when the column is declared with ``timezone=True``.  PostgreSQL always
        returns timezone-aware values, so this is a no-op in production.

        WARNING: This assumes naive datetimes are UTC.  If the runtime
        environment's local time differs from UTC, naive datetimes from
        SQLite will be misinterpreted. Production (Postgres) is unaffected.
        """
        if dt.tzinfo is None:
            import os
            if os.environ.get("TZ") and os.environ["TZ"] not in ("UTC", "Etc/UTC"):
                logger.warning(
                    "billing.naive_datetime_non_utc_tz",
                    tz=os.environ.get("TZ"),
                    value=dt.isoformat(),
                    msg="Naive datetime interpreted as UTC but TZ is set to a non-UTC value.",
                )
            return dt.replace(tzinfo=UTC)
        return dt

    @staticmethod
    def _coerce_stripe_id(value: Any) -> str | None:
        if isinstance(value, str):
            return value
        if isinstance(value, dict):
            identifier = value.get("id")
            return identifier if isinstance(identifier, str) else None
        identifier = getattr(value, "id", None)
        if isinstance(identifier, str):
            return identifier
        return None

    @staticmethod
    def _timestamp_to_datetime(value: Any) -> datetime | None:
        if value in {None, ""}:
            return None
        try:
            ts = int(value)
            if ts < 0:
                logger.warning("billing.negative_timestamp", value=value)
                return None
            return datetime.fromtimestamp(ts, tz=UTC)
        except (TypeError, ValueError, OSError):
            return None

    def get_stripe_client(self, *, skip_circuit_check: bool = False):
        """Return the Stripe SDK client, initialising it on first call.

        Checks the circuit-breaker key in Redis before returning. Pass
        ``skip_circuit_check=True`` to bypass (e.g. during account cleanup
        where the circuit state is irrelevant).
        """
        return self._get_stripe_client(skip_circuit_check=skip_circuit_check)

    def _get_stripe_client(self, *, skip_circuit_check: bool = False):
        if not skip_circuit_check:
            try:
                from backtestforecast.security import get_rate_limiter
                r = get_rate_limiter().get_redis()
                if r is not None and r.exists(_STRIPE_CIRCUIT_KEY):
                    raise ExternalServiceError("Stripe is temporarily unavailable. Please try again shortly.")
            except ExternalServiceError:
                raise
            except (ConnectionError, OSError, TimeoutError, RuntimeError):
                logger.debug("billing.circuit_check_skipped", reason="redis_unavailable")
        if self._stripe_client is not None:
            return self._stripe_client
        if not self.settings.stripe_secret_key or not self.settings.stripe_webhook_secret:
            raise ConfigurationError("Stripe billing is not configured.")
        try:
            import stripe  # type: ignore
        except ImportError as exc:  # pragma: no cover - environment dependent
            raise ConfigurationError("The Stripe SDK is not installed.") from exc
        self._stripe_client = stripe.StripeClient(self.settings.stripe_secret_key)
        return self._stripe_client

    def _mark_stripe_event_error(self, stripe_event_id: str, detail: str, *, event_type: str = "unknown", livemode: bool = False) -> None:
        """Best-effort: mark a claimed stripe event as errored."""
        rows_updated = 0
        try:
            result = self.stripe_events.mark_error(stripe_event_id, detail)
            rows_updated = 1 if result else 0
            self.session.commit()
        except Exception:
            self.session.rollback()

        if rows_updated == 0:
            try:
                from backtestforecast.models import StripeEvent
                error_event = StripeEvent(
                    stripe_event_id=stripe_event_id,
                    event_type=event_type,
                    livemode=livemode,
                    idempotency_status="error",
                    error_detail=detail[:2000] if detail else None,
                )
                nested = self.session.begin_nested()
                self.session.add(error_event)
                try:
                    nested.commit()
                except Exception:
                    nested.rollback()
                    return
                try:
                    self.session.commit()
                except Exception:
                    self.session.rollback()
                    logger.warning("billing.error_event_commit_failed", exc_info=True)
            except Exception:
                self.session.rollback()
                logger.debug("billing.stripe_event_error_mark_failed", event_id=stripe_event_id)

    def _trip_stripe_circuit(self) -> None:
        cooldown = _get_stripe_circuit_cooldown()
        try:
            from backtestforecast.security import get_rate_limiter
            r = get_rate_limiter().get_redis()
            if r is not None:
                r.setex(_STRIPE_CIRCUIT_KEY, cooldown, "1")
        except (ConnectionError, OSError, TimeoutError, RuntimeError):
            logger.debug("billing.circuit_trip_skipped", reason="redis_unavailable")
        logger.warning("billing.stripe_circuit_opened", cooldown_seconds=cooldown)
