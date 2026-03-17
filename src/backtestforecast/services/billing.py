from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select, update as sa_update
from sqlalchemy.orm import Session

from backtestforecast.billing.entitlements import PAID_STATUSES, BillingInterval, PlanTier
from backtestforecast.billing.events import log_billing_event
from backtestforecast.billing.urls import resolve_return_url
from backtestforecast.config import Settings, get_settings
from backtestforecast.errors import (
    AuthenticationError,
    ConfigurationError,
    ExternalServiceError,
    NotFoundError,
    ValidationError,
)
from backtestforecast.models import User
from backtestforecast.observability import get_logger
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

logger = get_logger("billing")


_STRIPE_CIRCUIT_COOLDOWN = 30
_STRIPE_CIRCUIT_KEY = "bff:stripe_circuit_open"


class BillingService:
    def __init__(self, session: Session, settings: Settings | None = None) -> None:
        self.session = session
        self.settings = settings or get_settings()
        self.users = UserRepository(session)
        self.audit = AuditService(session)
        self.audit_events = AuditEventRepository(session)
        self.stripe_events = StripeEventRepository(session)
        self._stripe_client: Any = None

    def create_checkout_session(
        self,
        user: User,
        payload: CreateCheckoutSessionRequest,
        *,
        request_id: str | None = None,
        ip_address: str | None = None,
    ) -> CheckoutSessionResponse:
        if payload.tier == PlanTier.FREE.value:
            raise ValidationError("Free does not require a Stripe checkout session.")
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

    def create_portal_session(
        self,
        user: User,
        payload: CreatePortalSessionRequest,
        *,
        request_id: str | None = None,
        ip_address: str | None = None,
    ) -> PortalSessionResponse:
        client = self._get_stripe_client()
        if not user.stripe_customer_id:
            raise NotFoundError("No Stripe customer is attached to this account yet.")
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

    def handle_webhook(
        self,
        payload_bytes: bytes,
        signature_header: str | None,
        *,
        request_id: str | None = None,
        ip_address: str | None = None,
    ) -> dict[str, str]:
        client = self._get_stripe_client()
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
        event_id = self._coerce_stripe_id(event.get("id")) or str(event.get("id") or "")
        if not event_id:
            logger.warning("billing.webhook.missing_event_id", event_type=event_type)
            STRIPE_WEBHOOK_EVENTS_TOTAL.labels(event_type=event_type, result="ignored").inc()
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
            STRIPE_WEBHOOK_EVENTS_TOTAL.labels(event_type=event_type, result="duplicate").inc()
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
                self._sync_subscription(data_object)
            else:
                logger.info("billing.webhook.ignored", event_type=event_type)
        except ExternalServiceError as ese:
            self.session.rollback()
            self._mark_stripe_event_error(event_id, str(ese))
            if not isinstance(ese.__cause__, NotFoundError) and "not found" not in str(ese).lower():
                self._trip_stripe_circuit()
            raise
        except NotFoundError as nfe:
            self.session.rollback()
            self._mark_stripe_event_error(event_id, str(nfe))
            raise
        except Exception:
            self.session.rollback()
            self._mark_stripe_event_error(event_id, "Unhandled processing error")
            logger.exception("billing.webhook.processing_error", event_id=event_id, event_type=event_type)
            STRIPE_WEBHOOK_EVENTS_TOTAL.labels(event_type=event_type, result="error").inc()
            raise

        self.session.commit()
        STRIPE_WEBHOOK_EVENTS_TOTAL.labels(event_type=event_type, result="ok").inc()
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

    def _sync_subscription(self, subscription: Any) -> User | None:
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
        self._apply_subscription_to_user(user, subscription)
        return user

    def _apply_subscription_to_user(self, user: User, subscription: Any) -> None:
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
        if status not in PAID_STATUSES:
            effective_tier = PlanTier.FREE.value

        if (
            subscription_id is not None
            and user.stripe_subscription_id is not None
            and subscription_id != user.stripe_subscription_id
            and user.subscription_status in ("active", "trialing", "past_due")
            and current_period_end is not None
            and user.subscription_current_period_end is not None
            and self._normalize_utc(current_period_end) < self._normalize_utc(user.subscription_current_period_end)
        ):
            logger.info(
                "billing.subscription.stale_subscription_event_skipped",
                user_id=str(user.id),
                incoming_subscription_id=subscription_id,
                current_subscription_id=user.stripe_subscription_id,
                current_status=user.subscription_status,
            )
            return

        is_terminal = status in ("canceled", "unpaid", "incomplete_expired")
        if (
            not is_terminal
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

        if effective_tier == PlanTier.FREE.value and old_state.get("plan_tier") != PlanTier.FREE.value:
            self._cancel_in_flight_jobs(user.id)

        try:
            log_billing_event(
                user_id=user.id,
                event_type="subscription.synced",
                subscription_id=subscription_id,
                old_state=old_state,
                new_state={"plan_tier": effective_tier, "subscription_status": status},
            )
        except Exception:
            logger.warning("billing.log_event_failed", user_id=str(user.id), exc_info=True)

        self.audit.record_always(
            event_type="billing.subscription.synced",
            subject_type="stripe_subscription",
            subject_id=subscription_id,
            user_id=user.id,
            metadata={
                "plan_tier": effective_tier,
                "status": status,
                "billing_interval": billing_interval,
                "price_id": price_id,
                "cancel_at_period_end": cancel_at_period_end,
            },
        )
        logger.info(
            "billing.subscription.synced",
            user_id=str(user.id),
            subscription_id=subscription_id,
            plan_tier=effective_tier,
            status=status,
        )

    def _cancel_in_flight_jobs(self, user_id: UUID) -> None:
        """Cancel queued/running jobs when a user's subscription is revoked."""
        from sqlalchemy import update as sa_update
        from backtestforecast.models import BacktestRun, ScannerJob, ExportJob, SymbolAnalysis
        _ACTIVE = ("queued", "running")
        task_ids: list[str] = []
        cancelled = 0
        for model_cls in (BacktestRun, ScannerJob, ExportJob, SymbolAnalysis):
            rows = self.session.execute(
                select(model_cls.celery_task_id).where(
                    model_cls.user_id == user_id,
                    model_cls.status.in_(_ACTIVE),
                    model_cls.celery_task_id.isnot(None),
                )
            ).scalars().all()
            task_ids.extend(rows)
            result = self.session.execute(
                sa_update(model_cls)
                .where(model_cls.user_id == user_id, model_cls.status.in_(_ACTIVE))
                .values(status="cancelled")
            )
            cancelled += result.rowcount
        if task_ids:
            try:
                from apps.worker.app.celery_app import celery_app
            except ImportError:
                logger.warning(
                    "billing.celery_import_unavailable",
                    user_id=str(user_id),
                    task_count=len(task_ids),
                    msg="Cannot revoke Celery tasks: worker module not importable in this process.",
                )
            else:
                try:
                    for tid in task_ids:
                        celery_app.control.revoke(tid, terminate=True, signal="SIGTERM")
                except Exception:
                    logger.warning("billing.celery_revoke_failed", user_id=str(user_id), task_count=len(task_ids))
        if cancelled > 0:
            logger.info("billing.in_flight_jobs_cancelled", user_id=str(user_id), count=cancelled)

    def _get_or_create_customer(self, user: User) -> str:
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
            .values(stripe_customer_id=customer.id)
        )
        self.session.flush()
        if result.rowcount == 0:
            try:
                client.customers.delete(customer.id)
            except Exception:
                logger.warning("billing.orphan_customer_cleanup_failed", customer_id=customer.id)
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
        if len(items) > 1:
            for item in items:
                price = item.get("price", {})
                pid = price.get("id") if isinstance(price, dict) else None
                if pid and pid in known_price_ids:
                    return self._interval_from_price(price)
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
        """
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt

    @staticmethod
    def _coerce_stripe_id(value: Any) -> str | None:
        if isinstance(value, str):
            return value
        if isinstance(value, dict):
            identifier = value.get("id")
            return identifier if isinstance(identifier, str) else None
        return None

    @staticmethod
    def _timestamp_to_datetime(value: Any) -> datetime | None:
        if value in {None, ""}:
            return None
        try:
            return datetime.fromtimestamp(int(value), tz=UTC)
        except (TypeError, ValueError, OSError):
            return None

    def _get_stripe_client(self):
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

    def _mark_stripe_event_error(self, stripe_event_id: str, detail: str) -> None:
        """Best-effort: mark a claimed stripe event as errored."""
        try:
            self.stripe_events.mark_error(stripe_event_id, detail)
            self.session.commit()
        except Exception:
            self.session.rollback()
            logger.debug("billing.stripe_event_error_mark_failed", event_id=stripe_event_id)

    def _trip_stripe_circuit(self) -> None:
        try:
            from backtestforecast.security import get_rate_limiter
            r = get_rate_limiter().get_redis()
            if r is not None:
                r.setex(_STRIPE_CIRCUIT_KEY, _STRIPE_CIRCUIT_COOLDOWN, "1")
        except (ConnectionError, OSError, TimeoutError, RuntimeError):
            logger.debug("billing.circuit_trip_skipped", reason="redis_unavailable")
        logger.warning("billing.stripe_circuit_opened", cooldown_seconds=_STRIPE_CIRCUIT_COOLDOWN)
