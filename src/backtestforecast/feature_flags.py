"""Lightweight feature gate with percentage-based rollouts.

Resolves FIXME(#100) from config.py.  Supports:

1. **Boolean kill-switch**: ``feature_{name}_enabled = False`` disables the
   feature for all users immediately (env var override).
2. **Percentage rollout**: ``feature_{name}_rollout_pct = 50`` enables the
   feature for ~50% of users.  Uses a deterministic CRC32 hash of
   ``user_id`` so the same user always gets a consistent experience.
3. **Tier targeting**: ``feature_{name}_tiers = "pro,premium"`` restricts
   the feature to specific plan tiers during beta.
4. **Allow-list override**: ``feature_{name}_allow_user_ids = "uuid1,uuid2"``
   for individual opt-in during internal testing.

Usage::

    from backtestforecast.feature_flags import is_feature_enabled

    if not is_feature_enabled("sweeps", user_id=user.id, plan_tier=user.plan_tier):
        raise FeatureLockedError("Sweeps are temporarily disabled.")
"""
from __future__ import annotations

import hashlib
from uuid import UUID

import structlog

from backtestforecast.config import get_settings

logger = structlog.get_logger("feature_flags")


def _deterministic_bucket(user_id: UUID, feature_name: str) -> int:
    """Return a stable 0-99 bucket for a (user_id, feature_name) pair.

    Uses CRC32 for speed. The feature_name is included so different
    features can roll out to different user cohorts independently.
    """
    import binascii
    key = f"{user_id}:{feature_name}".encode("utf-8")
    return binascii.crc32(key) % 100


def is_feature_enabled(
    feature_name: str,
    *,
    user_id: UUID | None = None,
    plan_tier: str | None = None,
) -> bool:
    """Check whether a feature is enabled for a specific user.

    Evaluation order (short-circuits on first decisive result):
    1. Boolean kill-switch (``feature_{name}_enabled``): if False → disabled
    2. Allow-list (``feature_{name}_allow_user_ids``): if user_id matches → enabled
    3. Tier targeting (``feature_{name}_tiers``): if set and tier doesn't match → disabled
    4. Percentage rollout (``feature_{name}_rollout_pct``): if < 100, hash user_id
    5. Default: enabled
    """
    settings = get_settings()

    enabled_attr = f"feature_{feature_name}_enabled"
    enabled = getattr(settings, enabled_attr, None)
    if enabled is False:
        return False

    if user_id is not None:
        allow_attr = f"feature_{feature_name}_allow_user_ids"
        allow_raw = getattr(settings, allow_attr, None)
        if isinstance(allow_raw, str) and allow_raw.strip():
            allow_ids = {uid.strip() for uid in allow_raw.split(",") if uid.strip()}
            if str(user_id) in allow_ids:
                logger.debug("feature_flags.allow_list_match", feature=feature_name, user_id=str(user_id))
                return True

    tiers_attr = f"feature_{feature_name}_tiers"
    tiers_raw = getattr(settings, tiers_attr, None)
    if isinstance(tiers_raw, str) and tiers_raw.strip():
        allowed_tiers = {t.strip().lower() for t in tiers_raw.split(",") if t.strip()}
        effective_tier = (plan_tier or "free").lower()
        if effective_tier not in allowed_tiers:
            logger.debug(
                "feature_flags.tier_excluded",
                feature=feature_name,
                tier=effective_tier,
                allowed=sorted(allowed_tiers),
            )
            return False

    rollout_attr = f"feature_{feature_name}_rollout_pct"
    rollout_pct = getattr(settings, rollout_attr, None)
    if isinstance(rollout_pct, int) and rollout_pct < 100:
        if rollout_pct <= 0:
            return False
        if user_id is None:
            return True
        bucket = _deterministic_bucket(user_id, feature_name)
        in_rollout = bucket < rollout_pct
        if not in_rollout:
            logger.debug(
                "feature_flags.rollout_excluded",
                feature=feature_name,
                user_id=str(user_id),
                bucket=bucket,
                rollout_pct=rollout_pct,
            )
        return in_rollout

    return True
