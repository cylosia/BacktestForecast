"""Test the feature flag percentage rollout system."""
from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

from backtestforecast.feature_flags import _deterministic_bucket, is_feature_enabled


class TestDeterministicBucket:
    def test_same_user_same_feature_always_same_bucket(self):
        uid = uuid.uuid4()
        buckets = [_deterministic_bucket(uid, "sweeps") for _ in range(100)]
        assert len(set(buckets)) == 1

    def test_different_users_get_different_buckets(self):
        buckets = {_deterministic_bucket(uuid.uuid4(), "sweeps") for _ in range(200)}
        assert len(buckets) > 10, "200 random users should spread across many buckets"

    def test_same_user_different_features_can_differ(self):
        uid = uuid.uuid4()
        b1 = _deterministic_bucket(uid, "sweeps")
        b2 = _deterministic_bucket(uid, "analysis")
        # They CAN be the same by chance, but for most UUIDs they'll differ.
        # Just verify both are in range.
        assert 0 <= b1 < 100
        assert 0 <= b2 < 100

    def test_bucket_range(self):
        for _ in range(500):
            b = _deterministic_bucket(uuid.uuid4(), "test")
            assert 0 <= b < 100


class TestIsFeatureEnabled:
    def _mock_settings(self, **overrides):
        mock = MagicMock()
        mock.feature_sweeps_enabled = True
        mock.feature_sweeps_rollout_pct = 100
        mock.feature_sweeps_tiers = ""
        mock.feature_sweeps_allow_user_ids = ""
        for k, v in overrides.items():
            setattr(mock, k, v)
        return mock

    def test_kill_switch_disables_for_all(self):
        settings = self._mock_settings(feature_sweeps_enabled=False)
        with patch("backtestforecast.feature_flags.get_settings", return_value=settings):
            assert is_feature_enabled("sweeps", user_id=uuid.uuid4()) is False

    def test_enabled_by_default(self):
        settings = self._mock_settings()
        with patch("backtestforecast.feature_flags.get_settings", return_value=settings):
            assert is_feature_enabled("sweeps", user_id=uuid.uuid4()) is True

    def test_allow_list_overrides_rollout(self):
        uid = uuid.uuid4()
        settings = self._mock_settings(
            feature_sweeps_rollout_pct=0,
            feature_sweeps_allow_user_ids=str(uid),
        )
        with patch("backtestforecast.feature_flags.get_settings", return_value=settings):
            assert is_feature_enabled("sweeps", user_id=uid) is True

    def test_allow_list_does_not_match_other_users(self):
        allowed = uuid.uuid4()
        other = uuid.uuid4()
        settings = self._mock_settings(
            feature_sweeps_rollout_pct=0,
            feature_sweeps_allow_user_ids=str(allowed),
        )
        with patch("backtestforecast.feature_flags.get_settings", return_value=settings):
            assert is_feature_enabled("sweeps", user_id=other) is False

    def test_tier_targeting_excludes_free(self):
        settings = self._mock_settings(feature_sweeps_tiers="pro,premium")
        with patch("backtestforecast.feature_flags.get_settings", return_value=settings):
            assert is_feature_enabled("sweeps", user_id=uuid.uuid4(), plan_tier="free") is False
            assert is_feature_enabled("sweeps", user_id=uuid.uuid4(), plan_tier="pro") is True
            assert is_feature_enabled("sweeps", user_id=uuid.uuid4(), plan_tier="premium") is True

    def test_percentage_rollout_50_pct(self):
        settings = self._mock_settings(feature_sweeps_rollout_pct=50)
        enabled_count = 0
        total = 500
        with patch("backtestforecast.feature_flags.get_settings", return_value=settings):
            for _ in range(total):
                if is_feature_enabled("sweeps", user_id=uuid.uuid4()):
                    enabled_count += 1
        ratio = enabled_count / total
        assert 0.3 < ratio < 0.7, f"Expected ~50% enabled, got {ratio:.1%}"

    def test_percentage_rollout_0_disables_all(self):
        settings = self._mock_settings(feature_sweeps_rollout_pct=0)
        with patch("backtestforecast.feature_flags.get_settings", return_value=settings):
            results = [is_feature_enabled("sweeps", user_id=uuid.uuid4()) for _ in range(100)]
            assert not any(results)

    def test_percentage_rollout_100_enables_all(self):
        settings = self._mock_settings(feature_sweeps_rollout_pct=100)
        with patch("backtestforecast.feature_flags.get_settings", return_value=settings):
            results = [is_feature_enabled("sweeps", user_id=uuid.uuid4()) for _ in range(100)]
            assert all(results)

    def test_no_user_id_with_partial_rollout_returns_false(self):
        """Without a user_id, partial rollout cannot hash, so returns False."""
        settings = self._mock_settings(feature_sweeps_rollout_pct=50)
        with patch("backtestforecast.feature_flags.get_settings", return_value=settings):
            assert is_feature_enabled("sweeps") is False

    def test_no_user_id_with_full_rollout_returns_true(self):
        """100% rollout does not need a user_id to hash."""
        settings = self._mock_settings(feature_sweeps_rollout_pct=100)
        with patch("backtestforecast.feature_flags.get_settings", return_value=settings):
            assert is_feature_enabled("sweeps") is True

    def test_unknown_feature_returns_false(self):
        settings = self._mock_settings()
        with patch("backtestforecast.feature_flags.get_settings", return_value=settings):
            assert is_feature_enabled("nonexistent_feature", user_id=uuid.uuid4()) is False

    def test_kill_switch_takes_precedence_over_allow_list(self):
        uid = uuid.uuid4()
        settings = self._mock_settings(
            feature_sweeps_enabled=False,
            feature_sweeps_allow_user_ids=str(uid),
        )
        with patch("backtestforecast.feature_flags.get_settings", return_value=settings):
            assert is_feature_enabled("sweeps", user_id=uid) is False

    def test_float_rollout_pct_is_respected(self):
        """A float rollout_pct (e.g. 50.0) must work the same as int 50."""
        settings = self._mock_settings(feature_sweeps_rollout_pct=50.0)
        enabled_count = 0
        total = 500
        with patch("backtestforecast.feature_flags.get_settings", return_value=settings):
            for _ in range(total):
                if is_feature_enabled("sweeps", user_id=uuid.uuid4()):
                    enabled_count += 1
        ratio = enabled_count / total
        assert 0.3 < ratio < 0.7, f"Float rollout 50.0 should enable ~50%, got {ratio:.1%}"

    def test_float_rollout_zero_disables_all(self):
        settings = self._mock_settings(feature_sweeps_rollout_pct=0.0)
        with patch("backtestforecast.feature_flags.get_settings", return_value=settings):
            results = [is_feature_enabled("sweeps", user_id=uuid.uuid4()) for _ in range(100)]
            assert not any(results)
