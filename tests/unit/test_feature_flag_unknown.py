"""Test that unknown feature names return False and log an error."""
from __future__ import annotations

from unittest.mock import MagicMock, patch
from uuid import uuid4

from backtestforecast.feature_flags import is_feature_enabled


class TestUnknownFeatureFlag:
    def test_unknown_feature_returns_false(self):
        settings = MagicMock()
        with patch("backtestforecast.feature_flags.get_settings", return_value=settings):
            result = is_feature_enabled("unknown_feature_name")
        assert result is False

    def test_unknown_feature_returns_false_with_user_id(self):
        settings = MagicMock()
        with patch("backtestforecast.feature_flags.get_settings", return_value=settings):
            result = is_feature_enabled(
                "unknown_feature_name", user_id=uuid4(), plan_tier="pro",
            )
        assert result is False

    def test_unknown_feature_logs_error(self):
        settings = MagicMock()
        with (
            patch("backtestforecast.feature_flags.get_settings", return_value=settings),
            patch("backtestforecast.feature_flags.logger") as mock_logger,
        ):
            is_feature_enabled("unknown_feature_name")

        mock_logger.error.assert_called_once_with(
            "feature_flags.unknown_feature", feature="unknown_feature_name",
        )

    def test_known_features_do_not_trigger_unknown_log(self):
        """Verify that known features go through the normal path (no error log)."""
        settings = MagicMock()
        settings.feature_backtests_enabled = True
        settings.feature_backtests_rollout_pct = 100
        settings.feature_backtests_tiers = ""
        settings.feature_backtests_allow_user_ids = ""
        with (
            patch("backtestforecast.feature_flags.get_settings", return_value=settings),
            patch("backtestforecast.feature_flags.logger") as mock_logger,
        ):
            result = is_feature_enabled("backtests", user_id=uuid4())

        assert result is True
        mock_logger.error.assert_not_called()

    def test_empty_string_feature_name_returns_false(self):
        settings = MagicMock()
        with (
            patch("backtestforecast.feature_flags.get_settings", return_value=settings),
            patch("backtestforecast.feature_flags.logger") as mock_logger,
        ):
            result = is_feature_enabled("")
        assert result is False
        mock_logger.error.assert_called_once()
