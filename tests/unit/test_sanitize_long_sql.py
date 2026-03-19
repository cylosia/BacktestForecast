"""Fix 22: Error sanitization catches SQL even in messages longer than 500 chars.

Before the fix, ``sanitize_error_message`` truncated the message to 500
characters *before* running the sensitive-pattern regexes. A SQL statement
like ``SELECT <498 chars of columns> FROM users WHERE ...`` would be
truncated to ``SELECT <498 chars>...`` — losing the ``FROM`` and ``WHERE``
keywords that the SQL regex needs to match. The truncated message would
pass through unsanitized, leaking schema details to the client.

After the fix, patterns are checked on the *full* message first, then
truncation happens only if no pattern matched.
"""
from __future__ import annotations

import pytest

from backtestforecast.schemas.common import sanitize_error_message

_SANITIZED = "An internal error occurred."


class TestSanitizeLongSQL:
    """Verify that SQL fragments are caught regardless of message length."""

    def test_short_sql_redacted(self):
        """Baseline: a short SQL message is caught normally."""
        msg = "SELECT id, email FROM users WHERE plan_tier = 'pro'"
        assert sanitize_error_message(msg) == _SANITIZED

    def test_long_sql_where_keywords_past_500_chars(self):
        """The critical case: SQL keywords (FROM/WHERE) appear after the
        500-char truncation boundary. Before the fix, this would leak."""
        columns = ", ".join(f"column_{i:03d}" for i in range(60))
        msg = f"SELECT {columns} FROM users WHERE id = 42"
        assert len(msg) > 500, f"Message must exceed 500 chars, got {len(msg)}"
        assert "FROM" in msg[500:], "FROM keyword must be past the 500-char mark"
        result = sanitize_error_message(msg)
        assert result == _SANITIZED, (
            f"SQL with FROM/WHERE past 500 chars was not redacted. Got: {result!r}"
        )

    def test_long_sql_insert_into_past_500(self):
        """INSERT INTO with the table name past the truncation point."""
        values = ", ".join(f"'{i}'" for i in range(200))
        msg = f"INSERT INTO sensitive_table VALUES ({values})"
        assert len(msg) > 500
        result = sanitize_error_message(msg)
        assert result == _SANITIZED

    def test_long_traceback_past_500(self):
        """Traceback marker 'Traceback (most recent call last)' at the start,
        but file paths past 500 chars, should still be caught by the
        traceback pattern (which only needs the opening line)."""
        msg = "Traceback (most recent call last):\n" + "  " * 250 + 'File "/app/secret.py"'
        assert len(msg) > 500
        result = sanitize_error_message(msg)
        assert result == _SANITIZED

    def test_long_safe_message_truncated_not_redacted(self):
        """A long message with no sensitive content should be truncated, not redacted."""
        msg = "The backtest could not complete because " + "x" * 500
        result = sanitize_error_message(msg)
        assert result is not None
        assert result != _SANITIZED, "Non-sensitive message should not be redacted"
        assert len(result) <= 503
        assert result.endswith("...")

    def test_redis_url_in_long_message(self):
        """Redis connection strings must be caught even in long messages."""
        padding = "Error details: " + "." * 480
        msg = f"{padding} redis://admin:secret@redis.internal:6379/0"
        assert len(msg) > 500
        result = sanitize_error_message(msg)
        assert result == _SANITIZED

    def test_stripe_key_in_long_message(self):
        """Stripe secret keys must be caught regardless of position."""
        padding = "Billing error " + "." * 490
        msg = f"{padding} sk_live_abc123def456ghi789"
        assert len(msg) > 500
        result = sanitize_error_message(msg)
        assert result == _SANITIZED

    def test_password_in_long_message(self):
        """password=<value> patterns must be caught even past 500 chars."""
        padding = "Connection failed " + "." * 490
        msg = f"{padding} password=supersecret123"
        assert len(msg) > 500
        result = sanitize_error_message(msg)
        assert result == _SANITIZED

    def test_internal_ip_in_long_message(self):
        """Internal network URLs must be caught past 500 chars."""
        padding = "Service error " + "." * 490
        msg = f"{padding} http://10.0.1.5:8080/internal/api"
        assert len(msg) > 500
        result = sanitize_error_message(msg)
        assert result == _SANITIZED

    def test_exactly_500_chars_not_truncated(self):
        """A message of exactly 500 chars should pass through without truncation."""
        msg = "A" * 500
        result = sanitize_error_message(msg)
        assert result == msg
        assert not result.endswith("...")

    def test_501_chars_truncated(self):
        """A message of 501 chars should be truncated."""
        msg = "B" * 501
        result = sanitize_error_message(msg)
        assert result is not None
        assert len(result) == 503
        assert result.endswith("...")

    def test_none_input(self):
        assert sanitize_error_message(None) is None

    def test_empty_string(self):
        assert sanitize_error_message("") == ""

    def test_sqlstate_in_long_message(self):
        """SQLSTATE error codes must be caught past truncation boundary."""
        padding = "Database error " + "." * 490
        msg = f"{padding} SQLSTATE[23505]: unique violation"
        assert len(msg) > 500
        result = sanitize_error_message(msg)
        assert result == _SANITIZED
