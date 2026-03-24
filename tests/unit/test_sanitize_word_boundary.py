"""Item 80: Test sanitize regex word boundary behavior.

Verifies that sanitize_error_message correctly redacts SQL keywords like
"SELECT" but does NOT redact words that merely *contain* those keywords
(e.g., "reselect", "updated", "insertion").
"""
from __future__ import annotations

from backtestforecast.schemas.common import sanitize_error_message


class TestSanitizeWordBoundaryBehavior:
    def test_standalone_select_is_redacted(self):
        result = sanitize_error_message("Failed during SELECT * FROM users")
        assert result == "An internal error occurred."

    def test_standalone_insert_is_redacted(self):
        result = sanitize_error_message("Error in INSERT INTO orders")
        assert result == "An internal error occurred."

    def test_standalone_update_is_redacted(self):
        result = sanitize_error_message("Error in UPDATE users SET name='x'")
        assert result == "An internal error occurred."

    def test_standalone_delete_is_redacted(self):
        result = sanitize_error_message("Error in DELETE FROM sessions")
        assert result == "An internal error occurred."

    def test_standalone_drop_is_redacted(self):
        result = sanitize_error_message("Error in DROP TABLE users")
        assert result == "An internal error occurred."

    def test_reselect_not_redacted(self):
        """The word 'reselect' contains 'select' but should NOT trigger
        redaction because the regex uses \\b word boundaries."""
        result = sanitize_error_message("Please reselect your option")
        assert result == "Please reselect your option"

    def test_updated_not_redacted(self):
        """'updated' contains 'update' but should NOT trigger redaction."""
        result = sanitize_error_message("The record was updated successfully")
        assert result == "The record was updated successfully"

    def test_insertion_not_redacted(self):
        """'insertion' contains 'insert' but should NOT trigger redaction."""
        result = sanitize_error_message("Insertion point found")
        assert result == "Insertion point found"

    def test_deleted_not_redacted(self):
        """'deleted' contains 'delete' but should NOT trigger redaction."""
        result = sanitize_error_message("File was deleted by user")
        assert result == "File was deleted by user"

    def test_dropdown_not_redacted(self):
        """'dropdown' contains 'drop' but should NOT trigger redaction."""
        result = sanitize_error_message("Open the dropdown menu")
        assert result == "Open the dropdown menu"

    def test_case_insensitive_select(self):
        result = sanitize_error_message("error in select * from users")
        assert result == "An internal error occurred."

    def test_none_passthrough(self):
        assert sanitize_error_message(None) is None

    def test_safe_message_unchanged(self):
        msg = "Something went wrong, please try again."
        assert sanitize_error_message(msg) == msg

    def test_long_message_truncated(self):
        msg = "A" * 600
        result = sanitize_error_message(msg)
        assert result is not None
        assert len(result) <= 504  # 500 + "..."
        assert result.endswith("...")
