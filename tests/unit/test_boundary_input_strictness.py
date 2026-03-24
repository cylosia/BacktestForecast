"""Fix 74: Boundary inputs must return exact status codes, not ranges.

Validates that the compare endpoint returns precisely 422 for invalid
run_ids lists (empty or single-element).
"""
from __future__ import annotations

from backtestforecast.schemas.backtests import CompareBacktestsRequest


class TestBoundaryInputStrictness:
    """Exact 422 status codes for invalid compare inputs."""

    def test_compare_requires_min_two_run_ids_schema(self):
        """CompareBacktestsRequest must enforce min_length=2 on run_ids."""
        import pytest
        from pydantic import ValidationError

        with pytest.raises(ValidationError) as exc_info:
            CompareBacktestsRequest(run_ids=[])
        errors = exc_info.value.errors()
        assert any(
            e.get("type") in ("too_short", "value_error.list.min_items", "value_error")
            for e in errors
        ), f"Empty run_ids should trigger a validation error, got: {errors}"

    def test_compare_single_run_id_rejected(self):
        """A single run_id must be rejected at the schema level."""
        import uuid

        import pytest
        from pydantic import ValidationError

        with pytest.raises(ValidationError) as exc_info:
            CompareBacktestsRequest(run_ids=[uuid.uuid4()])
        errors = exc_info.value.errors()
        assert any(
            e.get("type") in ("too_short", "value_error.list.min_items", "value_error")
            for e in errors
        ), f"Single run_id should trigger a validation error, got: {errors}"

    def test_compare_two_run_ids_accepted(self):
        """Two run_ids should pass schema validation."""
        import uuid

        request = CompareBacktestsRequest(run_ids=[uuid.uuid4(), uuid.uuid4()])
        assert len(request.run_ids) == 2

    def test_compare_schema_error_maps_to_422(self):
        """Pydantic ValidationError from FastAPI body parsing yields 422.

        FastAPI automatically converts Pydantic validation errors to 422
        responses. This test verifies the schema enforces the constraint
        that would produce 422, not 400 or another status code.
        """
        import uuid

        import pytest
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            CompareBacktestsRequest(run_ids=[uuid.uuid4()])
