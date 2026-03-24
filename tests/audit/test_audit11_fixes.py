"""Tests for audit round 11 - critical runtime crash fixes."""
from __future__ import annotations

import inspect


class TestNormalizeOriginExists:
    """C-1: _normalize_origin must be defined to prevent NameError."""

    def test_normalize_origin_is_importable(self):
        from apps.api.app.dependencies import _normalize_origin
        assert callable(_normalize_origin)

    def test_normalize_origin_strips_and_lowercases(self):
        from apps.api.app.dependencies import _normalize_origin
        assert _normalize_origin("  HTTP://Example.Com/  ") == "http://example.com"

    def test_normalize_origin_strips_trailing_slash(self):
        from apps.api.app.dependencies import _normalize_origin
        assert _normalize_origin("https://app.example.com/") == "https://app.example.com"


class TestVerifyOwnershipSignature:
    """C-2: SSE endpoints must call _verify_ownership with 3 args, not 4."""

    def test_verify_ownership_takes_three_params(self):
        from apps.api.app.routers.events import _verify_ownership
        sig = inspect.signature(_verify_ownership)
        assert len(sig.parameters) == 3, (
            f"_verify_ownership should take 3 params (model, resource_id, user_id), "
            f"got {len(sig.parameters)}: {list(sig.parameters.keys())}"
        )

    def test_sse_endpoints_do_not_pass_db_to_verify_ownership(self):
        """All SSE endpoint source code must call _verify_ownership without db."""
        import apps.api.app.routers.events as events_mod
        source = inspect.getsource(events_mod)
        assert "_verify_ownership, db," not in source, (
            "SSE endpoints must not pass `db` to _verify_ownership"
        )


class TestSecretFieldsNotLeaked:
    """BUG 30: Secret fields must have repr=False."""

    def test_secret_fields_have_repr_false(self):
        from backtestforecast.config import Settings
        secret_fields = [
            "clerk_secret_key", "clerk_jwt_key",
            "stripe_secret_key", "stripe_webhook_secret",
            "massive_api_key", "earnings_api_key",
            "aws_secret_access_key",
        ]
        for field_name in secret_fields:
            field_info = Settings.model_fields.get(field_name)
            if field_info is None:
                continue
            assert field_info.repr is False, (
                f"Settings.{field_name} must have repr=False to prevent "
                f"leaking secrets in tracebacks and logs"
            )


class TestSweepTimeoutSafety:
    """BUG 1: Sweep timeout must not go negative."""

    def test_sweep_timeout_has_minimum_validation(self):
        from backtestforecast.config import Settings
        field = Settings.model_fields["sweep_timeout_seconds"]
        metadata = field.metadata
        has_ge = any(getattr(m, "ge", None) is not None for m in metadata) if metadata else False
        json_schema = field.json_schema_extra
        assert has_ge or (json_schema and json_schema.get("ge")), (
            "sweep_timeout_seconds must have ge=1 validation"
        )

    def test_sweep_execute_hoists_timeout_before_loop(self):
        """Timeout should be read once before the strategy loop, not inside it."""
        from backtestforecast.services.sweeps import SweepService
        source = inspect.getsource(SweepService._execute_sweep)
        lines = source.split("\n")
        loop_start_idx = None
        timeout_idx = None
        for i, line in enumerate(lines):
            if "for strategy_type in" in line and loop_start_idx is None:
                loop_start_idx = i
            if "sweep_timeout" in line and "get_settings" in line:
                timeout_idx = i
                break
        assert timeout_idx is not None, "get_settings().sweep_timeout_seconds must appear in source"
        if loop_start_idx is not None:
            assert timeout_idx < loop_start_idx, (
                "sweep_timeout should be read before the strategy loop, not inside it"
            )


class TestGeneticSweepEmptyResults:
    """BUG 2: Genetic sweep with zero results must not be marked succeeded."""

    def test_genetic_sweep_zero_results_marked_failed(self):
        from backtestforecast.services.sweeps import SweepService
        source = inspect.getsource(SweepService._execute_genetic)
        assert '"sweep_empty"' in source, (
            "Genetic sweep must mark zero-result jobs as failed with error_code='sweep_empty'"
        )
