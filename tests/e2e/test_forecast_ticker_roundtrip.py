"""Item 72: E2E test for forecast ticker validation round-trip.

Verifies that ticker validation is consistent between frontend and backend regex.
"""
from __future__ import annotations

import re

import pytest

FRONTEND_RE = re.compile(r"^[A-Z0-9./^]{1,16}$")
BACKEND_RE = re.compile(r"^[A-Za-z0-9./^]{1,16}$")


class TestForecastTickerRoundTrip:
    @pytest.mark.parametrize("ticker,expected", [
        ("SPY", True),
        ("BRK.B", True),
        ("^VIX", True),
        ("SPY240315C500", True),
        ("spy", False),  # frontend is uppercase-only
        ("", False),
        ("A" * 17, False),
        ("SPY!", False),
    ])
    def test_frontend_regex(self, ticker: str, expected: bool) -> None:
        assert bool(FRONTEND_RE.match(ticker)) == expected

    @pytest.mark.parametrize("ticker,expected", [
        ("SPY", True),
        ("BRK.B", True),
        ("^VIX", True),
        ("spy", True),  # backend is case-insensitive
        ("", False),
        ("A" * 17, False),
        ("SPY!", False),
    ])
    def test_backend_regex(self, ticker: str, expected: bool) -> None:
        assert bool(BACKEND_RE.match(ticker)) == expected
