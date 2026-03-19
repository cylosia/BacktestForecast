"""Fix 65: Frontend TICKER_RE and backend SYMBOL_ALLOWED_CHARS accept/reject
the same inputs.

Both regexes must be equivalent: ^[\\^A-Z][A-Z0-9./^-]{0,15}$
"""
from __future__ import annotations

import re

import pytest

from backtestforecast.schemas.backtests import SYMBOL_ALLOWED_CHARS

FRONTEND_TICKER_RE = re.compile(r"^[\^A-Z][A-Z0-9./^-]{0,15}$")


_VALID_TICKERS = [
    "SPY",
    "^VIX",
    "BRK.B",
    "SPY-A",
]

_INVALID_TICKERS = [
    "1ABC",   # starts with digit
    "",       # empty
    "a",      # lowercase
]


class TestTickerRegexParity:
    @pytest.mark.parametrize("ticker", _VALID_TICKERS)
    def test_valid_ticker_accepted_by_backend(self, ticker: str):
        assert SYMBOL_ALLOWED_CHARS.match(ticker), (
            f"Backend SYMBOL_ALLOWED_CHARS should accept '{ticker}'"
        )

    @pytest.mark.parametrize("ticker", _VALID_TICKERS)
    def test_valid_ticker_accepted_by_frontend(self, ticker: str):
        assert FRONTEND_TICKER_RE.match(ticker), (
            f"Frontend TICKER_RE should accept '{ticker}'"
        )

    @pytest.mark.parametrize("ticker", _INVALID_TICKERS)
    def test_invalid_ticker_rejected_by_backend(self, ticker: str):
        assert not SYMBOL_ALLOWED_CHARS.match(ticker), (
            f"Backend SYMBOL_ALLOWED_CHARS should reject '{ticker}'"
        )

    @pytest.mark.parametrize("ticker", _INVALID_TICKERS)
    def test_invalid_ticker_rejected_by_frontend(self, ticker: str):
        assert not FRONTEND_TICKER_RE.match(ticker), (
            f"Frontend TICKER_RE should reject '{ticker}'"
        )

    @pytest.mark.parametrize("ticker", _VALID_TICKERS + _INVALID_TICKERS)
    def test_frontend_backend_parity(self, ticker: str):
        """Both regexes must agree on every test input."""
        backend_match = bool(SYMBOL_ALLOWED_CHARS.match(ticker))
        frontend_match = bool(FRONTEND_TICKER_RE.match(ticker))
        assert backend_match == frontend_match, (
            f"Parity mismatch for '{ticker}': "
            f"backend={backend_match}, frontend={frontend_match}"
        )

    def test_regex_patterns_are_identical(self):
        """The raw pattern strings must be equivalent."""
        assert SYMBOL_ALLOWED_CHARS.pattern == FRONTEND_TICKER_RE.pattern, (
            f"Pattern mismatch: backend={SYMBOL_ALLOWED_CHARS.pattern!r} "
            f"frontend={FRONTEND_TICKER_RE.pattern!r}"
        )
