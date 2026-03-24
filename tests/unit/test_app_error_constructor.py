"""Regression test: AppError must be called with keyword args."""
import pytest

from backtestforecast.errors import AppError


def test_app_error_requires_code_and_message():
    with pytest.raises(TypeError):
        AppError("single positional arg only")


def test_app_error_correct_usage():
    err = AppError(code="test_code", message="test message")
    assert err.code == "test_code"
    assert err.message == "test message"
    assert err.status_code == 400
