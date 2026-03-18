"""Canary test: verify that the CSP header in middleware.ts includes
both clerk.dev and clerk.com domains so Clerk works in dev and production.

CSP was moved from next.config.ts static headers to middleware.ts where it
is generated per-request with a unique nonce via ``buildCSP()``.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

MIDDLEWARE_PATH = Path(__file__).resolve().parent.parent.parent / "apps" / "web" / "middleware.ts"

REQUIRED_CLERK_DOMAINS = [
    "clerk.dev",
    "clerk.com",
]


@pytest.fixture
def csp_value() -> str:
    """Extract the CSP directives from the buildCSP function in middleware.ts."""
    content = MIDDLEWARE_PATH.read_text(encoding="utf-8")
    match = re.search(r"function\s+buildCSP[^{]*\{(.*?)\n\}", content, re.DOTALL)
    assert match, "Could not find buildCSP function in middleware.ts"
    return match.group(1)


@pytest.mark.parametrize("domain", REQUIRED_CLERK_DOMAINS)
def test_csp_includes_clerk_domain(csp_value: str, domain: str) -> None:
    assert domain in csp_value, (
        f"CSP header must include '{domain}' for Clerk to function. "
        f"Check apps/web/middleware.ts buildCSP function."
    )


def test_csp_script_src_includes_strict_dynamic(csp_value: str) -> None:
    """script-src must include 'strict-dynamic' for nonce-based CSP."""
    assert "script-src" in csp_value, "CSP must contain a script-src directive"
    assert "'strict-dynamic'" in csp_value, (
        "script-src must include 'strict-dynamic' for nonce-based CSP. "
        "Check apps/web/middleware.ts buildCSP function."
    )
