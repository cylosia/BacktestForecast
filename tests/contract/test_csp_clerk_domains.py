"""Canary test: verify that the CSP header in next.config.ts includes
both clerk.dev and clerk.com domains so Clerk works in dev and production."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

NEXT_CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "apps" / "web" / "next.config.ts"

REQUIRED_CLERK_DOMAINS = [
    "clerk.dev",
    "clerk.com",
]


@pytest.fixture
def csp_value() -> str:
    """Extract the raw Content-Security-Policy value string from next.config.ts."""
    content = NEXT_CONFIG_PATH.read_text(encoding="utf-8")
    match = re.search(r'"Content-Security-Policy".*?value:\s*`([^`]+)`', content, re.DOTALL)
    assert match, "Could not find Content-Security-Policy value in next.config.ts"
    return match.group(1)


@pytest.mark.parametrize("domain", REQUIRED_CLERK_DOMAINS)
def test_csp_includes_clerk_domain(csp_value: str, domain: str) -> None:
    assert domain in csp_value, (
        f"CSP header must include '{domain}' for Clerk to function. "
        f"Check apps/web/next.config.ts Content-Security-Policy value."
    )


def test_csp_script_src_includes_unsafe_inline(csp_value: str) -> None:
    """Item 91: script-src must include 'unsafe-inline' for Next.js inline scripts."""
    script_src_match = re.search(r"script-src\s+([^;]+)", csp_value)
    assert script_src_match, "CSP header must contain a script-src directive"
    script_src = script_src_match.group(1)
    assert "'unsafe-inline'" in script_src, (
        f"script-src must include 'unsafe-inline' for Next.js inline scripts. "
        f"Got: script-src {script_src}"
    )
