from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _read(relpath: str) -> str:
    return (ROOT / relpath).read_text(encoding="utf-8")


def test_web_has_fallback_csp_and_nonce_enforced_runtime_policy() -> None:
    next_config = _read("apps/web/next.config.ts")
    middleware = _read("apps/web/middleware.ts")
    layout = _read("apps/web/app/layout.tsx")

    assert "buildFallbackCsp" in next_config
    assert 'key: "Content-Security-Policy"' in next_config
    assert "FALLBACK_CSP" in next_config
    assert "response.headers.set(\"Content-Security-Policy\", csp)" in middleware
    assert 'requestHeaders.set("x-nonce", nonce)' in middleware
    assert 'headersList.get("x-nonce")' in layout
    assert "Missing CSP nonce header" in layout
