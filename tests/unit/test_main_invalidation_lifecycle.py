from __future__ import annotations

from unittest.mock import MagicMock


def test_register_startup_invalidation_callbacks_is_idempotent(monkeypatch) -> None:
    from apps.api.app import main

    registrations: list[object] = []

    monkeypatch.setattr(main, "register_invalidation_callback", lambda callback: registrations.append(callback))
    monkeypatch.setattr(main, "_startup_invalidation_callbacks_registered", False)

    main._register_startup_invalidation_callbacks()
    main._register_startup_invalidation_callbacks()

    assert registrations == [
        main.reset_trusted_networks,
        main.reset_token_verifier,
        main._invalidate_dlq_redis,
    ]


def test_invalidate_dlq_redis_closes_previous_client(monkeypatch) -> None:
    from apps.api.app import main

    fake_client = MagicMock()
    monkeypatch.setattr(main, "_dlq_redis", fake_client)

    main._invalidate_dlq_redis()

    fake_client.close.assert_called_once_with()
    assert main._dlq_redis is None
