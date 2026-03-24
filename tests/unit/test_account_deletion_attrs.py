"""Test that account deletion saves user attributes before db.delete().

Regression test for the bug where user.id and user.clerk_user_id were
accessed after db.delete(user) + db.commit(), which would raise
DetachedInstanceError with expire_on_commit=True (the default).
"""
from __future__ import annotations

import inspect


def test_saved_user_id_before_delete():
    """account.py must save user.id to a local variable before db.delete."""
    from apps.api.app.routers import account
    source = inspect.getsource(account.delete_account)
    delete_pos = source.find("db.delete(user)")
    saved_pos = source.find("saved_user_id")
    assert saved_pos >= 0, "delete_account must save user.id to saved_user_id"
    assert saved_pos < delete_pos, (
        "saved_user_id must be assigned BEFORE db.delete(user)"
    )


def test_clerk_user_id_is_captured_before_delete():
    """account.py must capture Clerk identity evidence before db.delete."""
    from apps.api.app.routers import account
    source = inspect.getsource(account.delete_account)
    delete_pos = source.find("db.delete(user)")
    saved_pos = source.find("clerk_user_id_hash")
    assert saved_pos >= 0, (
        "delete_account must capture clerk_user_id evidence before db.delete(user)"
    )
    assert saved_pos < delete_pos, (
        "clerk_user_id_hash must be recorded BEFORE db.delete(user)"
    )


def test_post_delete_uses_saved_variables():
    """After db.delete, code must use saved_user_id and avoid user attribute access."""
    from apps.api.app.routers import account
    source = inspect.getsource(account.delete_account)
    delete_pos = source.find("db.delete(user)")
    post_delete = source[delete_pos:]
    assert "saved_user_id" in post_delete, "Post-delete code must use saved_user_id"
    assert "user.clerk_user_id" not in post_delete, (
        "Post-delete code must not access user.clerk_user_id after deletion"
    )
