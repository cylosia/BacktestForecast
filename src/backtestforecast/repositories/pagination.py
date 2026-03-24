from __future__ import annotations

from datetime import datetime
from typing import Any, TypeVar
from uuid import UUID

from sqlalchemy import Select, and_, desc, or_
from sqlalchemy.orm import Session

T = TypeVar("T")


def apply_cursor_window(
    stmt: Select[Any],
    *,
    model: Any,
    cursor_before: tuple[datetime, UUID] | None,
    limit: int,
    offset: int,
    max_page_size: int,
) -> Select[Any]:
    if offset > 0 and cursor_before is not None:
        raise ValueError("Cannot combine offset and cursor_before pagination.")
    limit = max(limit, 1)
    offset = max(offset, 0)
    if cursor_before is not None:
        cursor_dt, cursor_id = cursor_before
        stmt = stmt.where(
            or_(
                model.created_at < cursor_dt,
                and_(model.created_at == cursor_dt, model.id < cursor_id),
            )
        )
    return stmt.order_by(desc(model.created_at), desc(model.id)).offset(offset).limit(min(limit, max_page_size))


def list_with_total(
    session: Session,
    *,
    base_stmt: Select[Any],
    count_stmt: Select[Any],
    model: Any,
    cursor_before: tuple[datetime, UUID] | None,
    limit: int,
    offset: int,
    max_page_size: int,
) -> tuple[list[T], int]:
    items = list(
        session.scalars(
            apply_cursor_window(
                base_stmt,
                model=model,
                cursor_before=cursor_before,
                limit=limit,
                offset=offset,
                max_page_size=max_page_size,
            )
        )
    )
    total = int(session.scalar(count_stmt) or 0)
    return items, total
