from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, TypeVar
from uuid import UUID

from backtestforecast.errors import ValidationError
from backtestforecast.utils import decode_cursor, encode_cursor


class CursorPageItem(Protocol):
    id: UUID
    created_at: datetime


T = TypeVar("T", bound=CursorPageItem)


@dataclass(frozen=True)
class CursorPage[T: CursorPageItem]:
    items: list[T]
    total: int
    offset: int
    limit: int
    next_cursor: str | None


def parse_cursor_param(cursor: str | None) -> tuple[tuple[datetime, UUID] | None, int]:
    if not cursor:
        return None, 0
    decoded = decode_cursor(cursor)
    if decoded is None:
        raise ValidationError("Invalid pagination cursor.")
    return decoded, 0


def finalize_cursor_page[T: CursorPageItem](
    items: list[T],
    *,
    total: int,
    offset: int,
    limit: int,
) -> CursorPage[T]:
    has_next = len(items) > limit
    page_items = items[:limit] if has_next else items
    next_cursor = encode_cursor(page_items[-1].created_at, page_items[-1].id) if has_next and page_items else None
    return CursorPage(
        items=page_items,
        total=total,
        offset=offset,
        limit=limit,
        next_cursor=next_cursor,
    )
