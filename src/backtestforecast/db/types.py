from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.engine import Dialect
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ClauseElement
from sqlalchemy.types import CHAR, TypeDecorator, TypeEngine


class _JsonDefault(ClauseElement):
    """JSON default that compiles to '{}'::jsonb for PostgreSQL, '{}' for SQLite.
    Matches migration format to prevent alembic autogenerate spurious diffs."""
    __visit_name__ = "json_default"

    def __init__(self, value: str) -> None:
        if value not in ("{}", "[]"):
            raise ValueError(f"_JsonDefault only supports '{{}}' and '[]', got: {value!r}")
        self.value = value


@compiles(_JsonDefault, "postgresql")
def _compile_json_default_pg(element: _JsonDefault, *_args: Any, **_kwargs: Any) -> str:
    return f"'{element.value}'::jsonb"


@compiles(_JsonDefault, "sqlite")
def _compile_json_default_sqlite(element: _JsonDefault, *_args: Any, **_kwargs: Any) -> str:
    return f"'{element.value}'"


JSON_DEFAULT_EMPTY_OBJECT = _JsonDefault("{}")
JSON_DEFAULT_EMPTY_ARRAY = _JsonDefault("[]")


class GUID(TypeDecorator[uuid.UUID]):
    """Portable UUID type with native PostgreSQL UUID support and CHAR(36) fallback."""

    impl = CHAR(36)
    cache_ok = True

    def load_dialect_impl(self, dialect: Dialect) -> TypeEngine:  # type: ignore[override]
        if dialect.name == "postgresql":
            return dialect.type_descriptor(PG_UUID(as_uuid=True))
        return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value: uuid.UUID | str | None, dialect: Dialect) -> uuid.UUID | str | None:  # type: ignore[override]
        if value is None:
            return None
        if isinstance(value, uuid.UUID):
            return value if dialect.name == "postgresql" else str(value)
        coerced = uuid.UUID(str(value))
        return coerced if dialect.name == "postgresql" else str(coerced)

    def process_result_value(self, value: Any, dialect: Dialect) -> uuid.UUID | None:  # type: ignore[override]
        if value is None or isinstance(value, uuid.UUID):
            return value
        return uuid.UUID(str(value))


JSON_VARIANT = JSON().with_variant(JSONB, "postgresql")

__all__ = [
    "GUID",
    "JSON_DEFAULT_EMPTY_ARRAY",
    "JSON_DEFAULT_EMPTY_OBJECT",
    "JSON_VARIANT",
]
