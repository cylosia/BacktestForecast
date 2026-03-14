"""Verify CHECK constraint names don't get doubled by the naming convention."""
from __future__ import annotations

from backtestforecast.db.base import Base


def test_check_constraint_names_not_doubled():
    for table in Base.metadata.tables.values():
        for constraint in table.constraints:
            if hasattr(constraint, 'name') and constraint.name and constraint.name.startswith('ck_'):
                parts = constraint.name.split('_', 3)
                if len(parts) >= 3:
                    table_part = parts[1]
                    rest = '_'.join(parts[2:])
                    assert not rest.startswith(f'ck_{table_part}_'), (
                        f"Constraint {constraint.name} on {table.name} has doubled prefix"
                    )
