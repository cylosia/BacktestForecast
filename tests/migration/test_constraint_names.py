"""Verify CHECK constraint names don't get doubled by the naming convention."""
from __future__ import annotations

from backtestforecast.db.base import Base


def test_check_constraint_names_not_doubled():
    for table in Base.metadata.tables.values():
        for constraint in table.constraints:
            if hasattr(constraint, 'name') and constraint.name and constraint.name.startswith('ck_'):
                prefix = f"ck_{table.name}_"
                if constraint.name.startswith(prefix):
                    rest = constraint.name[len(prefix):]
                    assert not rest.startswith(prefix), (
                        f"Constraint {constraint.name} on {table.name} has doubled prefix"
                    )
