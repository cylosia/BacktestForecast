#!/usr/bin/env python
"""Detect tables with duplicate BEFORE UPDATE triggers calling set_updated_at().

Run manually or in CI to verify no table has more than one updated_at trigger.
Returns exit code 1 if duplicates are found.
"""
from __future__ import annotations

import sys

from sqlalchemy import text

from backtestforecast.db.session import create_session

_QUERY = text("""
    SELECT event_object_table AS table_name,
           array_agg(trigger_name ORDER BY trigger_name) AS triggers,
           count(*) AS trigger_count
    FROM information_schema.triggers
    WHERE event_manipulation = 'UPDATE'
      AND action_timing = 'BEFORE'
      AND action_statement LIKE '%set_updated_at%'
    GROUP BY event_object_table
    HAVING count(*) > 1
    ORDER BY event_object_table;
""")


def main() -> int:
    with create_session() as session:
        rows = session.execute(_QUERY).fetchall()

    if not rows:
        print("OK: No duplicate updated_at triggers found.")
        return 0

    print("DUPLICATE TRIGGERS DETECTED:")
    for table_name, triggers, count in rows:
        print(f"  {table_name}: {count} triggers - {triggers}")
    print(
        "\nFix: Rebuild from the consolidated baseline migration so each table "
        "has a single trg_{table}_updated_at trigger."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
