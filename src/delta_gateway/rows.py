"""Compare-and-swap updates for rows that more than one process writes.

D1 has no transactions across requests, so a row carries a ``version`` column. An update
names the version it read and only lands if nobody changed the row since; otherwise the
row is read again and the change re-applied.
"""

ATTEMPTS = 10


class ConflictError(RuntimeError):
    """The row kept changing while an update was being applied."""


def where(key):
    return " AND ".join(f"{column} = ?" for column in key), list(key.values())


async def update_versioned(db, table, key, change, *, attempts=ATTEMPTS):
    """Apply ``change(row)`` to the row of *table* matching *key*; return the updated row."""
    condition, values = where(key)
    for _ in range(attempts):
        rows = await db.query(f"SELECT * FROM {table} WHERE {condition}", values)
        if not rows:
            raise KeyError(f"No {table} row {key}")
        current = rows[0]
        row = dict(current)
        change(row)
        changed = {
            column: value
            for column, value in row.items()
            if column != "version" and column not in key and value != current.get(column)
        }
        assignments = ["version = version + 1"]
        params = []
        for column, value in changed.items():
            if value is None:
                assignments.append(f"{column} = NULL")
            else:
                assignments.append(f"{column} = ?")
                params.append(value)
        landed = await db.run(
            f"UPDATE {table} SET {', '.join(assignments)} WHERE {condition} AND version = ?",
            [*params, *values, current["version"]],
        )
        if landed == 1:
            return {**row, "version": current["version"] + 1}
    raise ConflictError(f"{table} {key} kept changing; try again.")
