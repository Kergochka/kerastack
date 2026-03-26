# ORM Architecture

## Core Philosophy

The ORM is built around two execution contexts:

- `connection` context (`conn`): schema, infrastructure, and low-level SQL workflow.
- `user` context (`user`): row-level business operations (`load/save`) with strict guards.

This split is intentional: structural operations and record operations are separated to reduce accidental misuse.

---

## Minimal Setup Flow

```python
import sqlite3
from kerastack.KergaSQL import KCoreORM, KColumn, KUserMode, kregister
from kerastack.decorators import check_columns_for_update

@check_columns_for_update
@kregister
class DB(KCoreORM):
    __slots__ = ()
    _table_name = "Table_DB"
    id = KColumn("INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL")
    name = KColumn("TEXT")

    def __init__(self, db: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
        super().__init__(db, cursor)

db = sqlite3.connect("db.db")
cur = db.cursor()

conn = DB(db, cur)      # connection context
user = KUserMode(conn)  # user context
```

---

## Mandatory Model Requirements

For a model to work correctly:

- It **must inherit** from `KCoreORM`.
- It **must be decorated** with `@kregister` before creating instances.
- It **must define** an `id` column.
- `id` must be declared as `INTEGER PRIMARY KEY` (optionally with `AUTOINCREMENT`).

Why this is required:

- Registration initializes ORM contracts and class flags.
- `id` is required for stable row identity and `save()/load()` behavior.

---

## Decorators

Project structure note:

- `kerastack/KergaSQL.py` contains decorators directly tied to core DB behavior.
- `kerastack/decorators.py` contains decorators outside the core DB flow (support utilities).

### `@kregister` (`kerastack/KergaSQL.py`)

- Validates subclassing from `KCoreORM`.
- Validates `_table_name` and `_flag_of_cls`.
- Collects `KColumn` descriptors from class hierarchy.
- Enforces mandatory `id` and validates primary-key shape.
- Enables registration flag (`_flag_of_cls` bit 0).
- Wraps `__init__` to enforce `super().__init__(db, cursor)` call.
- Is idempotent (safe against duplicate wrapping).
- Isolates class flags to avoid inherited mutable-bytearray side effects.

### `@knocommands` (`kerastack/KergaSQL.py`)

- Enables class-level command/write lock (`_flag_of_cls` bit 1).
- Blocks write/command operations for that model class.

### `@knoread` (`kerastack/KergaSQL.py`)

- Enables class-level global lock (`_flag_of_cls` bit 2).
- Disables read operations and also blocks write/command operations.

### `@check_columns_for_update` (`kerastack/decorators.py`)

- Utility decorator (non-core DB command flow).
- Enables schema-sync flag (`_flag_of_cls` bit 3).
- Triggers table schema reconciliation in initializer path.

---

## `KColumn`: Descriptor Layer

`KColumn` is a descriptor that defines schema and mediates attribute access.

What it does:

- Validates SQL type definition at class-definition time.
- Binds descriptor name via `__set_name__`.
- In `user` mode: reads/writes values from/to `_data`.
- In `connection` mode: direct row-field access is blocked by design.

Why it matters:

- Model fields look like regular Python attributes, but actually enforce ORM mode, safety, and mapping consistency.

---

## `KUserMode`: User Context Factory

`KUserMode` is a factory-class that builds a user-mode proxy from a base connection instance.

Internal behavior:

- Builds a runtime subclass of the model.
- Reuses the same `db` and `cur`.
- Reuses structural mappings (`_table_name`, `_col_to_idx`) from base connection.
- Enables instance USER flag (`_flag_of_instance` bit 0).
- Allocates isolated `_data` buffer per user instance.

Practical effect:

- Multiple `user` wrappers can coexist with independent in-memory row buffers.
- Row-level workflows are isolated from raw connection operations.

---

## Connection Context: Complete Capability Map

`conn = DB(db, cur)` is the technical context for schema and SQL orchestration.

### Methods and behavior

- `execute(sql_or_list)` (connection-only)
  - Validates each SQL command using `check_sql3_request`.
  - Accepts one SQL string or list of SQL strings.
  - Executes pending queue from `add_requests` + current commands.
  - No automatic `commit()`.

- `add_requests(reqs)` (connection-only)
  - Adds validated SQL into `_list_requests`.
  - Useful for deferred batch execution.
  - No automatic `commit()`.

- `get_requests()`
  - Returns queued SQL list.
  - Read-only helper; no execution; no commit.

- `fetchall(select_sql)` (connection-only)
  - Requires `SELECT`.
  - Returns `list[tuple[Any, ...]]`.

- `fetchone(select_sql="", immediate=None)` (connection-only)
  - Supports two-step and immediate fetch modes.

- `delete(**filters)` (connection-only)
  - Exact-match delete; all conditions are joined with `AND`.
  - Commits on success.

- `delete_ranges(...)` (connection-only)
  - Range-based delete via `BETWEEN`.
  - Supports logical markers `KOR` / `KAND`.
  - Also supports exact-value shorthand: `column=value` becomes `column = ?`.
  - `KOR` means `OR`; `KAND` means `AND`.
  - Logical marker belongs to the current left condition and joins it with the next one.
  - Default join is `AND` when no marker is provided.
  - Last condition cannot have `KOR` / `KAND`.
  - Example:
    - `conn.delete_ranges(id=(1, 10, KOR), name="a")`
    - SQL shape: `... WHERE id BETWEEN 1 AND 10 OR name = 'a'`.
    `conn.delete_ranges(id=(1, 10, KOR), spec=(10, 20))`
    - SQL-shape: `... WHERE id BETWEEN 1 AND 10 OR spec BETWEEN 10 AND 20`
  - Commits on success.

- `drop(force)` (connection-only)
  - `None` / `"n"`: cancel.
  - `"y"`: execute table drop.
  - Commits on success; rollbacks on failure.

- `drop_columns(*columns)` (connection-only)
  - Drops selected columns from schema.
  - Forbids dropping mandatory `id`.
  - Commits on success; rollbacks on failure.
  - Removes corresponding `KColumn` descriptors from the model class (`delattr`).
  - Rebuilds class/instance mapping metadata after schema change.

- `check_sql3_request(sql, allowed_commands=None)`
  - SQL validator helper.
  - Returns `(ok: bool, reason: str)`.
  - Does not mutate DB state.

- `_quote_identifier(name)` (class helper)
  - Validates and safely quotes table/column identifiers.
  - Prevents unsafe identifier injection.

### Initializer responsibilities (`KCoreORM.__init__`)

- Stores `db`, `cur`.
- Initializes request queue and instance flags.
- Builds/loads table metadata and row buffer shape.
- Creates table when missing.
- If schema-sync flag is enabled, runs reconciliation path.

---

## Connection Restrictions (What Is Forbidden)

Main rule: connection context is not a row-level data API.

Forbidden in connection mode:

- Direct model field access through descriptors:
  - `conn.name`, `conn.name = "..."`.
- User-only APIs:
  - `load`, `save`.

Class locks that also apply:

- `@knocommands` blocks command/write paths.
- `@knoread` blocks read paths and also blocks write/command paths.

Operational rule:

- Use `conn` for schema/SQL orchestration.
- Use `user = KUserMode(conn)` for record-level operations.

---

## User Context: Complete Capability Map

`user = KUserMode(conn)` is the row-level context.

### Methods and behavior

- `load(row_id)`
  - Reads one row by `id`.
  - On success: loads row into `_data` and returns `True`.
  - On miss: returns `False`.

- `save()`
  - If `id is None`: performs `INSERT`, then writes back `lastrowid`.
  - If `id` exists: performs `UPDATE ... WHERE id = ?`.
  - Commits on success.
  - Requires mandatory `id` mapping.

### User restrictions

- User context cannot call connection-only operations:
  - `execute`, `add_requests`, `fetchall`, `fetchone`, `delete`, `delete_ranges`, `drop`, `drop_columns`.
- Class locks still apply:
  - `knocommands` blocks writes/commands.
  - `knoread` blocks reads and also blocks writes/commands.

---

## Commit / Rollback Behavior Matrix

### Connection side

- `execute` -> no auto-commit.
- `add_requests` -> no auto-commit.
- `get_requests` -> no commit.
- `check_sql3_request` -> no commit.
- `delete` -> commit on success; rollback on execution error.
- `delete_ranges` -> commit on success; rollback on execution error.
- `drop` -> commit on success, rollback on error.
- `drop_columns` -> commit on success, rollback on error.

### User side

- `save` -> commit on success.
- `save` -> rollback on error and raises wrapped exception.
- `load` -> read-only (no commit).

Recommended batch workflow:

1. `conn.add_requests(...)`
2. `conn.execute("")`
3. `conn.db.commit()`

---

## Error Contract for Application Developers

Why this matters:

- You can write predictable `try/except` handling.
- You can quickly distinguish access errors from data/SQL errors.
- You can keep tests and API error-mapping stable.

Primary exception types:

- `PermissionError`
  - Wrong context (`connection` vs `user`) or class locks.
- `ConnPermissionError`
  - Attempt to access `KColumn` descriptor data outside USER mode (`conn.field` or assignment).
- `TypeError`
  - Invalid argument types or invalid SQL-related input shape.
- `AttributeError`
  - Unknown/missing columns, invalid mapping, missing update target row.
- `sqlite3.OperationalError`
  - SQL rejected by validator or failed during SQLite execution.

Suggested handling order:

1. `PermissionError` (mode/access contract)
2. `TypeError` / `AttributeError` (input/schema contract)
3. `sqlite3.OperationalError` (SQL/runtime DB layer)
4. Generic `Exception` fallback

`get_error(...)` provides normalized ORM messages and supports message IDs for consistency.

---

## Internal Flags Reference

### Class flags (`_flag_of_cls[0]`)

- bit 0: class is registered.
- bit 1: global write/command lock.
- bit 2: global read + write + command lock.
- bit 3: force schema sync from current `KColumn` definitions.

### Instance flags (`_flag_of_instance[0]`)

- bit 0: instance is USER mode.
- bit 1: base initializer marker (`super().__init__` executed).

