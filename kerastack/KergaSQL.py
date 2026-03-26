import sqlite3
import re
from kerastack.decorators import decorator
from typing import Any, Iterable, Literal, Optional, Self, Type, TypeVar, Union, cast
from abc import abstractmethod
T = TypeVar("T", bound="KCoreORM")


class UserPermissionError(Exception):
    """Raised when a user-mode instance attempts a forbidden action"""
    __slots__ = ()



class ConnPermissionError(Exception):
    """Raised when an operation is not permitted for the current connection mode"""
    __slots__ = ()

class __kLogicNode:
    """
    Base for singleton logical marker instances (used as ORM/query logic tokens).

    Each concrete subclass exposes at most one instance via ``__new__`` (singleton).
    Subclasses must define ``_instance = None`` on the class.
    """

    __slots__ = ()

    @abstractmethod
    def __new__(cls, *args: Any, **kwds: Any) -> Self:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

class __kANDcls(__kLogicNode):
    """Singleton instance representing logical AND (conjunction) in query/flag composition."""

    __slots__ = ()
    _instance = None


class __kORcls(__kLogicNode):
    """Singleton instance representing logical OR (disjunction) in query/flag composition."""

    __slots__ = ()
    _instance = None


# Module-level singletons: use these as logical values (not per-call constructors).
KAND = __kANDcls()
KOR = __kORcls()


class KColumn:
    """
    KColumn instances act as descriptors for table columns.
    They are intended to be accessed primarily in the "user" context.
    allowing interaction with data associated with a specific user or record.
    """

    __slots__ = ("sql_types", "_name")

    SQLITE_TYPES = (
        "NULL",
        "INTEGER",
        "REAL",
        "TEXT",
        "BLOB",
        "INT",
        "NUMERIC",
        "FLOAT",
        "DOUBLE",
        "BOOLEAN",
        "DATE",
        "DATETIME",
    )
    def __init__(self, sql_command: str) -> None:
        if not isinstance(sql_command, str):
            raise KCoreORM.get_error(TypeError, id=11)

        if not sql_command.strip():
            raise KCoreORM.get_error(ValueError, id=12)

        if sql_command.lstrip().split()[0].upper() not in KColumn.SQLITE_TYPES:
            raise KCoreORM.get_error(TypeError, id=13)

        with sqlite3.connect(":memory:") as temp_db:
            cur = temp_db.cursor()

            try:
                cur.execute(f"CREATE TABLE test (col {sql_command})")
                self.sql_types = sql_command
            except sqlite3.OperationalError:
                raise KCoreORM.get_error(TypeError, id=18) from None

    def __set_name__(self, owner: Type["KCoreORM"], name: str) -> None:
        self._name = name

    def __get__(self, instance: Optional["KCoreORM"], owner: Type["KCoreORM"]) -> Any:

        if instance is None:
            return self

        if not instance._flag_of_instance[0] & (1 << 0):
            raise type(instance).get_error(ConnPermissionError, id=3)
        if type(instance)._flag_of_cls[0] & (1 << 2):
            raise type(instance).get_error(PermissionError, id=21)

        if not isinstance(instance._col_to_idx, dict):
            raise type(instance).get_error(AttributeError, id=4, add=self._name)
        if not isinstance(instance._data, list):
            raise type(instance).get_error(AttributeError, id=4, add=self._name)

        idx = instance._col_to_idx.get(self._name)
        if not isinstance(idx, int):
            raise type(instance).get_error(AttributeError, id=4, add=self._name)

        return instance._data[idx]

    def __set__(self, instance: Optional["KCoreORM"], value: Any) -> None:
        if instance is None:
            return

        if not (instance._flag_of_instance[0] & (1 << 0)):
            raise type(instance).get_error(ConnPermissionError, id=3)
        if type(instance)._flag_of_cls[0] & (1 << 2):
            raise type(instance).get_error(PermissionError, id=21)

        if not isinstance(instance._col_to_idx, dict):
            raise type(instance).get_error(AttributeError, id=4, add=self._name)
        if not isinstance(instance._data, list):
            raise type(instance).get_error(AttributeError, id=4, add=self._name)

        idx = instance._col_to_idx.get(self._name)
        if not isinstance(idx, int):
            raise type(instance).get_error(AttributeError, id=4, add=self._name)

        instance._data[idx] = value


class __KSql3Execution:
    __slots__ = ()

    cur: sqlite3.Cursor
    _table_name: str

    def execute(self, sql3_data: str | list[str]) -> None:
        """
        Execute one SQL statement or a list of statements with the current cursor.

        Args:
            sql3_data: Single SQL string or list of SQL strings.
        """
        if isinstance(sql3_data, list):
            for i in sql3_data:
                self.cur.execute(i)
            return
        self.cur.execute(sql3_data)

    def fetchall(self, sql3_select: str) -> list[tuple[Any, ...]]:
        """
        Fetch all rows from the database.
        """
        self.cur.execute(sql3_select)
        return self.cur.fetchall()

    def fetchone(
        self, immediate: None | Any, sql3_select: str = ""
    ) -> Optional[tuple[Any, ...]] | Literal["SELECTED"]:
        """
        Fetch one row or stage a SELECT query for the next fetch.

        Behavior:
        - If `immediate` is not None, returns `cursor.fetchone()` directly.
        - Else if `sql3_select` is empty, returns `cursor.fetchone()` directly.
        - Else executes `sql3_select` and returns the sentinel `"SELECTED"`.

        This allows two modes:
        1) two-step mode: call with SQL, then call again without SQL;
        2) immediate mode: pass `immediate` to force direct fetch.
        """
        if not sql3_select or immediate is not None:
            return self.cur.fetchone()
        
        self.cur.execute(sql3_select)
        return "SELECTED"


class KCoreORM(__KSql3Execution):
    IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
    SQLITE_RESERVED_WORDS = (
        "ABORT",
        "ACTION",
        "ADD",
        "AFTER",
        "ALL",
        "ALTER",
        "ALWAYS",
        "ANALYZE",
        "AND",
        "AS",
        "ASC",
        "ATTACH",
        "AUTOINCREMENT",
        "BEFORE",
        "BEGIN",
        "BETWEEN",
        "BY",
        "CASCADE",
        "CASE",
        "CAST",
        "CHECK",
        "COLLATE",
        "COLUMN",
        "COMMIT",
        "CONFLICT",
        "CONSTRAINT",
        "CREATE",
        "CROSS",
        "CURRENT",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "DATABASE",
        "DEFAULT",
        "DEFERRABLE",
        "DEFERRED",
        "DELETE",
        "DESC",
        "DETACH",
        "DISTINCT",
        "DO",
        "DROP",
        "EACH",
        "ELSE",
        "END",
        "ESCAPE",
        "EXCEPT",
        "EXCLUDE",
        "EXCLUSIVE",
        "EXISTS",
        "EXPLAIN",
        "FAIL",
        "FILTER",
        "FIRST",
        "FOLLOWING",
        "FOR",
        "FOREIGN",
        "FROM",
        "FULL",
        "GENERATED",
        "GLOB",
        "GROUP",
        "HAVING",
        "IF",
        "IGNORE",
        "IMMEDIATE",
        "IN",
        "INDEX",
        "INDEXED",
        "INITIALLY",
        "INNER",
        "INSERT",
        "INSTEAD",
        "INTERSECT",
        "INTO",
        "IS",
        "ISNULL",
        "JOIN",
        "KEY",
        "LAST",
        "LEFT",
        "LIKE",
        "LIMIT",
        "MATCH",
        "MATERIALIZED",
        "NATURAL",
        "NO",
        "NOT",
        "NOTHING",
        "NOTNULL",
        "NULL",
        "NULLS",
        "OF",
        "OFFSET",
        "ON",
        "OR",
        "ORDER",
        "OTHERS",
        "OUTER",
        "OVER",
        "PARTITION",
        "PLAN",
        "PRAGMA",
        "PRECEDING",
        "PRIMARY",
        "QUERY",
        "RAISE",
        "RANGE",
        "RECURSIVE",
        "REFERENCES",
        "REGEXP",
        "REINDEX",
        "RELEASE",
        "RENAME",
        "REPLACE",
        "RESTRICT",
        "RETURNING",
        "RIGHT",
        "ROLLBACK",
        "ROW",
        "ROWS",
        "SAVEPOINT",
        "SELECT",
        "SET",
        "TABLE",
        "TEMP",
        "TEMPORARY",
        "THEN",
        "TIES",
        "TO",
        "TRANSACTION",
        "TRIGGER",
        "UNBOUNDED",
        "UNION",
        "UNIQUE",
        "UPDATE",
        "USING",
        "VACUUM",
        "VALUES",
        "VIEW",
        "VIRTUAL",
        "WHEN",
        "WHERE",
        "WINDOW",
        "WITH",
        "WITHOUT",
    )

    __slots__ = (
        "db",
        "cur",
        "_list_requests",
        "_col_to_idx",
        "_flag_of_instance",
        "_data",
    )
    _table_name = "Default_name"

    # Class-level flags stored in `_flag_of_cls` (bytearray):
    # - bit 0: class is registered
    # - bit 1: global write/command lock (execute, save, delete, delete_ranges, drop, drop_columns)
    # - bit 2: global read/write/command lock
    # - bit 3: always synchronize table schema from current KColumn sql_types
    _flag_of_cls = bytearray(4)
    # Instance-level flags stored in `_flag_of_instance` (bytearray):
    # - bit 0: user mode
    # - bit 1: base initializer marker (super().__init__ called)
    # Cache ORM metadata per concrete subclass:
    # - column mapping attr_name -> db column index (cid)
    # - column SQL fragments for CREATE TABLE
    # This avoids re-running PRAGMA/mapping work on every instance.
    _orm_meta_cache: dict[type["KCoreORM"], dict[str, Any]] = {}

    def __init__(self, db: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
        self.db = db
        self.cur = cursor
        self._list_requests: list[str] = []

        if not hasattr(self, "_flag_of_instance"):
            self._flag_of_instance = bytearray(1)
        # Mark that the base ORM initializer ran.
        self._flag_of_instance[0] |= 1 << 1

        cls = type(self)
        if not (cls._flag_of_cls[0] & (1 << 0)):
            raise self.get_error(RuntimeError, id=7)
        force_schema_refresh = bool(cls._flag_of_cls[0] & (1 << 3))
        meta = None if force_schema_refresh else self._orm_meta_cache.get(cls)

        # Global read-lock mode: do not expose row-mapping buffers.
        if cls._flag_of_cls[0] & (1 << 2):
            self._col_to_idx = {}
            self._data = []
            return



        if meta is None:
            # Collect all KColumn instances from the class and its base classes.
            columns_in_class: dict[str, KColumn] = {}
            for base in cls.__mro__:
                for name, obj in base.__dict__.items():
                    if isinstance(obj, KColumn) and name not in columns_in_class:
                        columns_in_class[name] = obj

            cols_sql = [f"{obj._name} {obj.sql_types}" for obj in columns_in_class.values()]
            safe_table = self._quote_identifier(self._table_name)
            if force_schema_refresh:
                self._sync_table_schema_for_update(columns_in_class, cols_sql)
            else:
                self.cur.execute(f"CREATE TABLE IF NOT EXISTS {safe_table} ({', '.join(cols_sql)})")

            self.cur.execute(f"PRAGMA table_info({safe_table})")
            rows = self.cur.fetchall()
            # db_map: column name -> index in db (cid)
            db_map = {row[1]: row[0] for row in rows}
            # Map descriptor attribute names -> SQLite column indexes (cid)
    
            col_to_idx: dict[str, int] = {}
            for attr_name, obj in columns_in_class.items():
                idx = db_map.get(obj._name)
                if idx is not None:
                    col_to_idx[attr_name] = idx
                else:
                    raise self.get_error(AttributeError, id=4, add=obj._name)

            data_size = max(db_map.values()) + 1 if db_map else 0
            meta = {
                "cols_sql": cols_sql,
                "col_to_idx": col_to_idx,
                "data_size": data_size,
            }
            self._orm_meta_cache[cls] = meta
        else:
            # Ensure the table exists in the current connection.
            cols_sql = meta["cols_sql"]
            safe_table = self._quote_identifier(self._table_name)
            self.cur.execute(f"CREATE TABLE IF NOT EXISTS {safe_table} ({', '.join(cols_sql)})")

        self._col_to_idx = meta["col_to_idx"]
        self._data = [None] * meta["data_size"]

    def check_sql3_request(
        self, sql_query: str, *, allowed_commands: Optional[set[str]] = None
    ) -> tuple[bool, str]:
        """
        Checks if a SQL request is valid and returns a tuple containing a boolean indicating validity and a string with the reason for the validity.
        Args:
            sql_query: The SQL request to check.
            allowed_commands: A set of commands that are allowed in this context.
        Returns:
            A tuple containing a boolean indicating validity and a string with the reason for the validity.
        """

        if not isinstance(sql_query, str):
            return False, "SQL request must be a string."

        q = sql_query.strip().rstrip(";")
        if not q:
            return False, "SQL request cannot be empty."

        if ";" in q:
            return False, "Multiple SQL statements are not allowed."

        if q.count("(") != q.count(")"):
            return False, "Unbalanced parentheses in SQL request."

        cmd = q.split(maxsplit=1)[0].upper()
        if allowed_commands and cmd not in allowed_commands:
            return False, f"Command '{cmd}' is not allowed in this context."

        try:
            self.cur.execute(f"EXPLAIN {q}")
            return True, ""
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            syntax_signatures = (
                "syntax error",
                "incomplete input",
                "unrecognized token",
                "near ",
            )
            # Syntax-level problems are invalid requests.
            # Semantic/runtime errors (e.g. no such table) are still valid SQL.
            if any(signature in msg for signature in syntax_signatures):
                return False, str(e)
            return True, ""
        except Exception as e:
            return False, str(e)

    @classmethod
    def _quote_identifier(cls, name: str) -> str:
        """
        Validate and quote SQLite identifiers (table/column names).
        """
        if not isinstance(name, str) or not cls.IDENTIFIER_RE.fullmatch(name):
            raise cls.get_error(ValueError, id=22, add=str(name))
        return f'"{name}"'

    def _sync_table_schema_for_update(
        self, columns_in_class: dict[str, KColumn], cols_sql: list[str]
    ) -> None:
        """
        Rebuild table schema from current KColumn definitions and preserve common data.
        """
        safe_table = self._quote_identifier(self._table_name)

        self.cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            (self._table_name,),
        )
        exists = self.cur.fetchone() is not None

        if not exists:
            self.cur.execute(f"CREATE TABLE IF NOT EXISTS {safe_table} ({', '.join(cols_sql)})")
            return

        tmp_name = f"__tmp_sync_{self._table_name}"
        safe_tmp = self._quote_identifier(tmp_name)

        try:
            self.cur.execute(f"DROP TABLE IF EXISTS {safe_tmp}")
            self.cur.execute(f"CREATE TABLE {safe_tmp} ({', '.join(cols_sql)})")

            self.cur.execute(f"PRAGMA table_info({safe_table})")
            old_rows = self.cur.fetchall()
            old_names = {row[1] for row in old_rows}
            new_names = [obj._name for obj in columns_in_class.values()]
            common = [name for name in new_names if name in old_names]

            if common:
                cols = ", ".join(self._quote_identifier(name) for name in common)
                self.cur.execute(
                    f"INSERT INTO {safe_tmp} ({cols}) SELECT {cols} FROM {safe_table}"
                )

            self.cur.execute(f"DROP TABLE {safe_table}")
            self.cur.execute(f"ALTER TABLE {safe_tmp} RENAME TO {safe_table}")
        except Exception as e:
            self.db.rollback()
            raise self.get_error(type(e), message=str(e)) from e

    def execute(self, sql3_data: Union[str, list[str]] = "") -> None:
        """
        Validate and execute one SQL request or a queued batch.

        The method enforces permission flags, validates each SQL statement,
        normalizes delimiters, then executes the final queue.
        """
        # Access locks:
        # - operation requires a connection instance (not USER mode)
        # - class write/read locks also block execute
        if self._flag_of_instance[0] & (1 << 0):
            raise self.get_error(PermissionError, id=0)
        if type(self)._flag_of_cls[0] & (1 << 1) or type(self)._flag_of_cls[0] & (1 << 2):
            raise self.get_error(PermissionError, id=1)

        # Normalize input to a list of candidate queries.
        if isinstance(sql3_data, str):
            raw_queries = [sql3_data] if sql3_data.strip() else []
        else:
            raw_queries = sql3_data

        # Validate and normalize each query.
        clean_current = []
        for q in raw_queries:
            if not isinstance(q, str):
                raise self.get_error(TypeError, id=11)
            q_stripped = q.strip()
            if not q_stripped:
                continue

            query = q_stripped.rstrip(";") + ";"

            ok, reason = self.check_sql3_request(query)
            if not ok:
                details = f"{q} | {reason}" if reason else q
                raise self.get_error(sqlite3.OperationalError, id=20, add=details) from None

            clean_current.append(query)

        # Merge deferred requests and execute as one atomic batch.
        final_queue = self._list_requests + clean_current
        if not final_queue:
            return

        queued_before = list(self._list_requests)
        savepoint = "__korm_execute_batch"
        try:
            self.cur.execute(f"SAVEPOINT {savepoint}")
            super().execute(final_queue)
            self.cur.execute(f"RELEASE SAVEPOINT {savepoint}")
            # Clear deferred queue only after successful execution.
            self._list_requests.clear()
        except Exception as e:
            try:
                self.cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                self.cur.execute(f"RELEASE SAVEPOINT {savepoint}")
            except Exception:
                # Fallback to connection-wide rollback if savepoint handling failed.
                self.db.rollback()
            # Restore deferred queue; direct one-shot input is intentionally not queued.
            self._list_requests = queued_before
            raise self.get_error(type(e), message=str(e)) from e


    def add_requests(self, reqs: str | list[str] | Iterable[str] | tuple[str, ...] | set[str]) -> None:
        """
        Add one or more SQL requests to the deferred queue (connection-only).

        Args:
            reqs: Single SQL string or list of SQL strings.
        """
        if self._flag_of_instance[0] & (1 << 0):
            raise self.get_error(PermissionError, id=0)
        if type(self)._flag_of_cls[0] & (1 << 1) or type(self)._flag_of_cls[0] & (1 << 2):
            raise self.get_error(PermissionError, id=1)

        if isinstance(reqs, str):
            reqs = [reqs]

        for r in reqs:
            if not isinstance(r, str):
                raise self.get_error(TypeError, id=11)
            if r.strip():
                clean_r = r.strip().rstrip(";") + ";"
                ok, reason = self.check_sql3_request(clean_r)
                if ok:
                    self._list_requests.append(clean_r)
                else:
                    details = f"{r} | {reason}" if reason else r
                    raise self.get_error(
                        sqlite3.OperationalError, id=20, add=details
                    ) from None

    def get_requests(self) -> list[str]:
        """
        Get the list of requests that are queued for execution.
        """
        return self._list_requests

    def fetchall(self, sql3_select: str) -> list[tuple[Any, ...]]:
        """
        Fetch all rows from the database.
        """

        if self._flag_of_instance[0] & (1 << 0):
            raise self.get_error(PermissionError, id=0)

        if type(self)._flag_of_cls[0] & (1 << 2):
            raise self.get_error(PermissionError, id=21)

        if not isinstance(sql3_select, str):
            raise self.get_error(TypeError, id=14)

        sql3_select = sql3_select.strip().rstrip(";") + ";"


        if not sql3_select.upper().startswith("SELECT"):
            raise self.get_error(TypeError, id=15)

        return super().fetchall(sql3_select)

    def fetchone(
        self, sql3_select: str = "", immediate: None | Any = None
    ) -> Optional[tuple[Any, ...]] | Literal["SELECTED"]:
        """
        Fetch one row with ORM permission/type checks.

        Behavior mirrors base `fetchone`:
        - `immediate is not None` -> return `cursor.fetchone()` directly;
        - empty `sql3_select` -> return `cursor.fetchone()` directly;
        - non-empty `sql3_select` -> execute query and return `"SELECTED"`.
        """

        if self._flag_of_instance[0] & (1 << 0):
            raise self.get_error(PermissionError, id=0)

        if type(self)._flag_of_cls[0] & (1 << 2):
            raise self.get_error(PermissionError, id=21)

        if not isinstance(sql3_select, str):
            raise self.get_error(TypeError, id=14)

        sql3_select = sql3_select.strip().rstrip(";") + ";" if sql3_select else ""

        if sql3_select and not sql3_select.upper().startswith("SELECT"):
            raise self.get_error(TypeError, id=15)

        return super().fetchone(immediate, sql3_select)

    def delete_ranges(
        self,
        **ranges: tuple[Any, Any]
        | tuple[tuple[Any, Any], Any]
        | tuple[Any, Any, Any],
    ) -> str:
        """
        Delete rows where each specified column value is within a given range.

        Each keyword argument supports one of these forms:
            column_name=exact_value
            column_name=(exact_value,)
            column_name=(start_value, end_value)
            column_name=((start_value, end_value), KOR|KAND|None)
            column_name=(start_value, end_value, KOR|KAND|None)

        Example:
            delete_ranges(id=(1, 100), price=(10, 50))

        Notes:
        - Column names are validated against known ORM columns.
        - Identifiers are SQL-quoted to prevent identifier injection.
        - If logic marker is omitted, AND is used.
        - Exact-value shorthand is translated into `column = ?`.
        - `KOR` joins current condition with the next one using OR.
        - `KAND` joins current condition with the next one using AND.

        Returns:
            Human-readable message with deleted row count.

        Raises:
            PermissionError: if operation is blocked by mode/class flags.
            ValueError: if no ranges are provided.
            AttributeError: if a column is unknown/unauthorized.
            TypeError: if range format/logic marker is invalid.
        """
        if self._flag_of_instance[0] & (1 << 0):
            raise self.get_error(PermissionError, id=0)
        if type(self)._flag_of_cls[0] & (1 << 1) or type(self)._flag_of_cls[0] & (1 << 2):
            raise self.get_error(PermissionError, id=1)

        if not ranges:
            raise self.get_error(ValueError, id=16)


        conditions: list[str] = []
        params: list[Any] = []
        join_ops: list[str] = []

        total_ranges = len(ranges)
        for idx, (column, raw_value) in enumerate(ranges.items()):
            if not isinstance(self._col_to_idx, dict) or column not in self._col_to_idx:
                raise self.get_error(AttributeError, id=23, add=str(column))

            logic: Any = None
            use_exact_match = False
            exact_value: Any = None
            bounds: tuple[Any, Any]

            if not isinstance(raw_value, tuple):
                use_exact_match = True
                exact_value = raw_value
            elif isinstance(raw_value, tuple) and len(raw_value) == 2:
                # (start, end) OR ((start, end), logic)
                if isinstance(raw_value[0], tuple):
                    bounds = cast(tuple[Any, Any], raw_value[0])
                    logic = raw_value[1]
                else:
                    bounds = cast(tuple[Any, Any], raw_value)
            elif isinstance(raw_value, tuple) and len(raw_value) == 3:
                # (start, end, logic)
                bounds = cast(tuple[Any, Any], (raw_value[0], raw_value[1]))
                logic = raw_value[2]
            else:
                raise self.get_error(TypeError, message=f"Invalid range format for '{column}'")

            if not use_exact_match and (not isinstance(bounds, tuple) or len(bounds) != 2):
                raise self.get_error(TypeError, message=f"Range for '{column}' must contain exactly 2 bounds")

            if logic is None or logic is KAND:
                join_op = "AND"
            elif logic is KOR:
                join_op = "OR"
            else:
                raise self.get_error(TypeError, message=f"Unsupported logic marker for '{column}'")

            # Logic marker belongs to the current (left) condition, so it cannot be
            # attached to the last condition (there is no right condition to join).
            if idx == total_ranges - 1 and logic is not None:
                raise self.get_error(
                    TypeError,
                    message=f"Logic marker on last range '{column}' is not allowed",
                )

            safe_column = self._quote_identifier(column)
            if use_exact_match:
                conditions.append(f"{safe_column} = ?")
                params.append(exact_value)
            else:
                conditions.append(f"{safe_column} BETWEEN ? AND ?")
                params.extend((bounds[0], bounds[1]))
            join_ops.append(join_op)

        condition_str = conditions[0]
        for i, cond in enumerate(conditions[1:], start=1):
            # Operator is taken from the previous (left) condition.
            condition_str = f"{condition_str} {join_ops[i - 1]} {cond}"

        safe_table = self._quote_identifier(self._table_name)
        sql = f"DELETE FROM {safe_table} WHERE {condition_str};"

        try:
            self.cur.execute(sql, tuple(params))
            self.db.commit()
            return f"DELETED {self.cur.rowcount} rows from {self._table_name}"
        except Exception as e:
            self.db.rollback()
            raise self.get_error(type(e), message=str(e)) from e

    def delete(self, **filters: Any) -> str:
        """
        Delete rows by exact-match filters.

        Each keyword argument is treated as:
            column_name = value

        Example:
            delete(id=1, name="admin")

        Notes:
        - At least one filter is required.
        - Unknown columns are rejected.
        - Column identifiers are SQL-quoted for safety.
        - All filter conditions are joined with AND.

        Returns:
            Human-readable message with deleted row count.

        Raises:
            PermissionError: if operation is blocked by mode/class flags.
            ValueError: if no filters are provided.
            AttributeError: if a column is unknown/unauthorized.
        """
        if self._flag_of_instance[0] & (1 << 0):
            raise self.get_error(PermissionError, id=0)
        if type(self)._flag_of_cls[0] & (1 << 1) or type(self)._flag_of_cls[0] & (1 << 2):
            raise self.get_error(PermissionError, id=1)

        if not filters:
            raise self.get_error(ValueError, id=17)

        parts: list[str] = []
        for key in filters:
            if not isinstance(self._col_to_idx, dict) or key not in self._col_to_idx:
                raise self.get_error(AttributeError, id=23, add=str(key))
            parts.append(f"{self._quote_identifier(key)} = ?")
        condition_str = " AND ".join(parts)
        params = tuple(filters.values())
        safe_table = self._quote_identifier(self._table_name)
        sql = f"DELETE FROM {safe_table} WHERE {condition_str};"

        try:
            self.cur.execute(sql, params)
            self.db.commit()
            return f"DELETED {self.cur.rowcount} rows from {self._table_name}"
        except Exception as e:
            self.db.rollback()
            raise self.get_error(type(e), message=str(e)) from e

    def drop_columns(self, *columns: str) -> str:
        """
        Drop one or more columns from the table schema.

        Restrictions:
        - Available only for connection instances (not USER mode wrappers).
        - Mandatory primary key column `id` cannot be dropped.

        Args:
            *columns: ORM attribute names/column names to drop.

        Returns:
            Human-readable message with dropped column names.

        Raises:
            PermissionError: if blocked by mode/class lock flags.
            ValueError: if no columns are provided.
            TypeError: if any column name is not a string.
            AttributeError: if a column is unknown/unauthorized.
        """
        if self._flag_of_instance[0] & (1 << 0):
            raise self.get_error(PermissionError, id=0)
        if type(self)._flag_of_cls[0] & (1 << 1) or type(self)._flag_of_cls[0] & (1 << 2):
            raise self.get_error(PermissionError, id=1)
        if not columns:
            raise self.get_error(ValueError, message="At least one column is required.")

        if not isinstance(self._col_to_idx, dict):
            raise self.get_error(AttributeError, id=4)

        cols_to_drop: list[str] = []
        seen: set[str] = set()
        for col in columns:
            if not isinstance(col, str):
                raise self.get_error(TypeError, message="Column names must be strings.")
            if col in seen:
                continue
            seen.add(col)

            if col == "id":
                raise self.get_error(
                    PermissionError,
                    message="Cannot drop mandatory primary key column 'id'.",
                )
            if col not in self._col_to_idx:
                raise self.get_error(AttributeError, id=23, add=col)
            cols_to_drop.append(col)

        safe_table = self._quote_identifier(self._table_name)
        try:
            for col in cols_to_drop:
                safe_col = self._quote_identifier(col)
                self.cur.execute(f"ALTER TABLE {safe_table} DROP COLUMN {safe_col}")
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            raise self.get_error(type(e), message=str(e)) from e

        # Keep class metadata consistent with dropped columns.
        cls = type(self)
        for col in cols_to_drop:
            if col in cls.__dict__ and isinstance(cls.__dict__[col], KColumn):
                delattr(cls, col)

        cls._orm_meta_cache.pop(cls, None)

        # Rebuild in-memory mapping for the current instance.
        columns_in_class: dict[str, KColumn] = {}
        for base in cls.__mro__:
            for name, obj in base.__dict__.items():
                if isinstance(obj, KColumn) and name not in columns_in_class:
                    columns_in_class[name] = obj

        self.cur.execute(f"PRAGMA table_info({safe_table})")
        rows = self.cur.fetchall()
        db_map = {row[1]: row[0] for row in rows}

        col_to_idx: dict[str, int] = {}
        for attr_name, obj in columns_in_class.items():
            idx = db_map.get(obj._name)
            if idx is not None:
                col_to_idx[attr_name] = idx

        self._col_to_idx = col_to_idx
        self._data = [None] * (max(db_map.values()) + 1 if db_map else 0)

        return f"DROPPED columns {', '.join(cols_to_drop)} from {self._table_name}"

    def drop(self, force: str | None = None) -> Literal["CORRECT: Table dropped", "CANCELED"]:
        """
        Drop the table if it exists (connection-only operation).

        Args:
            force:
                - None -> cancel without dropping;
                - 'n'  -> cancel without dropping;
                - 'y'  -> drop immediately.

        Returns:
            "CORRECT: Table dropped" on success, otherwise "CANCELED".

        Raises:
            PermissionError: if operation is blocked by mode/class flags.
            TypeError: if force parameter is not a string or None.
            ValueError: if force parameter is not 'y' or 'n'.
            Exception: wrapped database error from sqlite execution/commit.
        """

        if self._flag_of_instance[0] & (1 << 0):
            raise self.get_error(PermissionError, id=0)

        if type(self)._flag_of_cls[0] & (1 << 1) or type(self)._flag_of_cls[0] & (1 << 2):
            raise self.get_error(PermissionError, id=1)
        
        if not isinstance(force, (str, type(None))):
            raise self.get_error(
                TypeError, message="Force parameter must be a string or None."
            )
        if force is None:
            return "CANCELED"

        if force.strip().lower() not in ("y", "n"):
            raise self.get_error(ValueError, message="Force parameter must be 'y' or 'n'.")

        if force.strip().lower() == "n":
            return "CANCELED"

        safe_table = self._quote_identifier(self._table_name)
        try:
            self.cur.execute(f"DROP TABLE IF EXISTS {safe_table}")
            self.db.commit()
            return "CORRECT: Table dropped"
        except Exception as e:
            self.db.rollback()
            raise self.get_error(type(e), message=str(e)) from e


    def load(self, row_id: int) -> bool:

        if not (self._flag_of_instance[0] & (1 << 0)):
            raise self.get_error(PermissionError, id=0)
        if type(self)._flag_of_cls[0] & (1 << 2):
            raise self.get_error(PermissionError, id=21)

        safe_table = self._quote_identifier(self._table_name)
        safe_id = self._quote_identifier("id")
        self.cur.execute(f"SELECT * FROM {safe_table} WHERE {safe_id} = ?", (row_id,))
        row = self.cur.fetchone()
        if row:
            self._data = list(row)
            return True

        return False

    def save(self) -> bool:
        """
        Persist the current in-memory row data to the database.

        Behavior:
        - If `id` is `None`, performs INSERT and writes back `lastrowid` into `self._data`.
        - If `id` is set, performs UPDATE by primary key.

        Notes:
        - Operation requires USER mode.
        - Operation is blocked by class lock flags (`knocommands` / `knoread`).
        - Table schema must include `id`.

        Returns:
            `True` when INSERT/UPDATE is committed successfully.

        Raises:
            PermissionError: if mode/class flags block this operation.
            AttributeError: if table mapping is invalid or target row for UPDATE is missing.
            Exception: wrapped database error from sqlite execution/commit.
        """

        if not (self._flag_of_instance[0] & (1 << 0)):
            raise self.get_error(PermissionError, id=0)

        if type(self)._flag_of_cls[0] & (1 << 1) or type(self)._flag_of_cls[0] & (1 << 2):
            raise self.get_error(PermissionError, id=1)

        if (pk_name := "id") not in self._col_to_idx:
            raise self.get_error(AttributeError, id=6)

        all_cols = self._col_to_idx

        # get id from _data
        pk_value = self._data[all_cols[pk_name]]

        update_cols = [name for name in all_cols if name != pk_name]

        try:
            if pk_value is None:
                # logic insert
                cols_names = ", ".join(self._quote_identifier(name) for name in all_cols.keys())
                placeholders = ", ".join(["?"] * len(all_cols))
                safe_table = self._quote_identifier(self._table_name)
                sql = f"INSERT INTO {safe_table} ({cols_names}) VALUES ({placeholders})"
                params = [self._data[all_cols[name]] for name in all_cols]

                self.cur.execute(sql, params)

                if self.cur.lastrowid:
                    self._data[all_cols[pk_name]] = cast(Any, self.cur.lastrowid)
            else:
                # logic update
                if not update_cols:
                    return True  

                set_clause = ", ".join([f"{self._quote_identifier(name)} = ?" for name in update_cols])
                safe_table = self._quote_identifier(self._table_name)
                safe_pk_name = self._quote_identifier(pk_name)
                sql = f"UPDATE {safe_table} SET {set_clause} WHERE {safe_pk_name} = ?"
                params = [self._data[all_cols[name]] for name in update_cols]
                params.append(pk_value)

                self.cur.execute(sql, params)

        
                if self.cur.rowcount == 0:
                    raise self.get_error(AttributeError, id=24, add=str(pk_value))


            if hasattr(self, "db"):
                self.db.commit()

            return True

        except Exception as e:
            if hasattr(self, "db"):
                self.db.rollback()
            raise self.get_error(type(e), message=str(e)) from e


    @staticmethod
    def get_error(error, message="", id=None, add=None) -> Exception:
        """
        Build and return an exception instance with a normalized ORM message.

        Args:
            error: Exception class/type to instantiate.
            message: Fallback message used when `id` is not provided.
            id: Optional internal message identifier from the local `msgs` map.
            add: Optional extra context appended to the final message.

        Returns:
            Instantiated exception object (`error(final_message)`).

        Notes:
        - If `id` is provided and exists in `msgs`, mapped text is used.
        - If `id` is unknown, `message` is used as-is.
        - `add` is appended as `": {add}"` when `add is not None`.
        """
        msgs = {
            0: "Operation is not permitted for the current connection mode.",
            1: "Operation blocked by class-level lock flags.",
            2: "Reserved/legacy error id.",
            3: "Operation restricted: Column access requires USER mode. Current connection is not authorized.",
            4: "Table doesn't have column",
            5: "Reserved/legacy error id.",
            6: "Database must have a column named 'id' for save()",
            7: "Class isn't registered.",
            8: "Name table reserved word in SQLite. Choose another name.",
            9: "The table has no KColumn defined.",
            10: "The table is missing the mandatory 'id' column.",
            11: "SQL command must be a string",
            12: "SQL command cannot be empty",
            13: "SQL type is invalid",
            14: "Sql3_request-select must be string",
            15: "Sql3_request-select doesn't start with 'SELECT'",
            16: "At least one range is required.",
            17: "At least one filter is required.",
            18: "SQL column definition is invalid for SQLite.",
            19: "Reserved/legacy error id.",
            20: "SQL command rejected by ORM validator.",
            21: "Read access is globally disabled for this class (knoread).",
            22: "Unsafe SQL identifier. Use letters/digits/underscore and do not start with a digit.",
            23: "Unknown or unauthorized column in filter/range.",
            24: "No row found to update by primary key 'id'.",
        }

        txt = msgs.get(id, message) if id is not None else message

        if add is not None:
            txt = f"{txt}: {add}"

        return error(txt)



def kregister(cls: Type[T]) -> Type[T]:
    """
    Register ORM model class and enforce base-initializer contract.
    """
    if not issubclass(cls, KCoreORM):
        raise TypeError(f"Class {cls.__name__} must inherit from KCoreORM to be registered.")

    if not hasattr(cls, "_table_name") or not isinstance(cls._table_name, str):
        raise TypeError(f"Class {cls.__name__} must define string _table_name.")
    if not hasattr(cls, "_flag_of_cls"):
        raise TypeError(f"Class {cls.__name__} must define _flag_of_cls.")
    if not isinstance(cls._flag_of_cls, bytearray) or len(cls._flag_of_cls) == 0:
        raise TypeError(f"Class {cls.__name__} must have non-empty bytearray _flag_of_cls.")

    if cls._table_name.upper() in cls.SQLITE_RESERVED_WORDS:
        raise cls.get_error(
            AttributeError,
            message=f"Name table '{cls._table_name}' reserved word in SQLite. Choose another name.",
        )

    # Collect all KColumn descriptors from class hierarchy.
    columns = {}
    for base in cls.__mro__:
        for name, obj in base.__dict__.items():
            if isinstance(obj, KColumn) and name not in columns:
                columns[name] = obj
    if not columns:
        raise cls.get_error(
            AttributeError,
            message=f"The table '{cls._table_name}' has no KColumn defined.",
        )

    if "id" not in columns:
        raise cls.get_error(
            AttributeError,
            message=f"The table '{cls._table_name}' is missing the mandatory 'id' column.",
        )
    id_sql = columns["id"].sql_types.upper()
    if "INTEGER" not in id_sql or "PRIMARY KEY" not in id_sql:
        raise cls.get_error(
            TypeError,
            message=(
                f"The table '{cls._table_name}' must define id as "
                "INTEGER PRIMARY KEY (optionally AUTOINCREMENT)."
            ),
        )

    # Idempotency guard: avoid wrapping __init__ more than once.
    if cls.__dict__.get("__kregister_wrapped__", False):
        return cls

    # Isolate class flags from base classes (avoid shared mutable bytearray).
    if "_flag_of_cls" not in cls.__dict__:
        cls._flag_of_cls = bytearray(cls._flag_of_cls)

    # Activate class (bit 0): constructor is allowed for registered classes.
    cls._flag_of_cls[0] |= 1 << 0

    orig_init = cls.__init__

    def wrapped_init(self, *args, **kwargs):
        # Clear the "base init ran" marker, then run user init.
        # If user forgets to call super().__init__(db, cursor), we'll detect it.
        if hasattr(self, "_flag_of_instance") and isinstance(self._flag_of_instance, bytearray):
            self._flag_of_instance[0] &= ~(1 << 1)

        orig_init(self, *args, **kwargs)

        if not hasattr(self, "_flag_of_instance") or not (self._flag_of_instance[0] & (1 << 1)):
            raise cls.get_error(
                RuntimeError,
                message=f"{cls.__name__}.__init__ must call super().__init__(db, cursor)",
            )

    cls.__init__ = wrapped_init
    setattr(cls, "__kregister_wrapped__", True)
    return cls




@decorator
def knocommands(cls: Type[T]) -> Type[T]:
    """
    Enable class-level write/command lock (bit 1)."""
    if "_flag_of_cls" not in cls.__dict__:
        cls._flag_of_cls = bytearray(cls._flag_of_cls)
    cls._flag_of_cls[0] |= 1 << 1
    return cls


@decorator
def knoread(cls: Type[T]) -> Type[T]:
    """
    Enable class-level read lock (bit 2).
    """
    if "_flag_of_cls" not in cls.__dict__:
        cls._flag_of_cls = bytearray(cls._flag_of_cls)
    cls._flag_of_cls[0] |= 1 << 2
    return cls


def KUserMode(conn_with_orm: T) -> T:
    """
    Build and return a USER-mode proxy from a base ORM connection.

    Kept as a plain generic function so IDEs can infer the concrete model type
    from `KUserMode(conn)` more reliably than from a dynamic class factory.
    """
    # Build a user-mode proxy instance around base ORM connection.
    class UserMode(conn_with_orm.__class__):
        # Explicit slots isolate instance state for the user-mode wrapper.
        __slots__ = (
            "_flag_of_instance",
            "_data",
            "db",
            "cur",
            "_list_requests",
            "_col_to_idx",
            "_table_name",
        )

        def __init__(self, base: T) -> None:
            # Share DB resources from the base connection.
            self.db = base.db
            self.cur = base.cur
            self._table_name = base._table_name
            self._col_to_idx = base._col_to_idx
            self._list_requests = []

            # Enable USER mode flag.
            self._flag_of_instance = bytearray(1)
            self._flag_of_instance[0] = 1 << 0
            # Own in-memory row buffer per user wrapper instance.
            if isinstance(base._data, list):
                self._data = [None] * len(base._data)
            else:
                self._data = []

    # Return USER-mode proxy instance.
    return cast(T, UserMode(conn_with_orm))

