import sqlite3
import unittest
from typing import Any, Callable, Iterable, cast

from kerastack.KergaSQL import (
    KAND,
    KOR,
    KColumn,
    KCoreORM,
    KUserMode,
    kregister,
    knocommands,
    knoread,
)
from kerastack.decorators import check_columns_for_update


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NAME_COUNTER = 0


def _next_name(prefix: str) -> str:
    global _NAME_COUNTER
    _NAME_COUNTER += 1
    return f"{prefix}_{_NAME_COUNTER}"


def _expect_raises(
    test: unittest.TestCase,
    exc_type: type[BaseException],
    fn: Callable[[], Any],
    contains: str | None = None,
) -> BaseException:
    with test.assertRaises(exc_type) as ctx:
        fn()
    err = ctx.exception
    if contains is not None:
        test.assertIn(contains, str(err))
    return err


def _create_model(
    *,
    class_prefix: str,
    table_prefix: str,
    with_id: bool = True,
    with_name: bool = True,
    with_age: bool = True,
    call_super_init: bool = True,
    register: bool = True,
    decorators: Iterable[Callable[[type], type]] = (),
) -> type[KCoreORM]:
    class_name = _next_name(class_prefix)
    table_name = _next_name(table_prefix)

    namespace: dict[str, Any] = {
        "__slots__": (),
        "_table_name": table_name,
    }

    if with_id:
        namespace["id"] = KColumn("INTEGER PRIMARY KEY AUTOINCREMENT")
    if with_name:
        namespace["name"] = KColumn("TEXT")
    if with_age:
        namespace["age"] = KColumn("INTEGER")

    if call_super_init:
        def __init__(self: KCoreORM, db: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
            KCoreORM.__init__(self, db, cursor)
    else:
        def __init__(self: KCoreORM, db: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
            # Intentionally skip base init for contract-check tests.
            _ = (db, cursor)

    namespace["__init__"] = __init__

    cls = cast(type[KCoreORM], type(class_name, (KCoreORM,), namespace))
    if register:
        cls = kregister(cls)
    for dec in decorators:
        cls = cast(type[KCoreORM], dec(cls))
    return cls


def _build_env(model_cls: type[KCoreORM]) -> tuple[sqlite3.Connection, sqlite3.Cursor, Any, Any]:
    db = sqlite3.connect(":memory:")
    cur = db.cursor()
    base = model_cls(db, cur)
    user = KUserMode(base)
    return db, cur, base, user


def _insert_rows(orm: KCoreORM, table_name: str, rows: list[tuple[str, int]]) -> None:
    for name, age in rows:
        orm.cur.execute(f'INSERT INTO "{table_name}" (name, age) VALUES ("{name}", {age})')
    orm.db.commit()


# ---------------------------------------------------------------------------
# Registration and class-level behavior
# ---------------------------------------------------------------------------


class TestRegistrationAndSchema(unittest.TestCase):
    def test_kregister_rejects_non_subclass(self) -> None:
        class NotOrm:
            pass

        _expect_raises(
            self,
            TypeError,
            lambda: kregister(cast(Any, NotOrm)),
            "must inherit from KCoreORM",
        )

    def test_kregister_requires_columns(self) -> None:
        cls = _create_model(
            class_prefix="NoColsClass",
            table_prefix="no_cols_table",
            with_id=False,
            with_name=False,
            with_age=False,
            register=False,
        )
        _expect_raises(self, AttributeError, lambda: kregister(cls), "has no KColumn defined")

    def test_kregister_requires_id_column(self) -> None:
        cls = _create_model(
            class_prefix="NoIdClass",
            table_prefix="no_id_table",
            with_id=False,
            with_name=True,
            with_age=True,
            register=False,
        )
        _expect_raises(self, AttributeError, lambda: kregister(cls), "mandatory 'id' column")

    def test_kregister_rejects_non_integer_primary_key_id(self) -> None:
        class_name = _next_name("BadIdType")
        table_name = _next_name("bad_id_type_table")

        namespace: dict[str, Any] = {
            "__slots__": (),
            "_table_name": table_name,
            "id": KColumn("TEXT"),
            "name": KColumn("TEXT"),
            "age": KColumn("INTEGER"),
        }

        def __init__(self: KCoreORM, db: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
            KCoreORM.__init__(self, db, cursor)

        namespace["__init__"] = __init__
        cls = cast(type[KCoreORM], type(class_name, (KCoreORM,), namespace))
        _expect_raises(
            self,
            TypeError,
            lambda: kregister(cls),
            "INTEGER PRIMARY KEY",
        )

    def test_kregister_rejects_reserved_table_name(self) -> None:
        cls = _create_model(
            class_prefix="ReservedClass",
            table_prefix="reserved_table",
            register=False,
        )
        cls._table_name = "SELECT"
        _expect_raises(self, AttributeError, lambda: kregister(cls), "reserved word")

    def test_init_rejects_unregistered_model(self) -> None:
        cls = _create_model(class_prefix="NoRegInit", table_prefix="no_reg_init", register=False)
        db = sqlite3.connect(":memory:")
        cur = db.cursor()
        _expect_raises(self, RuntimeError, lambda: cls(db, cur), "isn't registered")

    def test_kregister_is_idempotent_for_init_wrapping(self) -> None:
        cls = _create_model(class_prefix="RegClass", table_prefix="reg_table", register=True)
        wrapped_once = cls.__init__
        wrapped_again = kregister(cls).__init__
        self.assertIs(wrapped_once, wrapped_again)

    def test_kregister_enforces_super_init_contract(self) -> None:
        cls = _create_model(
            class_prefix="NoSuperInit",
            table_prefix="no_super_table",
            call_super_init=False,
            register=True,
        )
        db = sqlite3.connect(":memory:")
        cur = db.cursor()
        _expect_raises(self, RuntimeError, lambda: cls(db, cur), "must call super().__init__")

    def test_flag_buffers_are_isolated_between_classes(self) -> None:
        a = _create_model(class_prefix="IsoA", table_prefix="iso_a", register=True)
        b = _create_model(class_prefix="IsoB", table_prefix="iso_b", register=True)
        self.assertIsNot(a._flag_of_cls, b._flag_of_cls)

        b_before = b._flag_of_cls[0]
        knocommands(a)
        self.assertEqual(b._flag_of_cls[0], b_before)

    def test_check_columns_for_update_rebuilds_sql_types(self) -> None:
        shared_table = _next_name("schema_sync_table")
        db = sqlite3.connect(":memory:")
        cur = db.cursor()

        @kregister
        class SchemaV1(KCoreORM):
            __slots__ = ()
            _table_name = shared_table
            id = KColumn("INTEGER PRIMARY KEY AUTOINCREMENT")
            payload = KColumn("INTEGER")

            def __init__(self, db: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
                super().__init__(db, cursor)

        base_v1 = SchemaV1(db, cur)
        user_v1 = KUserMode(base_v1)
        user_v1.payload = 123
        self.assertTrue(user_v1.save())
        old_id = user_v1.id

        @check_columns_for_update
        @kregister
        class SchemaV2(KCoreORM):
            __slots__ = ()
            _table_name = shared_table
            id = KColumn("INTEGER PRIMARY KEY AUTOINCREMENT")
            payload = KColumn("TEXT")

            def __init__(self, db: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
                super().__init__(db, cursor)

        _ = SchemaV2(db, cur)
        cur.execute(f'PRAGMA table_info("{shared_table}")')
        info = cur.fetchall()
        type_map = {row[1]: row[2].upper() for row in info}

        self.assertEqual(type_map["payload"], "TEXT")

        user_v2 = KUserMode(SchemaV2(db, cur))
        self.assertTrue(user_v2.load(old_id))


# ---------------------------------------------------------------------------
# SQL validation and execution pipeline
# ---------------------------------------------------------------------------


class TestSqlValidationAndExecution(unittest.TestCase):
    def test_check_sql3_request_accepts_valid_select(self) -> None:
        cls = _create_model(class_prefix="CheckOK", table_prefix="check_ok")
        _, _, _, user = _build_env(cls)
        ok, reason = user.check_sql3_request("SELECT 1")
        self.assertTrue(ok)
        self.assertEqual(reason, "")

    def test_check_sql3_request_rejects_multiple_statements(self) -> None:
        cls = _create_model(class_prefix="CheckMulti", table_prefix="check_multi")
        _, _, _, user = _build_env(cls)
        ok, reason = user.check_sql3_request("SELECT 1; SELECT 2")
        self.assertFalse(ok)
        self.assertIn("Multiple SQL statements", reason)

    def test_check_sql3_request_rejects_unbalanced_parentheses(self) -> None:
        cls = _create_model(class_prefix="CheckParen", table_prefix="check_paren")
        _, _, _, user = _build_env(cls)
        ok, reason = user.check_sql3_request("SELECT (1")
        self.assertFalse(ok)
        self.assertIn("Unbalanced parentheses", reason)

    def test_execute_rejects_user_mode(self) -> None:
        cls = _create_model(class_prefix="ExecUserMode", table_prefix="exec_user_mode")
        _, _, _, user = _build_env(cls)
        _expect_raises(
            self, PermissionError, lambda: user.execute("SELECT 1"), "current connection mode"
        )

    def test_execute_rejects_non_str_in_list(self) -> None:
        cls = _create_model(class_prefix="ExecTypes", table_prefix="exec_types")
        _, _, base, _ = _build_env(cls)
        _expect_raises(self, TypeError, lambda: base.execute(cast(Any, ["SELECT 1", 42])), "must be a string")

    def test_execute_rejects_invalid_sql(self) -> None:
        cls = _create_model(class_prefix="ExecInvalid", table_prefix="exec_invalid")
        _, _, base, _ = _build_env(cls)
        _expect_raises(self, sqlite3.OperationalError, lambda: base.execute("SELCT 1"), "rejected by ORM validator")

    def test_add_requests_and_queue_execution(self) -> None:
        cls = _create_model(class_prefix="ExecQueue", table_prefix="exec_queue")
        _, _, base, user = _build_env(cls)

        base.add_requests(
            [
                f'INSERT INTO "{cls._table_name}" (name, age) VALUES ("alice", 11)',
                f'INSERT INTO "{cls._table_name}" (name, age) VALUES ("bob", 22)',
            ]
        )
        self.assertEqual(len(base.get_requests()), 2)

        # Empty execute still flushes queued requests.
        base.execute("")
        self.assertEqual(base.get_requests(), [])

        rows = base.fetchall(f'SELECT name, age FROM "{cls._table_name}" ORDER BY age')
        self.assertEqual(rows, [("alice", 11), ("bob", 22)])

    def test_add_requests_rejects_invalid_sql(self) -> None:
        cls = _create_model(class_prefix="ExecQueueBad", table_prefix="exec_queue_bad")
        _, _, base, _ = _build_env(cls)
        _expect_raises(self, sqlite3.OperationalError, lambda: base.add_requests("SELCT 1"), "rejected by ORM validator")

    def test_add_requests_rejects_user_mode(self) -> None:
        cls = _create_model(class_prefix="ExecQueueUserMode", table_prefix="exec_queue_user_mode")
        _, _, _, user = _build_env(cls)
        _expect_raises(
            self, PermissionError, lambda: user.add_requests("SELECT 1"), "current connection mode"
        )

    def test_execute_respects_knocommands_lock(self) -> None:
        cls = _create_model(
            class_prefix="ExecCmdLock",
            table_prefix="exec_cmd_lock",
            decorators=(knocommands,),
        )
        _, _, base, _ = _build_env(cls)
        _expect_raises(self, PermissionError, lambda: base.execute("SELECT 1"), "class-level lock")


# ---------------------------------------------------------------------------
# Fetch APIs and descriptor behavior
# ---------------------------------------------------------------------------


class TestFetchAndDescriptors(unittest.TestCase):
    def test_descriptor_set_get_in_user_mode(self) -> None:
        cls = _create_model(class_prefix="DescOK", table_prefix="desc_ok")
        _, _, _, user = _build_env(cls)
        user.name = "neo"
        user.age = 42
        self.assertEqual(user.name, "neo")
        self.assertEqual(user.age, 42)

    def test_descriptor_get_set_blocked_outside_user_mode(self) -> None:
        cls = _create_model(class_prefix="DescBase", table_prefix="desc_base")
        db = sqlite3.connect(":memory:")
        cur = db.cursor()
        base = cls(db, cur)

        _expect_raises(self, Exception, lambda: setattr(base, "name", "x"), "USER mode")
        _expect_raises(self, Exception, lambda: getattr(base, "name"), "USER mode")

    def test_descriptor_blocked_by_knoread(self) -> None:
        cls = _create_model(
            class_prefix="DescReadLock",
            table_prefix="desc_read_lock",
            decorators=(knoread,),
        )
        _, _, _, user = _build_env(cls)
        _expect_raises(self, PermissionError, lambda: setattr(user, "name", "x"), "knoread")
        _expect_raises(self, PermissionError, lambda: getattr(user, "name"), "knoread")

    def test_fetchall_success_path(self) -> None:
        cls = _create_model(class_prefix="FetchAll", table_prefix="fetch_all")
        _, _, base, _ = _build_env(cls)
        _insert_rows(base, cls._table_name, [("a", 1), ("b", 2)])
        rows = base.fetchall(f'SELECT name, age FROM "{cls._table_name}" ORDER BY age')
        self.assertEqual(rows, [("a", 1), ("b", 2)])

    def test_fetchall_requires_select(self) -> None:
        cls = _create_model(class_prefix="FetchAllBad", table_prefix="fetch_all_bad")
        _, _, base, _ = _build_env(cls)
        _expect_raises(self, TypeError, lambda: base.fetchall("DELETE FROM x"), "doesn't start with 'SELECT'")

    def test_fetchone_two_step_mode(self) -> None:
        cls = _create_model(class_prefix="FetchOneTwoStep", table_prefix="fetch_one_two_step")
        _, _, base, _ = _build_env(cls)
        _insert_rows(base, cls._table_name, [("x", 10), ("y", 20)])

        first_call = base.fetchone(f'SELECT name FROM "{cls._table_name}" ORDER BY age')
        self.assertEqual(first_call, "SELECTED")
        self.assertEqual(base.fetchone(""), ("x",))
        self.assertEqual(base.fetchone(""), ("y",))

    def test_fetchone_immediate_mode(self) -> None:
        cls = _create_model(class_prefix="FetchOneImmediate", table_prefix="fetch_one_immediate")
        _, _, base, _ = _build_env(cls)
        _insert_rows(base, cls._table_name, [("x", 10), ("y", 20)])
        base.fetchone(f'SELECT name FROM "{cls._table_name}" ORDER BY age')
        self.assertEqual(base.fetchone("", immediate=1), ("x",))

    def test_knoread_blocks_fetchall_and_fetchone(self) -> None:
        cls = _create_model(
            class_prefix="FetchReadLock",
            table_prefix="fetch_read_lock",
            decorators=(knoread,),
        )
        _, _, base, _ = _build_env(cls)
        _expect_raises(self, PermissionError, lambda: base.fetchall("SELECT 1"), "knoread")
        _expect_raises(self, PermissionError, lambda: base.fetchone("SELECT 1"), "knoread")

    def test_user_mode_rejects_fetchall_and_fetchone(self) -> None:
        cls = _create_model(class_prefix="FetchUserBlocked", table_prefix="fetch_user_blocked")
        _, _, _, user = _build_env(cls)
        _expect_raises(self, PermissionError, lambda: user.fetchall("SELECT 1"), "current connection mode")
        _expect_raises(self, PermissionError, lambda: user.fetchone("SELECT 1"), "current connection mode")


# ---------------------------------------------------------------------------
# Delete API coverage
# ---------------------------------------------------------------------------


class TestDeleteApis(unittest.TestCase):
    def test_delete_requires_at_least_one_filter(self) -> None:
        cls = _create_model(class_prefix="DelReq", table_prefix="del_req")
        _, _, base, _ = _build_env(cls)
        _expect_raises(self, ValueError, lambda: base.delete(), "At least one filter")

    def test_delete_rejects_unknown_column(self) -> None:
        cls = _create_model(class_prefix="DelUnknown", table_prefix="del_unknown")
        _, _, base, _ = _build_env(cls)
        _expect_raises(self, AttributeError, lambda: base.delete(ghost=1), "Unknown or unauthorized column")

    def test_delete_success(self) -> None:
        cls = _create_model(class_prefix="DelOK", table_prefix="del_ok")
        _, _, base, _ = _build_env(cls)
        _insert_rows(base, cls._table_name, [("a", 10), ("b", 20), ("c", 30)])
        msg = base.delete(name="b")
        self.assertIn("DELETED 1", msg)
        rows = base.fetchall(f'SELECT name FROM "{cls._table_name}" ORDER BY age')
        self.assertEqual(rows, [("a",), ("c",)])

    def test_delete_ranges_requires_input(self) -> None:
        cls = _create_model(class_prefix="DelRangeReq", table_prefix="del_range_req")
        _, _, base, _ = _build_env(cls)
        _expect_raises(self, ValueError, lambda: base.delete_ranges(), "At least one range")

    def test_delete_ranges_rejects_unknown_column(self) -> None:
        cls = _create_model(class_prefix="DelRangeUnknown", table_prefix="del_range_unknown")
        _, _, base, _ = _build_env(cls)
        _expect_raises(self, AttributeError, lambda: base.delete_ranges(ghost=(1, 2)), "Unknown or unauthorized column")

    def test_delete_ranges_rejects_invalid_format(self) -> None:
        cls = _create_model(class_prefix="DelRangeFmt", table_prefix="del_range_fmt")
        _, _, base, _ = _build_env(cls)
        _expect_raises(self, TypeError, lambda: base.delete_ranges(id=(1,)), "Invalid range format")

    def test_delete_ranges_rejects_last_logic_marker(self) -> None:
        cls = _create_model(class_prefix="DelRangeLast", table_prefix="del_range_last")
        _, _, base, _ = _build_env(cls)
        _expect_raises(
            self,
            TypeError,
            lambda: base.delete_ranges(id=(1, 10), age=(5, 15, KOR)),
            "Logic marker on last range",
        )

    def test_delete_ranges_accepts_exact_value_shorthand(self) -> None:
        cls = _create_model(class_prefix="DelRangeExact", table_prefix="del_range_exact")
        _, _, base, _ = _build_env(cls)
        _insert_rows(base, cls._table_name, [("a", 10), ("b", 20), ("a", 30)])

        msg = base.delete_ranges(name="a")
        self.assertIn("DELETED 2", msg)
        rows = base.fetchall(f'SELECT name, age FROM "{cls._table_name}" ORDER BY age')
        self.assertEqual(rows, [("b", 20)])

    def test_delete_ranges_logic_semantics_or_and(self) -> None:
        cls = _create_model(class_prefix="DelRangeLogic", table_prefix="del_range_logic")
        _, _, base, _ = _build_env(cls)
        _insert_rows(base, cls._table_name, [("n1", 10), ("n2", 20), ("n3", 30), ("n4", 40)])

        # id in [1,2] OR age in [35,50] should remove ids 1,2,4 and keep id 3.
        msg = base.delete_ranges(id=(1, 2, KOR), age=(35, 50))
        self.assertIn("DELETED 3", msg)
        rows = base.fetchall(f'SELECT id, age FROM "{cls._table_name}" ORDER BY id')
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], 30)

    def test_delete_ranges_nested_bounds_format(self) -> None:
        cls = _create_model(class_prefix="DelRangeNested", table_prefix="del_range_nested")
        _, _, base, _ = _build_env(cls)
        _insert_rows(base, cls._table_name, [("x", 1), ("y", 2)])
        msg = base.delete_ranges(age=((100, 200), KAND), id=(500, 500))
        self.assertIn("DELETED 0", msg)

    def test_user_mode_rejects_delete_and_delete_ranges(self) -> None:
        cls = _create_model(class_prefix="DelUserBlocked", table_prefix="del_user_blocked")
        _, _, _, user = _build_env(cls)
        _expect_raises(self, PermissionError, lambda: user.delete(name="x"), "current connection mode")
        _expect_raises(
            self,
            PermissionError,
            lambda: user.delete_ranges(id=(1, 2)),
            "current connection mode",
        )


# ---------------------------------------------------------------------------
# Drop APIs coverage
# ---------------------------------------------------------------------------


class TestDropApis(unittest.TestCase):
    def test_drop_paths_none_n_y(self) -> None:
        cls = _create_model(class_prefix="DropPaths", table_prefix="drop_paths")
        _, _, base, _ = _build_env(cls)
        self.assertEqual(base.drop(None), "CANCELED")
        self.assertEqual(base.drop("n"), "CANCELED")
        self.assertEqual(base.drop("y"), "CORRECT: Table dropped")

    def test_drop_rejects_invalid_force(self) -> None:
        cls = _create_model(class_prefix="DropBadForce", table_prefix="drop_bad_force")
        _, _, base, _ = _build_env(cls)
        _expect_raises(self, ValueError, lambda: base.drop("zzz"), "must be 'y' or 'n'")

    def test_drop_columns_conn_only(self) -> None:
        cls = _create_model(class_prefix="DropColsConnOnly", table_prefix="drop_cols_conn_only")
        _, _, base, user = _build_env(cls)
        _expect_raises(
            self, PermissionError, lambda: user.drop_columns("age"), "current connection mode"
        )
        # base path should not raise for valid column
        self.assertIn("DROPPED columns", base.drop_columns("age"))

    def test_drop_columns_rejects_empty(self) -> None:
        cls = _create_model(class_prefix="DropColsEmpty", table_prefix="drop_cols_empty")
        _, _, base, _ = _build_env(cls)
        _expect_raises(self, ValueError, lambda: base.drop_columns(), "At least one column is required")

    def test_drop_columns_rejects_non_string(self) -> None:
        cls = _create_model(class_prefix="DropColsType", table_prefix="drop_cols_type")
        _, _, base, _ = _build_env(cls)
        _expect_raises(self, TypeError, lambda: base.drop_columns(cast(Any, 1)), "must be strings")

    def test_drop_columns_rejects_unknown_column(self) -> None:
        cls = _create_model(class_prefix="DropColsUnknown", table_prefix="drop_cols_unknown")
        _, _, base, _ = _build_env(cls)
        _expect_raises(self, AttributeError, lambda: base.drop_columns("ghost"), "Unknown or unauthorized column")

    def test_drop_columns_protects_id(self) -> None:
        cls = _create_model(class_prefix="DropColsId", table_prefix="drop_cols_id")
        _, _, base, _ = _build_env(cls)
        _expect_raises(self, PermissionError, lambda: base.drop_columns("id"), "Cannot drop mandatory primary key")

    def test_drop_columns_updates_schema_and_mapping(self) -> None:
        cls = _create_model(class_prefix="DropColsUpdate", table_prefix="drop_cols_update")
        db, cur, base, _ = _build_env(cls)
        self.assertIn("age", base._col_to_idx)

        msg = base.drop_columns("age")
        self.assertIn("DROPPED columns age", msg)
        self.assertNotIn("age", base._col_to_idx)
        self.assertNotIn("age", cls.__dict__)

        # New instances should also keep the updated schema state.
        other = cls(db, cur)
        self.assertNotIn("age", other._col_to_idx)


# ---------------------------------------------------------------------------
# Save / Load and mode restrictions
# ---------------------------------------------------------------------------


class TestSaveLoadAndMode(unittest.TestCase):
    def test_save_insert_then_update(self) -> None:
        cls = _create_model(class_prefix="SaveIU", table_prefix="save_iu")
        _, _, base, user = _build_env(cls)

        user.name = "alice"
        user.age = 10
        self.assertIsNone(user.id)
        self.assertTrue(user.save())
        inserted_id = user.id
        self.assertIsInstance(inserted_id, int)

        user.age = 11
        self.assertTrue(user.save())
        rows = base.fetchall(f'SELECT id, age FROM "{cls._table_name}"')
        self.assertEqual(rows, [(inserted_id, 11)])

    def test_save_update_missing_row_raises(self) -> None:
        cls = _create_model(class_prefix="SaveMissing", table_prefix="save_missing")
        _, _, _, user = _build_env(cls)
        user.id = 99999
        user.name = "ghost"
        user.age = 1
        _expect_raises(self, AttributeError, lambda: user.save(), "No row found to update")

    def test_save_blocked_for_non_user(self) -> None:
        cls = _create_model(class_prefix="SaveBase", table_prefix="save_base")
        db = sqlite3.connect(":memory:")
        cur = db.cursor()
        base = cls(db, cur)
        _expect_raises(self, PermissionError, lambda: base.save(), "current connection mode")

    def test_save_blocked_by_class_locks(self) -> None:
        cls = _create_model(
            class_prefix="SaveLock",
            table_prefix="save_lock",
            decorators=(knocommands,),
        )
        _, _, _, user = _build_env(cls)
        _expect_raises(self, PermissionError, lambda: user.save(), "class-level lock")

    def test_load_success(self) -> None:
        cls = _create_model(class_prefix="LoadOK", table_prefix="load_ok")
        _, _, _, user = _build_env(cls)
        _insert_rows(user, cls._table_name, [("alice", 50)])
        self.assertTrue(user.load(1))
        self.assertEqual(user.name, "alice")
        self.assertEqual(user.age, 50)

    def test_load_returns_false_when_row_missing(self) -> None:
        cls = _create_model(class_prefix="LoadMiss", table_prefix="load_miss")
        _, _, _, user = _build_env(cls)
        self.assertFalse(user.load(123))

    def test_load_blocked_for_non_user(self) -> None:
        cls = _create_model(class_prefix="LoadBase", table_prefix="load_base")
        db = sqlite3.connect(":memory:")
        cur = db.cursor()
        base = cls(db, cur)
        _expect_raises(self, PermissionError, lambda: base.load(1), "current connection mode")

    def test_load_blocked_by_knoread(self) -> None:
        cls = _create_model(
            class_prefix="LoadReadLock",
            table_prefix="load_read_lock",
            decorators=(knoread,),
        )
        _, _, _, user = _build_env(cls)
        _expect_raises(self, PermissionError, lambda: user.load(1), "knoread")

    def test_kusermode_data_buffers_are_isolated(self) -> None:
        cls = _create_model(class_prefix="UserIso", table_prefix="user_iso")
        db = sqlite3.connect(":memory:")
        cur = db.cursor()
        base = cls(db, cur)

        u1 = cast(Any, KUserMode(base))
        u2 = cast(Any, KUserMode(base))
        u1.name = "u1"
        u2.name = "u2"
        self.assertEqual(u1.name, "u1")
        self.assertEqual(u2.name, "u2")
        self.assertIsNot(u1._data, u2._data)

    def test_kusermode_result_is_subclass_instance(self) -> None:
        cls = _create_model(class_prefix="UserSubType", table_prefix="user_subtype")
        _, _, base, user = _build_env(cls)
        self.assertIsInstance(user, cls)
        self.assertIsNot(user, base)


# ---------------------------------------------------------------------------
# Error helper
# ---------------------------------------------------------------------------


class TestGetErrorHelper(unittest.TestCase):
    def test_get_error_uses_id_message(self) -> None:
        err = KCoreORM.get_error(ValueError, id=0)
        self.assertIsInstance(err, ValueError)
        self.assertIn("current connection mode", str(err))

    def test_get_error_fallback_message(self) -> None:
        err = KCoreORM.get_error(RuntimeError, message="custom")
        self.assertEqual(str(err), "custom")

    def test_get_error_appends_add_when_present(self) -> None:
        err = KCoreORM.get_error(ValueError, id=23, add="ghost")
        self.assertIn("ghost", str(err))


# ---------------------------------------------------------------------------
# Integration smoke
# ---------------------------------------------------------------------------


class TestIntegrationSmoke(unittest.TestCase):
    def test_end_to_end_crud_and_filters(self) -> None:
        cls = _create_model(class_prefix="Smoke", table_prefix="smoke")
        _, _, base, user = _build_env(cls)

        user.name = "a"
        user.age = 10
        self.assertTrue(user.save())
        id1 = user.id

        user.id = None
        user.name = "b"
        user.age = 20
        self.assertTrue(user.save())
        id2 = user.id

        rows = base.fetchall(f'SELECT id, name, age FROM "{cls._table_name}" ORDER BY id')
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], id1)
        self.assertEqual(rows[1][0], id2)

        base.delete_ranges(id=(id1, id1, KOR), age=(100, 200))
        rows = base.fetchall(f'SELECT id, name FROM "{cls._table_name}" ORDER BY id')
        self.assertEqual(rows, [(id2, "b")])

        base.delete(id=id2)
        self.assertEqual(base.fetchall(f'SELECT id FROM "{cls._table_name}"'), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
