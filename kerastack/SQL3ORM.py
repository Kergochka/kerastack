import re
import sqlite3
from abc import abstractmethod
from typing import Any, Iterable, Literal, Optional, Self, Type, TypeVar, Union, cast

from kerastack.decorators import decorator

T = TypeVar("T", bound="KCoreORM")


class UserPermissionError(Exception):
    """Выбрасывается, когда экземпляр в USER-режиме пытается выполнить запрещенное действие."""

    __slots__ = ()


class ConnPermissionError(Exception):
    """Выбрасывается, когда операция не разрешена для текущего режима соединения."""

    __slots__ = ()


class __kLogicNode:
    """
    Базовый класс для синглтонов логических маркеров (используются как токены логики ORM/запросов).

    Каждый конкретный подкласс предоставляет не более одного экземпляра через ``__new__`` (singleton).
    Подклассы должны определить ``_instance = None`` на уровне класса.
    """

    __slots__ = ()

    @abstractmethod
    def __new__(cls, *args: Any, **kwds: Any) -> Self:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance


class __kANDcls(__kLogicNode):
    """Синглтон, представляющий логический AND (конъюнкцию) в композиции запросов/флагов."""

    __slots__ = ()
    _instance = None


class __kORcls(__kLogicNode):
    """Синглтон, представляющий логический OR (дизъюнкцию) в композиции запросов/флагов."""

    __slots__ = ()
    _instance = None


# Синглтоны уровня модуля: используйте их как логические значения (а не как конструкторы на каждый вызов).
KAND = __kANDcls()
KOR = __kORcls()


class KColumn:
    """
    Экземпляры KColumn работают как дескрипторы колонок таблицы.
    Предполагается, что доступ к ним в основном идет в "пользовательском" контексте,
    что позволяет работать с данными конкретного пользователя или записи.
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
        Выполнить один SQL-запрос или список SQL-запросов текущим курсором.

        Параметры:
            sql3_data: Одна SQL-строка или список SQL-строк.
        """
        if isinstance(sql3_data, list):
            for i in sql3_data:
                self.cur.execute(i)
            return
        self.cur.execute(sql3_data)

    def fetchall(self, sql3_select: str) -> list[tuple[Any, ...]]:
        """
        Получить все строки из базы данных.
        """
        self.cur.execute(sql3_select)
        return self.cur.fetchall()

    def fetchone(
        self, immediate: None | Any, sql3_select: str = ""
    ) -> Optional[tuple[Any, ...]] | Literal["SELECTED"]:
        """
        Получить одну строку или подготовить SELECT для следующего чтения.

        Поведение:
        - Если `immediate` не None, сразу возвращает `cursor.fetchone()`.
        - Иначе, если `sql3_select` пуст, также сразу возвращает `cursor.fetchone()`.
        - Иначе выполняет `sql3_select` и возвращает маркер `"SELECTED"`.

        Это позволяет два режима:
        1) двухшаговый режим: сначала вызвать с SQL, затем повторно без SQL;
        2) немедленный режим: передать `immediate`, чтобы принудительно сделать прямой fetch.
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

    # Флаги уровня класса хранятся в `_flag_of_cls` (bytearray):
    # - бит 0: класс зарегистрирован
    # - бит 1: глобальная блокировка записи/команд (execute, save, delete, delete_ranges, drop, drop_columns)
    # - бит 2: глобальная блокировка чтения/записи/команд
    # - бит 3: всегда синхронизировать схему таблицы по текущим KColumn sql_types
    _flag_of_cls = bytearray(1)
    # Флаги уровня экземпляра хранятся в `_flag_of_instance` (bytearray):
    # - бит 0: пользовательский режим
    # - бит 1: маркер вызова базового инициализатора (super().__init__ вызван)

    _orm_meta_cache: dict[tuple[type["KCoreORM"], str], dict[str, Any]] = {}

    @staticmethod
    def _get_db_key(db: sqlite3.Connection) -> str:
        """
        Получить ключ текущей main-базы для кеша метаданных ORM.

        Формат ключа:
        - file:<абсолютный_путь> для файловой БД;
        - memory:<id(connection)> для in-memory/временной БД.
        """
        cur = db.cursor()
        cur.execute("PRAGMA database_list")
        for _, db_name, file_path in cur.fetchall():
            if db_name == "main":
                return f"file:{file_path}" if file_path else f"memory:{id(db)}"
        return f"unknown:{id(db)}"

    def __init__(self, db: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
        self.db = db
        self.cur = cursor
        self._list_requests: list[str] = []

        # При нескольких соединениях к одному файлу DDL (DROP/ALTER) может ждать блокировку —
        # busy_timeout заставляет SQLite подождать миллисекунды вместо мгновенного OperationalError.
        self.db.execute("PRAGMA busy_timeout = 3000")

        if not hasattr(self, "_flag_of_instance"):
            self._flag_of_instance = bytearray(1)

        # Чекер __init__
        self._flag_of_instance[0] |= 1 << 1

        cls = type(self)
        # db_key — айди файла БД или id(self.db) для in-memory.
        db_key = self._get_db_key(self.db)
        # cache_key — пара (класс модели, ключ БД): отдельный кеш метаданных на каждую БД.
        cache_key = (cls, db_key)

        # Проверка, что класс зарегистрирован
        if not (cls._flag_of_cls[0] & (1 << 0)):
            raise self.get_error(RuntimeError, id=7)

        # Флаг @check_columns_for_update: синхронизировать схему с KColumn при расхождении
        force_schema_refresh = bool(cls._flag_of_cls[0] & (1 << 3))

        # Берём кеш по (cls, db_key)
        meta: dict[str, Any] | None = self._orm_meta_cache.get(cache_key)

        # Режим глобальной блокировки чтения: не раскрываем буферы маппинга строки.
        if cls._flag_of_cls[0] & (1 << 2):
            self._col_to_idx = {}
            self._data = []
            return

        # columns_in_class — все KColumn из иерархии класса; cols_sql — фрагменты для CREATE TABLE.
        columns_in_class: dict[str, KColumn] = {}
        for base in cls.__mro__:
            for name, obj in base.__dict__.items():
                if isinstance(obj, KColumn) and name not in columns_in_class:
                    columns_in_class[name] = obj

        cols_sql = [f"{obj._name} {obj.sql_types}" for obj in columns_in_class.values()]

        # Если включена авто-синхронизация и в классе изменились колонки — кеш недействителен.
        if (
            force_schema_refresh
            and meta is not None
            and meta.get("cols_sql") != cols_sql
        ):
            self._orm_meta_cache.pop(cache_key, None)
            meta = None

        if meta is None:
            safe_table = self._quote_identifier(self._table_name)
            # force_schema_refresh: перестраиваем таблицу под актуальный список колонок при необходимости.
            if force_schema_refresh:
                self._sync_table_schema_for_update(columns_in_class, cols_sql)
            else:
                self.cur.execute(
                    f"CREATE TABLE IF NOT EXISTS {safe_table} ({', '.join(cols_sql)})"
                )

            self.cur.execute(f"PRAGMA table_info({safe_table})")
            rows = self.cur.fetchall()
            # db_map: имя колонки -> индекс в БД (cid)
            db_map = {row[1]: row[0] for row in rows}
            # Маппинг имён атрибутов дескрипторов -> индексы колонок SQLite (cid)

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
            self._orm_meta_cache[cache_key] = meta

        else:
            # Убеждаемся, что таблица существует в текущем соединении.
            cols_sql = meta["cols_sql"]
            safe_table = self._quote_identifier(self._table_name)
            self.cur.execute(
                f"CREATE TABLE IF NOT EXISTS {safe_table} ({', '.join(cols_sql)})"
            )

        self._col_to_idx = meta["col_to_idx"]
        self._data = [None] * meta["data_size"]

    def check_sql3_request(
        self, sql_query: str, *, allowed_commands: Optional[set[str]] = None
    ) -> tuple[bool, str]:
        """
        Проверяет валидность SQL-запроса и возвращает кортеж:
        булево значение валидности и строку с причиной.
        Параметры:
            sql_query: SQL-запрос для проверки.
            allowed_commands: Набор команд, разрешенных в данном контексте.
        Возвращает:
            Кортеж из булева признака валидности и строки с причиной.
        """

        if not isinstance(sql_query, str):
            return False, "SQL request must be a string."

        q = sql_query.strip().rstrip(";")

        if not q:
            return False, "SQL request cannot be empty."

        if q.count("(") != q.count(")"):
            return False, "Unbalanced parentheses in SQL request."

        no_quotes = re.sub(r"'.*?'|\".*?\"", "", q)
        if ";" in no_quotes:
            return False, "Multiple SQL statements"

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
            # Проблемы уровня синтаксиса — это невалидные запросы.
            # Семантические/выполняемые ошибки (например, no such table) все еще означают валидный SQL.
            if any(signature in msg for signature in syntax_signatures):
                return False, str(e)
            return True, ""
        except Exception as e:
            return False, str(e)

    @classmethod
    def _quote_identifier(cls: type, name: str) -> str:
        """
        Проверить и экранировать идентификаторы SQLite (имена таблиц/колонок).
        """
        if not isinstance(name, str) or not cls.IDENTIFIER_RE.fullmatch(name):
            raise cls.get_error(ValueError, id=22, add=str(name))
        return f'"{name}"'

    def _sync_table_schema_for_update(
        self, columns_in_class: dict[str, KColumn], cols_sql: list[str]
    ) -> None:
        """
        Пересобрать схему таблицы по текущим определениям KColumn и сохранить общие данные.
        """
        safe_table = self._quote_identifier(self._table_name)

        self.cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            (self._table_name,),
        )
        exists = self.cur.fetchone() is not None

        if not exists:
            self.cur.execute(
                f"CREATE TABLE IF NOT EXISTS {safe_table} ({', '.join(cols_sql)})"
            )
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
        Валидировать и выполнить один SQL-запрос или отложенный батч.

        Метод проверяет флаги доступа, валидирует каждый SQL-запрос,
        нормализует разделители, затем выполняет итоговую очередь.
        """
        # Блокировки доступа:
        # - операция требует экземпляр соединения (не USER-режим)
        # - классовые блокировки записи/чтения также блокируют execute
        if self._flag_of_instance[0] & (1 << 0):
            raise self.get_error(PermissionError, id=0)
        if type(self)._flag_of_cls[0] & (1 << 1) or type(self)._flag_of_cls[0] & (
            1 << 2
        ):
            raise self.get_error(PermissionError, id=1)

        # Нормализуем вход в список кандидатных запросов.
        if isinstance(sql3_data, str):
            raw_queries = [sql3_data] if sql3_data.strip() else []
        else:
            raw_queries = sql3_data

        # Валидируем и нормализуем каждый запрос.
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
                raise self.get_error(
                    sqlite3.OperationalError, id=20, add=details
                ) from None

            clean_current.append(query)

        # Объединяем отложенные запросы и выполняем как один атомарный батч.
        final_queue = self._list_requests + clean_current
        if not final_queue:
            return

        queued_before = list(self._list_requests)
        savepoint = "__korm_execute_batch"
        try:
            self.cur.execute(f"SAVEPOINT {savepoint}")
            super().execute(final_queue)
            self.cur.execute(f"RELEASE SAVEPOINT {savepoint}")
            # Очищаем очередь отложенных запросов только после успешного выполнения.
            self._list_requests.clear()
        except Exception as e:
            try:
                self.cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                self.cur.execute(f"RELEASE SAVEPOINT {savepoint}")
            except Exception:
                # Резервный путь: откат всего соединения, если работа с savepoint не удалась.
                self.db.rollback()
            # Восстанавливаем отложенную очередь; одноразовый прямой ввод специально не ставится в очередь.
            self._list_requests = queued_before
            raise self.get_error(type(e), message=str(e)) from e

    def add_requests(
        self, reqs: str | list[str] | Iterable[str] | tuple[str, ...] | set[str]
    ) -> None:
        """
        Добавить один или несколько SQL-запросов в отложенную очередь (только для connection-режима).

        Параметры:
            reqs: Одна SQL-строка или список SQL-строк.
        """
        if self._flag_of_instance[0] & (1 << 0):
            raise self.get_error(PermissionError, id=0)
        if type(self)._flag_of_cls[0] & (1 << 1) or type(self)._flag_of_cls[0] & (
            1 << 2
        ):
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
        Получить список запросов, поставленных в очередь на выполнение.
        """
        return self._list_requests

    def fetchall(self, sql3_select: str) -> list[tuple[Any, ...]]:
        """
        Получить все строки из базы данных.
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
        Получить одну строку с проверками прав доступа/типов ORM.

        Поведение повторяет базовый `fetchone`:
        - `immediate is not None` -> сразу вернуть `cursor.fetchone()`;
        - пустой `sql3_select` -> сразу вернуть `cursor.fetchone()`;
        - непустой `sql3_select` -> выполнить запрос и вернуть `"SELECTED"`.
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
        **ranges: tuple[Any, Any] | tuple[tuple[Any, Any], Any] | tuple[Any, Any, Any],
    ) -> str:
        """
        Удалить строки, где значение каждой указанной колонки попадает в заданный диапазон.

        Каждый именованный аргумент поддерживает один из форматов:
            column_name=exact_value
            column_name=(exact_value,)
            column_name=(start_value, end_value)
            column_name=((start_value, end_value), KOR|KAND|None)
            column_name=(start_value, end_value, KOR|KAND|None)

        Пример:
            delete_ranges(id=(1, 100), price=(10, 50))

        Примечания:
        - Имена колонок валидируются по известным колонкам ORM.
        - Идентификаторы экранируются для защиты от подстановки в имена колонок/таблиц.
        - Если логический маркер не указан, используется AND.
        - Сокращенная форма точного значения преобразуется в `column = ?`.
        - `KOR` объединяет текущее условие со следующим через OR.
        - `KAND` объединяет текущее условие со следующим через AND.

        Возвращает:
            Человекочитаемое сообщение с количеством удаленных строк.

        Исключения:
            PermissionError: если операция заблокирована флагами режима/класса.
            ValueError: если диапазоны не переданы.
            AttributeError: если колонка неизвестна/недоступна.
            TypeError: если формат диапазона/логического маркера некорректен.
        """
        if self._flag_of_instance[0] & (1 << 0):
            raise self.get_error(PermissionError, id=0)
        if type(self)._flag_of_cls[0] & (1 << 1) or type(self)._flag_of_cls[0] & (
            1 << 2
        ):
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
                # (start, end) ИЛИ ((start, end), logic)
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
                raise self.get_error(
                    TypeError, message=f"Invalid range format for '{column}'"
                )

            if not use_exact_match and (
                not isinstance(bounds, tuple) or len(bounds) != 2
            ):
                raise self.get_error(
                    TypeError,
                    message=f"Range for '{column}' must contain exactly 2 bounds",
                )

            if logic is None or logic is KAND:
                join_op = "AND"
            elif logic is KOR:
                join_op = "OR"
            else:
                raise self.get_error(
                    TypeError, message=f"Unsupported logic marker for '{column}'"
                )

            # Маркер логики относится к текущему (левому) условию, поэтому его нельзя
            # прикреплять к последнему условию (нет правого условия для объединения).
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
            # Оператор берется из предыдущего (левого) условия.
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
        Удалить строки по фильтрам точного совпадения.

        Каждый именованный аргумент трактуется как:
            column_name = value

        Пример:
            delete(id=1, name="admin")

        Примечания:
        - Требуется минимум один фильтр.
        - Неизвестные колонки отклоняются.
        - Идентификаторы колонок безопасно экранируются.
        - Все условия фильтра объединяются через AND.

        Возвращает:
            Человекочитаемое сообщение с количеством удаленных строк.

        Исключения:
            PermissionError: если операция заблокирована флагами режима/класса.
            ValueError: если фильтры не переданы.
            AttributeError: если колонка неизвестна/недоступна.
        """
        if self._flag_of_instance[0] & (1 << 0):
            raise self.get_error(PermissionError, id=0)
        if type(self)._flag_of_cls[0] & (1 << 1) or type(self)._flag_of_cls[0] & (
            1 << 2
        ):
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
        Удалить одну или несколько колонок из схемы таблицы.

        Ограничения:
        - Доступно только для экземпляров соединения (не для USER-оберток).
        - Обязательную колонку первичного ключа `id` удалять нельзя.

        Параметры:
            *columns: Имена атрибутов ORM/имен колонок для удаления.

        Возвращает:
            Человекочитаемое сообщение с именами удаленных колонок.

        Исключения:
            PermissionError: если операция заблокирована флагами режима/класса.
            ValueError: если не передана ни одна колонка.
            TypeError: если имя любой колонки не является строкой.
            AttributeError: если колонка неизвестна/недоступна.
        """
        if self._flag_of_instance[0] & (1 << 0):
            raise self.get_error(PermissionError, id=0)
        if type(self)._flag_of_cls[0] & (1 << 1) or type(self)._flag_of_cls[0] & (
            1 << 2
        ):
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

        # Поддерживаем метаданные класса в согласованном состоянии после удаления колонок.
        cls = type(self)
        for col in cols_to_drop:
            if col in cls.__dict__ and isinstance(cls.__dict__[col], KColumn):
                delattr(cls, col)

        # Сбрасываем кеш только для текущей БД, а не для всех соединений этого класса.
        db_key = self._get_db_key(self.db)
        cls._orm_meta_cache.pop((cls, db_key), None)

        # Пересобираем in-memory маппинг для текущего экземпляра.
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

    def drop(
        self, force: str | None = None
    ) -> Literal["CORRECT: Table dropped", "CANCELED"]:
        """
        Удалить таблицу, если она существует (операция только для connection-режима).

        Параметры:
            force:
                - None -> отменить без удаления;
                - 'n'  -> отменить без удаления;
                - 'y'  -> удалить сразу.

        Возвращает:
            "CORRECT: Table dropped" при успехе, иначе "CANCELED".

        Исключения:
            PermissionError: если операция заблокирована флагами режима/класса.
            TypeError: если параметр force не строка и не None.
            ValueError: если параметр force не равен 'y' или 'n'.
            Exception: завернутая ошибка БД при выполнении/commit SQLite.
        """

        if self._flag_of_instance[0] & (1 << 0):
            raise self.get_error(PermissionError, id=0)

        if type(self)._flag_of_cls[0] & (1 << 1) or type(self)._flag_of_cls[0] & (
            1 << 2
        ):
            raise self.get_error(PermissionError, id=1)

        if not isinstance(force, (str, type(None))):
            raise self.get_error(
                TypeError, message="Force parameter must be a string or None."
            )
        if force is None:
            return "CANCELED"

        if force.strip().lower() not in ("y", "n"):
            raise self.get_error(
                ValueError, message="Force parameter must be 'y' or 'n'."
            )

        if force.strip().lower() == "n":
            return "CANCELED"

        safe_table = self._quote_identifier(self._table_name)
        try:
            self.cur.execute(f"DROP TABLE IF EXISTS {safe_table}")
            self.db.commit()
            # После удаления таблицы инвалидируем кеш метаданных только для текущей БД.
            cls = type(self)
            db_key = self._get_db_key(self.db)
            cls._orm_meta_cache.pop((cls, db_key), None)
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
        Сохранить текущие данные строки из памяти в базу данных.

        Поведение:
        - Если `id` равен `None`, выполняется INSERT и `lastrowid` записывается обратно в `self._data`.
        - Если `id` задан, выполняется UPDATE по первичному ключу.

        Примечания:
        - Операция требует USER-режим.
        - Операция блокируется флагами класса (`knocommands` / `knoread`).
        - Схема таблицы должна содержать колонку `id`.

        Возвращает:
            `True`, когда INSERT/UPDATE успешно зафиксирован.

        Исключения:
            PermissionError: если операцию блокируют флаги режима/класса.
            AttributeError: если маппинг таблицы некорректен или строка для UPDATE не найдена.
            Exception: завернутая ошибка БД при выполнении/commit SQLite.
        """

        if not (self._flag_of_instance[0] & (1 << 0)):
            raise self.get_error(PermissionError, id=0)

        if type(self)._flag_of_cls[0] & (1 << 1) or type(self)._flag_of_cls[0] & (
            1 << 2
        ):
            raise self.get_error(PermissionError, id=1)

        if (pk_name := "id") not in self._col_to_idx:
            raise self.get_error(AttributeError, id=6)

        all_cols = self._col_to_idx

        # Получаем id из _data
        pk_value = self._data[all_cols[pk_name]]

        update_cols = [name for name in all_cols if name != pk_name]

        try:
            if pk_value is None:
                # Логика вставки
                cols_names = ", ".join(
                    self._quote_identifier(name) for name in all_cols.keys()
                )
                placeholders = ", ".join(["?"] * len(all_cols))
                safe_table = self._quote_identifier(self._table_name)
                sql = f"INSERT INTO {safe_table} ({cols_names}) VALUES ({placeholders})"
                params = [self._data[all_cols[name]] for name in all_cols]

                self.cur.execute(sql, params)

                if self.cur.lastrowid:
                    self._data[all_cols[pk_name]] = cast(Any, self.cur.lastrowid)
            else:
                # Логика обновления
                if not update_cols:
                    return True

                set_clause = ", ".join(
                    [f"{self._quote_identifier(name)} = ?" for name in update_cols]
                )
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
        Сформировать и вернуть экземпляр исключения с нормализованным сообщением ORM.

        Параметры:
            error: Класс/тип исключения для создания экземпляра.
            message: Резервное сообщение, когда `id` не передан.
            id: Необязательный внутренний идентификатор сообщения из локального `msgs`.
            add: Необязательный дополнительный контекст, добавляемый в итоговое сообщение.

        Возвращает:
            Созданный объект исключения (`error(final_message)`).

        Примечания:
        - Если `id` передан и существует в `msgs`, используется сопоставленный текст.
        - Если `id` неизвестен, используется `message` как есть.
        - `add` добавляется как `": {add}"`, когда `add is not None`.
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
    Зарегистрировать ORM-модель и проверить контракт вызова базового инициализатора.
    """

    if not issubclass(cls, KCoreORM):
        raise TypeError(
            f"Class {cls.__name__} must inherit from KCoreORM to be registered."
        )

    if cls._flag_of_cls[0] & (1 << 0):
        return cls

    if not hasattr(cls, "_table_name") or not isinstance(cls._table_name, str):
        raise TypeError(f"Class {cls.__name__} must define string _table_name.")
    if not hasattr(cls, "_flag_of_cls"):
        raise TypeError(f"Class {cls.__name__} must define _flag_of_cls.")
    if not isinstance(cls._flag_of_cls, bytearray) or len(cls._flag_of_cls) == 0:
        raise TypeError(
            f"Class {cls.__name__} must have non-empty bytearray _flag_of_cls."
        )

    if cls._table_name.upper() in cls.SQLITE_RESERVED_WORDS:
        raise cls.get_error(
            AttributeError,
            message=f"Name table '{cls._table_name}' reserved word in SQLite. Choose another name.",
        )

    # Собираем все дескрипторы KColumn из иерархии классов.
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

    # Изолируем флаги класса от базовых классов (избегаем общего изменяемого bytearray).
    if "_flag_of_cls" not in cls.__dict__:
        cls._flag_of_cls = bytearray(cls._flag_of_cls)

    # Активируем класс (бит 0): конструктор разрешен для зарегистрированных классов.
    cls._flag_of_cls[0] |= 1 << 0

    orig_init = cls.__init__

    # 1. Сначала ОПРЕДЕЛЯЕМ функцию (но не выходим!)
    def wrapped_init(self, *args, **kwargs):
        if hasattr(self, "_flag_of_instance") and isinstance(
            self._flag_of_instance, bytearray
        ):
            self._flag_of_instance[0] &= ~(1 << 1)

        orig_init(self, *args, **kwargs)

        if not hasattr(self, "_flag_of_instance") or not (
            self._flag_of_instance[0] & (1 << 1)
        ):
            raise cls.get_error(
                RuntimeError,
                message=f"{cls.__name__}.__init__ must call super().__init__(db, cursor)",
            )

    cls.__init__ = wrapped_init

    # ВКЛЮЧАЕМ бит 0: "Класс зарегистрирован"
    cls._flag_of_cls[0] |= 1 << 0

    return cls


@decorator
def knocommands(cls: Type[T]) -> Type[T]:
    """
    Включить блокировку записи/команд на уровне класса (бит 1)."""
    if "_flag_of_cls" not in cls.__dict__:
        cls._flag_of_cls = bytearray(cls._flag_of_cls)
    cls._flag_of_cls[0] |= 1 << 1
    return cls


@decorator
def knoread(cls: Type[T]) -> Type[T]:
    """
    Включить блокировку чтения на уровне класса (бит 2).
    """
    if "_flag_of_cls" not in cls.__dict__:
        cls._flag_of_cls = bytearray(cls._flag_of_cls)
    cls._flag_of_cls[0] |= 1 << 2
    return cls


def KUserMode(conn_with_orm: T) -> T:
    """
    Создать и вернуть USER-mode прокси на основе базового ORM-соединения.

    Оставлено как обычная generic-функция, чтобы IDE надёжнее выводили конкретный тип модели
    из `KUserMode(conn)`, чем при динамической фабрике классов.
    """

    # Создаем прокси-экземпляр пользовательского режима поверх базового ORM-соединения.
    class UserMode(conn_with_orm.__class__):
        # Явные slots изолируют состояние экземпляра для обертки user-mode.
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
            # Разделяем ресурсы БД из базового соединения.
            self.db = base.db
            self.cur = base.cur
            self._table_name = base._table_name
            self._col_to_idx = base._col_to_idx
            self._list_requests = []

            # Включаем флаг USER-режима.
            self._flag_of_instance = bytearray(1)
            self._flag_of_instance[0] = 1 << 0
            # Собственный in-memory буфер строки для каждого экземпляра user-обертки.
            if isinstance(base._data, list):
                self._data = [None] * len(base._data)
            else:
                self._data = []

    # Возвращаем прокси-экземпляр USER-режима.
    return cast(T, UserMode(conn_with_orm))
