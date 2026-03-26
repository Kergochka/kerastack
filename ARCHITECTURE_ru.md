## Архитектура ORM

## Базовая философия

ORM построена вокруг двух контекстов выполнения:

- `connection`-контекст (`conn`): схема, инфраструктура, низкоуровневый SQL.
- `user`-контекст (`user`): прикладные операции с записями (`load/save`) с жесткими проверками.

Это разделение сделано специально: структурные операции и операции уровня строки не смешиваются.

---

## Минимальный сценарий подключения

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

conn = DB(db, cur)      # connection-контекст
user = KUserMode(conn)  # user-контекст
```

---

## Обязательные требования к модели

Чтобы модель работала корректно:

- Она **обязана наследоваться** от `KCoreORM`.
- Она **обязана быть декорирована** через `@kregister` до создания инстансов.
- В ней **обязана быть колонка** `id`.
- `id` должен быть объявлен как `INTEGER PRIMARY KEY` (допустимо с `AUTOINCREMENT`).

Почему это важно:

- Регистрация включает контракт ORM и флаги класса.
- `id` нужен для стабильной адресации строк в `save()/load()`.

---

## Декораторы

Замечание по структуре проекта:

- `kerastack/KergaSQL.py` содержит декораторы, связанные с ядром DB-логики.
- `kerastack/decorators.py` содержит декораторы вне основного командного DB-контура (вспомогательные).

### `@kregister` (`kerastack/KergaSQL.py`)

- Проверяет наследование от `KCoreORM`.
- Проверяет `_table_name` и `_flag_of_cls`.
- Собирает `KColumn` по иерархии классов.
- Проверяет обязательный `id` и форму primary key.
- Включает флаг регистрации (`_flag_of_cls`, bit 0).
- Оборачивает `__init__`, чтобы гарантировать вызов `super().__init__(db, cursor)`.
- Идемпотентен (не оборачивает `__init__` повторно).
- Изолирует class-флаги от родителя (чтобы не делить общий mutable `bytearray`).

### `@knocommands` (`kerastack/KergaSQL.py`)

- Включает class-lock на команды/запись (`_flag_of_cls`, bit 1).
- Блокирует write/command операции для класса.

### `@knoread` (`kerastack/KergaSQL.py`)

- Включает глобальный class-lock (`_flag_of_cls`, bit 2).
- Блокирует не только чтение, но и запись/команды.

### `@check_columns_for_update` (`kerastack/decorators.py`)

- Вспомогательный декоратор (не часть core DB command flow).
- Включает флаг синхронизации схемы (`_flag_of_cls`, bit 3).
- При инициализации запускает путь согласования схемы таблицы.

---

## `KColumn`: слой дескрипторов

`KColumn` — дескриптор, который одновременно задает схему и управляет доступом к полям.

Что делает:

- Валидирует SQL-описание типа на этапе определения класса.
- Привязывает имя поля через `__set_name__`.
- В `user`-режиме читает/пишет значения в `_data`.
- В `connection`-режиме прямой доступ к полям записи запрещен по контракту.

Почему это важно:

- Поля модели выглядят как обычные Python-атрибуты, но под капотом соблюдают режим, безопасность и консистентность маппинга.

---

## `KUserMode`: фабрика user-контекста

`KUserMode` — factory-класс, который строит USER-прокси поверх базового connection-объекта.

Внутреннее поведение:

- Создает runtime-подкласс модели.
- Переиспользует те же `db` и `cur`.
- Переиспользует структурные маппинги (`_table_name`, `_col_to_idx`) базового connection-объекта.
- Включает instance-флаг USER (`_flag_of_instance`, bit 0).
- Выделяет отдельный `_data`-буфер на каждый user-инстанс.

Практический эффект:

- Можно иметь несколько `user`-оберток с независимым in-memory состоянием.
- Row-level операции изолированы от низкоуровневого SQL-контура connection.

---

## Connection-контекст: полная карта возможностей

`conn = DB(db, cur)` — технический контекст для схемы и SQL-оркестрации.

### Методы и поведение

- `execute(sql_or_list)` (только connection)
  - Валидирует каждую SQL-команду через `check_sql3_request`.
  - Принимает одну строку SQL или список строк.
  - Исполняет отложенную очередь из `add_requests` и текущие команды.
  - Автокоммита нет.

- `add_requests(reqs)` (только connection)
  - Кладет валидные SQL в `_list_requests`.
  - Удобен для отложенного батча.
  - Автокоммита нет.

- `get_requests()`
  - Возвращает текущую очередь SQL.
  - Только просмотр; без исполнения; без commit.

- `fetchall(select_sql)` (только connection)
  - Принимает только `SELECT`.
  - Возвращает `list[tuple[Any, ...]]`.

- `fetchone(select_sql="", immediate=None)` (только connection)
  - Поддерживает two-step и immediate режимы выборки одной строки.

- `delete(**filters)` (только connection)
  - Удаление по точным фильтрам; условия объединяются через `AND`.
  - При успехе делает `commit()`.

- `delete_ranges(...)` (только connection)
  - Удаление по диапазонам `BETWEEN`.
  - Поддерживает логические маркеры `KOR` / `KAND`.
  - Поддерживает и shorthand для точного совпадения: `column=value` превращается в `column = ?`.
  - `KOR` означает `OR`; `KAND` означает `AND`.
  - Логический маркер относится к текущему левому условию и связывает его со следующим.
  - Если маркер не указан, по умолчанию используется `AND`.
  - У последнего условия `KOR` / `KAND` ставить нельзя.
  - Пример:
    - `conn.delete_ranges(id=(1, 10, KOR), name="a")`
    - SQL-форма: `... WHERE id BETWEEN 1 AND 10 OR name = 'a'`.
    `conn.delete_ranges(id=(1, 10, KOR), spec=(10, 20))`
    - SQL-форма: `... WHERE id BETWEEN 1 AND 10 OR spec BETWEEN 10 AND 20`
  - При успехе делает `commit()`.

- `drop(force)` (только connection)
  - `None` / `"n"`: отмена.
  - `"y"`: удаление таблицы.
  - При успехе `commit()`, при ошибке `rollback()`.

- `drop_columns(*columns)` (только connection)
  - Удаляет выбранные колонки из схемы.
  - Запрещает удалять обязательный `id`.
  - При успехе `commit()`, при ошибке `rollback()`.
  - Удаляет соответствующие `KColumn`-дескрипторы из класса модели (`delattr`).
  - Пересобирает metadata/маппинги после изменения схемы.

- `check_sql3_request(sql, allowed_commands=None)`
  - Вспомогательная SQL-валидация.
  - Возвращает `(ok: bool, reason: str)`.
  - Состояние БД не изменяет.

- `_quote_identifier(name)` (class helper)
  - Валидирует и безопасно экранирует идентификаторы таблиц/колонок.
  - Защищает от небезопасных имен.

### Роль инициализатора (`KCoreORM.__init__`)

- Привязывает `db`, `cur`.
- Инициализирует очередь запросов и instance-флаги.
- Строит/подгружает metadata и форму row-буфера.
- Создает таблицу при отсутствии.
- При включенном флаге синхронизации запускает согласование схемы.

---

## Ограничения connection-контекста

Главное правило: connection-контекст не является row-level API.

В connection-режиме запрещено:

- Прямой доступ к полям модели через дескриптор:
  - `conn.name`, `conn.name = "..."`.
- User-only API:
  - `load`, `save`.

Class-lock ограничения тоже действуют:

- `@knocommands` блокирует command/write-контур.
- `@knoread` блокирует read-контур и также блокирует write/command-контур.

Рабочее правило:

- `conn` используем для схемы/SQL-оркестрации.
- `user = KUserMode(conn)` используем для операций над записями.

---

## User-контекст: полная карта возможностей

`user = KUserMode(conn)` — контекст работы с конкретными строками и буфером данных.

### Методы и поведение

- `load(row_id)`
  - Читает строку по `id`.
  - Если строка найдена: загружает в `_data`, возвращает `True`.
  - Если не найдена: возвращает `False`.

- `save()`
  - Если `id is None`: делает `INSERT`, затем пишет `lastrowid` обратно.
  - Если `id` задан: делает `UPDATE ... WHERE id = ?`.
  - При успехе делает `commit()`.
  - Требует корректный маппинг `id`.

### Ограничения user-контекста

- В user-контексте запрещены connection-only операции:
  - `execute`, `add_requests`, `fetchall`, `fetchone`, `delete`, `delete_ranges`, `drop`, `drop_columns`.
- Class-lock декораторы продолжают действовать:
  - `knocommands` блокирует write/command;
  - `knoread` блокирует read и также блокирует write/command.

---

## Матрица commit/rollback поведения

### Connection-сторона

- `execute` -> автокоммита нет.
- `add_requests` -> автокоммита нет.
- `get_requests` -> commit не делает.
- `check_sql3_request` -> commit не делает.
- `delete` -> commit при успехе, rollback при ошибке выполнения.
- `delete_ranges` -> commit при успехе, rollback при ошибке выполнения.
- `drop` -> commit при успехе, rollback при ошибке.
- `drop_columns` -> commit при успехе, rollback при ошибке.

### User-сторона

- `save` -> commit при успехе.
- `save` -> rollback при ошибке и поднимает обернутое исключение.
- `load` -> read-only, commit не делает.

Рекомендуемый паттерн для батчей:

1. `conn.add_requests(...)`
2. `conn.execute("")`
3. `conn.db.commit()`

---

## Контракт исключений для разработчика

Зачем это нужно:

- Позволяет писать предсказуемый `try/except`.
- Быстро отделяет ошибки доступа от ошибок данных и SQL.
- Стабилизирует тесты и API-level обработку ошибок.

Ключевые типы исключений:

- `PermissionError`
  - Неверный контекст (`connection` vs `user`) или class-lock.
- `ConnPermissionError`
  - Попытка доступа к данным через дескриптор `KColumn` вне USER mode (`conn.field` или присваивание).
- `TypeError`
  - Неверные типы аргументов или неверная форма SQL-входа.
- `AttributeError`
  - Неизвестная/отсутствующая колонка, проблемы маппинга, отсутствие целевой строки для update.
- `sqlite3.OperationalError`
  - SQL отклонен валидатором или упал на выполнении в SQLite.

Рекомендуемый порядок обработки:

1. `PermissionError` (контракт доступа/режима)
2. `TypeError` / `AttributeError` (контракт входа/схемы)
3. `sqlite3.OperationalError` (SQL/runtime слой)
4. Общий `Exception` как fallback

`get_error(...)` централизует сообщения ORM и поддерживает сообщения по `id` для единообразия.

---

## Справка по внутренним флагам

### Флаги класса (`_flag_of_cls[0]`)

- bit 0: класс зарегистрирован.
- bit 1: глобальный lock на запись/команды.
- bit 2: глобальный lock на чтение + запись + команды.
- bit 3: принудительная синхронизация схемы по актуальным `KColumn`.

### Флаги инстанса (`_flag_of_instance[0]`)

- bit 0: инстанс работает в USER mode.
- bit 1: маркер вызова базового инициализатора (`super().__init__` выполнен).
