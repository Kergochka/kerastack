import sqlite3
import sys
from pathlib import Path
from typing import Optional

# Allow running this file directly from the examples folder.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kerastack.KergaSQL import KColumn, KCoreORM, KUserMode, kregister


DB_PATH = "example.db"


@kregister
class UsersDB(KCoreORM):
    __slots__ = ()
    _table_name = "users"

    id = KColumn("INTEGER PRIMARY KEY AUTOINCREMENT")
    name = KColumn("TEXT")
    age = KColumn("INTEGER")

    def __init__(self, db: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
        super().__init__(db, cursor)


def ask_int(prompt: str) -> Optional[int]:
    raw = input(prompt).strip()
    if raw == "":
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def run() -> None:
    db = sqlite3.connect(DB_PATH)
    cur = db.cursor()

    conn = UsersDB(db, cur)
    user = KUserMode(conn)

    print("Current rows:")
    rows = conn.fetchall('SELECT id, name, age FROM "users" ORDER BY id')
    if not rows:
        print("Table is empty. First run: examples/01_seed_database.py")
        db.close()
        return

    for row in rows:
        print(row)

    row_id = ask_int("Enter user id to edit: ")
    if row_id is None:
        print("Invalid id.")
        db.close()
        return

    if not user.load(row_id):
        print("Row not found.")
        db.close()
        return

    print(f"Loaded row -> id={user.id}, name={user.name}, age={user.age}")
    new_name = input("New name (empty = keep current): ").strip()
    new_age_raw = input("New age (empty = keep current): ").strip()

    if new_name:
        user.name = new_name

    if new_age_raw:
        try:
            user.age = int(new_age_raw)
        except ValueError:
            print("Age must be integer.")
            db.close()
            return

    user.save()
    print("Row updated.")
    print("Updated row:")
    user.load(row_id)
    print((user.id, user.name, user.age))

    db.close()


if __name__ == "__main__":
    run()
