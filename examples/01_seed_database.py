import sqlite3
import sys
from pathlib import Path

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


def seed() -> None:
    db = sqlite3.connect(DB_PATH)
    cur = db.cursor()

    conn = UsersDB(db, cur)
    user = KUserMode(conn)

    # Reset table contents for deterministic demo behavior.
    conn.execute('DELETE FROM "users"')
    # Also reset AUTOINCREMENT sequence so ids restart from 1.
    conn.execute("DELETE FROM sqlite_sequence WHERE name = 'users'")
    db.commit()

    rows = [
        ("Alice", 24),
        ("Bob", 31),
        ("Charlie", 19),
    ]

    for name, age in rows:
        user.id = None
        user.name = name
        user.age = age
        user.save()

    print("Database seeded.")
    print(f"Path: {DB_PATH}")
    print("Rows:")
    for row in conn.fetchall('SELECT id, name, age FROM "users" ORDER BY id'):
        print(row)

    db.close()


if __name__ == "__main__":
    seed()
